package node

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

func TestThreeNodeRoutingAndReplication(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	for i := 0; i < 100; i++ {
		mustSet(t, cl, fmt.Sprint("key-", i), fmt.Sprint("val-", i))
	}
	// Every node answers for every key, forwarding to the owner as needed.
	for _, id := range c.ids() {
		for i := 0; i < 100; i += 7 {
			resp := call(t, c.nodes[id], &protocol.Message{Op: protocol.OpGet, Key: fmt.Sprint("key-", i)})
			if resp.Status != protocol.StatusOK || !resp.Found || string(resp.Value) != fmt.Sprint("val-", i) {
				t.Fatalf("node %s key-%d: %+v", id, i, resp)
			}
		}
	}
	// RF=3 on 3 nodes: every node eventually holds every key (W=2 acks before
	// success, the third replica is updated in the background).
	eventually(t, 5*time.Second, "full replication", func() bool {
		for _, n := range c.nodes {
			for i := 0; i < 100; i++ {
				if v, ok := localValue(n, fmt.Sprint("key-", i)); !ok || v != fmt.Sprint("val-", i) {
					return false
				}
			}
		}
		return true
	})
}

func TestReplicaPlacementMatchesRing(t *testing.T) {
	c := newCluster(t, 5, func(cfg *config.Config) { cfg.ReplicationFactor = 2 })
	cl := c.client()
	keys := 200
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "v")
	}
	r := c.nodes["node-1"].Ring()
	eventually(t, 5*time.Second, "placement", func() bool {
		for i := 0; i < keys; i++ {
			k := fmt.Sprint("k", i)
			owners := r.Owners(k, 2)
			if len(owners) != 2 || owners[0] == owners[1] {
				t.Fatalf("bad owners %v", owners)
			}
			for id, n := range c.nodes {
				_, has := localValue(n, k)
				if has != slices.Contains(owners, id) {
					return false
				}
			}
		}
		return true
	})
}

func TestValidationAndErrors(t *testing.T) {
	c := newCluster(t, 1, func(cfg *config.Config) { cfg.ReplicationFactor = 1; cfg.MaxValueBytes = 16; cfg.MaxFrameBytes = 4096 })
	n := c.nodes["node-1"]
	cases := []struct {
		req  *protocol.Message
		code string
	}{
		{&protocol.Message{Op: protocol.OpSet, Key: ""}, protocol.CodeBadRequest},
		{&protocol.Message{Op: protocol.OpSet, Key: "k", Value: make([]byte, 17)}, protocol.CodeTooLarge},
		{&protocol.Message{Op: protocol.OpSet, Key: string(make([]byte, 600))}, protocol.CodeTooLarge},
		{&protocol.Message{Op: protocol.OpSet, Key: "k", TTLMs: -1}, protocol.CodeBadRequest},
		{&protocol.Message{Op: protocol.OpGet, Key: "k", Consistency: "all"}, protocol.CodeBadRequest},
		{&protocol.Message{Op: "FLUSHALL"}, protocol.CodeUnknownOp},
		{&protocol.Message{Op: protocol.OpReplicate}, protocol.CodeBadRequest},
	}
	for _, tc := range cases {
		if resp := call(t, n, tc.req); resp.Code != tc.code {
			t.Errorf("%s %q: got %q (%s), want %q", tc.req.Op, tc.req.Key, resp.Code, resp.Error, tc.code)
		}
	}
	resp := call(t, n, &protocol.Message{Op: protocol.OpGet, Key: "missing"})
	if resp.Status != protocol.StatusOK || resp.Found {
		t.Fatalf("missing key must be a successful miss: %+v", resp)
	}
}

func TestDuplicateAndOutOfOrderReplication(t *testing.T) {
	c := newCluster(t, 1, func(cfg *config.Config) { cfg.ReplicationFactor = 1 })
	n := c.nodes["node-1"]
	newer := store.Entry{Key: "k", Value: []byte("new"), Version: version.Version{T: 200, Node: "x"}}
	older := store.Entry{Key: "k", Value: []byte("old"), Version: version.Version{T: 100, Node: "x"}}
	for _, e := range []store.Entry{newer, older, newer, older} { // delayed + duplicated delivery
		e := e
		if resp := call(t, n, &protocol.Message{Op: protocol.OpReplicate, Entry: &e}); resp.Status != protocol.StatusOK {
			t.Fatal(resp.Error)
		}
	}
	if v, _ := localValue(n, "k"); v != "new" {
		t.Fatalf("got %q", v)
	}
	// The node's clock now exceeds the observed version, so a fresh write wins.
	resp := call(t, n, &protocol.Message{Op: protocol.OpSet, Key: "k", Value: []byte("fresh")})
	if resp.Version.T <= 200 {
		t.Fatalf("HLC did not advance past observed version: %d", resp.Version.T)
	}
}

func TestQuorumReadRepairsStaleReplica(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	mustSet(t, cl, "user:1", "alice")
	eventually(t, 3*time.Second, "replication", func() bool {
		for _, n := range c.nodes {
			if _, ok := localValue(n, "user:1"); !ok {
				return false
			}
		}
		return true
	})
	// Simulate a replica that lost its copy.
	victim := c.nodes["node-3"]
	e, _ := victim.Store().Lookup("user:1")
	victim.Store().RemoveIfVersion("user:1", e.Version)
	// A quorum read coordinated by the victim always includes its own answer.
	resp := call(t, victim, &protocol.Message{Op: protocol.OpGet, Key: "user:1", Consistency: config.ReadQuorum})
	if !resp.Found || string(resp.Value) != "alice" {
		t.Fatalf("quorum read: %+v", resp)
	}
	eventually(t, 3*time.Second, "read repair", func() bool {
		v, ok := localValue(victim, "user:1")
		return ok && v == "alice"
	})
}

func TestNodeCrashFailoverAndRecovery(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	for i := 0; i < 100; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "v1")
	}
	c.kill("node-2")
	eventually(t, 5*time.Second, "failure detection", func() bool {
		return !c.nodes["node-1"].Members().Routable("node-2") && !c.nodes["node-3"].Members().Routable("node-2")
	})
	// Reads and writes continue on the two surviving replicas (W=2).
	for i := 0; i < 100; i++ {
		if v, ok := mustGet(t, cl, fmt.Sprint("k", i)); !ok || v != "v1" {
			t.Fatalf("k%d after crash: %q %v", i, v, ok)
		}
	}
	for i := 0; i < 50; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "v2")
	}
	// node-2 restarts with an empty memory and must catch up before serving.
	n2 := c.start("node-2")
	c.waitConverged()
	rec, _ := n2.Status()
	if rec.State != "complete" || rec.KeysReceived < 100 {
		t.Fatalf("recovery status %+v", rec)
	}
	for i := 0; i < 100; i++ {
		want := "v1"
		if i < 50 {
			want = "v2"
		}
		if v, ok := localValue(n2, fmt.Sprint("k", i)); !ok || v != want {
			t.Fatalf("restarted node has k%d=%q (%v), want %q", i, v, ok, want)
		}
	}
}

func TestPartitionedNodeRejectsRequestsAndResyncsOnHeal(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	mustSet(t, cl, "color", "red")
	mustSet(t, cl, "doomed", "x")
	eventually(t, 3*time.Second, "replication", func() bool {
		_, ok := localValue(c.nodes["node-3"], "doomed")
		return ok
	})
	n3 := c.nodes["node-3"]
	c.partition("node-3")
	eventually(t, 5*time.Second, "partition detected on both sides", func() bool {
		return !c.nodes["node-1"].Members().Routable("node-3") && n3.inMinority()
	})
	// The majority keeps accepting writes.
	resp := call(t, c.nodes["node-1"], &protocol.Message{Op: protocol.OpSet, Key: "color", Value: []byte("blue")})
	if resp.Status != protocol.StatusOK {
		t.Fatalf("majority write failed: %+v", resp)
	}
	if resp := call(t, c.nodes["node-2"], &protocol.Message{Op: protocol.OpDelete, Key: "doomed"}); resp.Status != protocol.StatusOK {
		t.Fatalf("majority delete failed: %+v", resp)
	}
	// The isolated node refuses rather than serve its stale "red".
	resp = call(t, n3, &protocol.Message{Op: protocol.OpGet, Key: "color"})
	if resp.Code != protocol.CodeMinority {
		t.Fatalf("isolated node answered: %+v", resp)
	}
	if v, _ := localValue(n3, "color"); v != "red" {
		t.Fatalf("precondition: isolated node should still hold the stale value, has %q", v)
	}
	c.heal()
	c.waitConverged()
	if v, _ := localValue(n3, "color"); v != "blue" {
		t.Fatalf("healed node not resynchronized: %q", v)
	}
	if _, ok := localValue(n3, "doomed"); ok {
		t.Fatal("delete made during the partition was undone")
	}
	// Anti-entropy from the stale side must not resurrect anything.
	c.antiEntropy()
	if v, ok := mustGet(t, cl, "doomed"); ok {
		t.Fatalf("resurrected deleted key: %q", v)
	}
	if v, _ := mustGet(t, cl, "color"); v != "blue" {
		t.Fatalf("color=%q", v)
	}
	// The majority nodes were never forced to resynchronize.
	if rec, _ := c.nodes["node-1"].Status(); rec.Runs != 1 {
		t.Fatalf("majority node recovered %d times", rec.Runs)
	}
	if rec, _ := n3.Status(); rec.Runs < 2 {
		t.Fatalf("isolated node did not resynchronize (runs=%d)", rec.Runs)
	}
}

func TestLossOfAllReplicasIsAMiss(t *testing.T) {
	c := newCluster(t, 5, func(cfg *config.Config) {
		cfg.ReplicationFactor = 2
		cfg.RemoveDeadAfter = 300 * time.Millisecond
	})
	cl := c.client()
	mustSet(t, cl, "precious", "data")
	owners := c.nodes["node-1"].Ring().Owners("precious", 2)
	for _, o := range owners {
		c.kill(o)
	}
	survivor := c.nodes[c.ids()[0]]
	eventually(t, 10*time.Second, "auto-removal of dead owners", func() bool {
		return survivor.Ring().Len() == 3
	})
	c.waitConverged()
	// No copy survives and there is no durable source: an explicit miss, not
	// an error and not invented data.
	if _, ok := mustGet(t, cl, "precious"); ok {
		t.Fatal("value cannot exist after losing every replica")
	}
	// The key's range has new owners and accepts writes again.
	mustSet(t, cl, "precious", "rewritten")
	if v, _ := mustGet(t, cl, "precious"); v != "rewritten" {
		t.Fatal(v)
	}
}

func TestAutoRemovalRestoresReplicationFactor(t *testing.T) {
	c := newCluster(t, 4, func(cfg *config.Config) {
		cfg.ReplicationFactor = 2
		cfg.RemoveDeadAfter = 300 * time.Millisecond
	})
	cl := c.client()
	for i := 0; i < 200; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "v")
	}
	c.kill("node-4")
	eventually(t, 10*time.Second, "removal", func() bool { return c.nodes["node-1"].Ring().Len() == 3 })
	eventually(t, 10*time.Second, "replication factor restored", func() bool {
		r := c.nodes["node-1"].Ring()
		for i := 0; i < 200; i++ {
			k := fmt.Sprint("k", i)
			for _, o := range r.Owners(k, 2) {
				if _, ok := localValue(c.nodes[o], k); !ok {
					return false
				}
			}
		}
		return true
	})
}

func TestJoinRebalancesWithoutLosingConcurrentWrites(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.ReplicationFactor = 2 })
	cl := c.client()
	const keys = 400
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "0")
	}
	// Keep writing while node-4 joins; remember the last acknowledged value.
	var mu sync.Mutex
	last := map[string]string{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for round := 1; ; round++ {
			for i := 0; i < keys; i += 3 {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprint("k", i), fmt.Sprint(round)
				if _, err := cl.Set(context.Background(), k, []byte(v), 0); err == nil {
					mu.Lock()
					last[k] = v
					mu.Unlock()
				}
			}
		}
	}()
	c.start("node-4")
	c.waitConverged()
	close(stop)
	<-done
	c.antiEntropy() // hand off and drop no-longer-owned copies
	r := c.nodes["node-1"].Ring()
	moved := 0
	for i := 0; i < keys; i++ {
		k := fmt.Sprint("k", i)
		want := "0"
		mu.Lock()
		if v, ok := last[k]; ok {
			want = v
		}
		mu.Unlock()
		got, ok := mustGet(t, cl, k)
		if !ok || got != want {
			t.Fatalf("%s = %q (%v), want %q: acknowledged write lost", k, got, ok, want)
		}
		owners := r.Owners(k, 2)
		if slices.Contains(owners, "node-4") {
			moved++
		}
		for id, n := range c.nodes {
			_, has := localValue(n, k)
			if has != slices.Contains(owners, id) {
				t.Fatalf("%s on %s: has=%v but owners=%v", k, id, has, owners)
			}
		}
	}
	t.Logf("node-4 now owns a replica of %d/%d keys", moved, keys)
	if moved == 0 {
		t.Fatal("join moved no data")
	}
}

func TestGracefulLeaveHandsOffData(t *testing.T) {
	c := newCluster(t, 4, func(cfg *config.Config) { cfg.ReplicationFactor = 2 })
	cl := c.client()
	for i := 0; i < 300; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), fmt.Sprint(i))
	}
	leaver := c.nodes["node-4"]
	if err := leaver.Leave(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.kill("node-4")
	eventually(t, 5*time.Second, "ring shrink", func() bool {
		for _, n := range c.nodes {
			if n.Ring().Len() != 3 {
				return false
			}
		}
		return true
	})
	for i := 0; i < 300; i++ {
		if v, ok := mustGet(t, cl, fmt.Sprint("k", i)); !ok || v != fmt.Sprint(i) {
			t.Fatalf("k%d lost in graceful leave: %q %v", i, v, ok)
		}
	}
}

func TestConcurrentWritesToSameKeyConverge(t *testing.T) {
	c := newCluster(t, 3, nil)
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			n := c.nodes[fmt.Sprintf("node-%d", g%3+1)] // every node, so forwarding races too
			for i := 0; i < 30; i++ {
				call(t, n, &protocol.Message{Op: protocol.OpSet, Key: "hot", Value: []byte(fmt.Sprintf("g%d-%d", g, i))})
			}
		}(g)
	}
	wg.Wait()
	c.antiEntropy()
	var ref store.Entry
	for i, id := range c.ids() {
		e, ok := c.nodes[id].Store().Lookup("hot")
		if !ok {
			t.Fatalf("%s lacks key", id)
		}
		if i == 0 {
			ref = e
		} else if e.Version != ref.Version || string(e.Value) != string(ref.Value) {
			t.Fatalf("replicas diverged: %s has %v=%q, want %v=%q", id, e.Version, e.Value, ref.Version, ref.Value)
		}
	}
}

func TestSlowReplicaDoesNotBlockQuorumWrite(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	key := "slow-key"
	owners := c.nodes["node-1"].Ring().Owners(key, 3)
	primary, slowest := c.nodes[owners[0]], owners[2]
	primary.Faults().SetDelay(slowest, 2*time.Second) // beyond RequestTimeout
	start := time.Now()
	if _, err := cl.Set(context.Background(), key, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("write waited for the slow replica: %v", d)
	}
	if _, ok := localValue(c.nodes[slowest], key); ok {
		t.Fatal("precondition: slow replica should not have the write yet")
	}
	primary.Faults().Reset()
	primary.RebalanceNow(context.Background(), "test")
	if v, ok := localValue(c.nodes[slowest], key); !ok || v != "v" {
		t.Fatal("anti-entropy did not repair the slow replica")
	}
}

func TestWriteFailsWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.RejectWhenMinority = false })
	c.kill("node-2")
	c.kill("node-3")
	n1 := c.nodes["node-1"]
	eventually(t, 5*time.Second, "failure detection", func() bool { return !n1.Members().Routable("node-2") && !n1.Members().Routable("node-3") })
	resp := call(t, n1, &protocol.Message{Op: protocol.OpSet, Key: "k", Value: []byte("v")})
	if resp.Code != protocol.CodeUnavailable {
		t.Fatalf("expected unavailable, got %+v", resp)
	}
	if _, ok := n1.Store().Lookup("k"); ok {
		t.Fatal("write rejected for lack of quorum must not be applied locally")
	}
}

func TestTTLIsAbsoluteAcrossReplicas(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	if _, err := cl.Set(context.Background(), "session", []byte("s"), 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "replication", func() bool {
		for _, n := range c.nodes {
			if _, ok := n.Store().Lookup("session"); !ok {
				return false
			}
		}
		return true
	})
	var exp int64
	for _, n := range c.nodes {
		e, _ := n.Store().Lookup("session")
		if exp == 0 {
			exp = e.ExpireAt
		} else if e.ExpireAt != exp {
			t.Fatal("replicas disagree on absolute expiry")
		}
	}
	// Re-syncing must not extend the lifetime.
	c.antiEntropy()
	for _, n := range c.nodes {
		if e, ok := n.Store().Lookup("session"); ok && e.ExpireAt != exp {
			t.Fatal("anti-entropy changed the expiry")
		}
	}
	eventually(t, 3*time.Second, "expiry everywhere", func() bool {
		if _, ok := mustGet(t, cl, "session"); ok {
			return false
		}
		for _, n := range c.nodes {
			if _, ok := n.Store().Lookup("session"); ok {
				return false
			}
		}
		return true
	})
}

func TestFlappingNodeIsNotRoutedUntilResynced(t *testing.T) {
	c := newCluster(t, 3, nil)
	n1 := c.nodes["node-1"]
	for round := 0; round < 3; round++ {
		c.partition("node-3")
		eventually(t, 5*time.Second, "declared dead", func() bool { return n1.Members().Liveness("node-3").String() == "dead" })
		c.heal()
		// As soon as node-3 answers again it is reachable, but n1 must not route
		// to it until it has resynchronized and announced a new incarnation.
		eventually(t, 5*time.Second, "reachable again", func() bool { return n1.Members().Reachable("node-3") })
		m, _ := n1.Members().Member("node-3")
		if n1.Members().Routable("node-3") && m.Phase != "active" {
			t.Fatal("routable while not active")
		}
		c.waitConverged()
	}
	if got := n1.m.transitions.With("liveness", "dead").Value(); got < 3 {
		t.Fatalf("expected >=3 dead transitions recorded, got %d", got)
	}
}

func TestJoinSurvivesSourceCrashMidSync(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.SyncRateKeys = 300; cfg.SyncBatch = 50 })
	cl := c.client()
	const keys = 600
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), fmt.Sprint(i))
	}
	n4 := c.start("node-4")
	eventually(t, 5*time.Second, "sync in progress", func() bool {
		rec, _ := n4.Status()
		return rec.State == "running" && n4.Store().Stats().Entries > 0
	})
	c.kill("node-2") // interrupt one of the sources
	eventually(t, 20*time.Second, "node-4 active", func() bool { return n4.Ready() })
	rec, _ := n4.Status()
	t.Logf("recovery after source crash: state=%s received=%d errors=%v", rec.State, rec.KeysReceived, rec.Errors)
	// With RF=3 over 4 nodes every key has another surviving owner, so the
	// new node still converges to the full, correct data set for its ranges.
	r := n4.Ring()
	for i := 0; i < keys; i++ {
		k := fmt.Sprint("k", i)
		if slices.Contains(r.Owners(k, 3), "node-4") {
			if v, ok := localValue(n4, k); !ok || v != fmt.Sprint(i) {
				t.Fatalf("node-4 missing %s after interrupted sync", k)
			}
		}
	}
}
