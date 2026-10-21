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

// writeLog tracks, per key, the last acknowledged value and the values of
// later writes whose outcome is unknown (they returned an error, but may have
// been applied: see CONSISTENCY_MODEL "Write semantics"). The final value
// must be one of those; anything older means an acknowledged write was lost.
type writeLog struct {
	mu      sync.Mutex
	acked   map[string]string
	unknown map[string]map[string]bool
}

func newWriteLog() *writeLog {
	return &writeLog{acked: map[string]string{}, unknown: map[string]map[string]bool{}}
}

func (w *writeLog) record(k, v string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil {
		w.acked[k] = v
		delete(w.unknown, k)
		return
	}
	if w.unknown[k] == nil {
		w.unknown[k] = map[string]bool{}
	}
	w.unknown[k][v] = true
}

func (w *writeLog) check(t *testing.T, k, initial, got string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	want, ok := w.acked[k]
	if !ok {
		want = initial
	}
	if got != want && !w.unknown[k][got] {
		t.Fatalf("%s = %q, want %q (or a later unacknowledged write): acknowledged write lost", k, got, want)
	}
}

func TestJoinRebalancesWithoutLosingConcurrentWrites(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.ReplicationFactor = 2 })
	cl := c.client()
	const keys = 400
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "0")
	}
	// Keep writing while node-4 joins; remember the last acknowledged value.
	log := newWriteLog()
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
				_, err := cl.Set(context.Background(), k, []byte(v), 0)
				log.record(k, v, err)
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
		got, ok := mustGet(t, cl, k)
		if !ok {
			t.Fatalf("%s missing", k)
		}
		log.check(t, k, "0", got)
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

// Regression test: the coordinator once raced its own context cancellation
// against buffered acknowledgements and occasionally reported quorum_failed
// on a healthy cluster (found by the benchmark, roughly 1 in 50k writes).
// collectAcks must count acknowledgements that are already buffered.
func TestCollectAcksCountsBufferedResults(t *testing.T) {
	counted := map[string]bool{"a": true, "b": true}
	for i := 0; i < 1000; i++ {
		ch := make(chan replicaResult, 3)
		ch <- replicaResult{"x", true} // a future owner: not counted
		ch <- replicaResult{"a", false}
		ch <- replicaResult{"b", true}
		if got := collectAcks(ch, counted, 2, 2); got != 1 {
			t.Fatalf("got %d acks, want 1", got)
		}
	}
	ch := make(chan replicaResult, 2)
	ch <- replicaResult{"a", true}
	ch <- replicaResult{"b", true}
	if got := collectAcks(ch, counted, 2, 1); got != 1 || len(ch) != 1 {
		t.Fatal("must stop as soon as the quorum is reached")
	}
}

func TestHealthyClusterNeverReportsQuorumFailure(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1500; i++ {
				if _, err := cl.Set(context.Background(), fmt.Sprint("k", (g*1500+i)%3000), []byte("v"), 0); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("healthy cluster rejected a write: %v", err)
	}
}

// Non-transitive partition: node-1 and node-3 cannot talk to each other, but
// both can talk to node-2. Each side still sees a majority, so neither may
// remove the other on its own opinion alone.
func TestNonTransitivePartitionDoesNotRemoveMembers(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.RemoveDeadAfter = 300 * time.Millisecond })
	n1, n3 := c.nodes["node-1"], c.nodes["node-3"]
	n1.Faults().Block("node-3")
	n3.Faults().Block("node-1")
	eventually(t, 5*time.Second, "mutual dead opinions", func() bool {
		return n1.Members().Liveness("node-3").String() == "dead" && n3.Members().Liveness("node-1").String() == "dead"
	})
	time.Sleep(time.Second) // well past RemoveDeadAfter: removal must not happen
	for _, id := range c.ids() {
		if got := c.nodes[id].m.removals.With("auto").Value(); got != 0 {
			t.Fatalf("%s removed %d members that the rest of the cluster still sees", id, got)
		}
		if rec, _ := c.nodes[id].Status(); rec.Reason == "removed from cluster" {
			t.Fatalf("%s was removed and had to rejoin", id)
		}
	}
	cl := c.client()
	mustSet(t, cl, "k", "v")
	if v, _ := mustGet(t, cl, "k"); v != "v" {
		t.Fatal(v)
	}
	c.heal()
	c.waitConverged()
}

// keyWithPrimary finds a key whose first ring owner is id.
func keyWithPrimary(t *testing.T, n *Node, id string) string {
	for i := 0; i < 10000; i++ {
		k := fmt.Sprint("skew-", i)
		if n.Ring().Primary(k) == id {
			return k
		}
	}
	t.Fatal("no key found")
	return ""
}

// A coordinator whose clock runs 10 s fast writes v1; it then dies and a
// coordinator with a correct clock writes v2. Without the hybrid logical clock,
// v2 would carry a lower timestamp and lose to v1 on every replica. Because
// every node advances its clock past the versions it receives, v2 wins.
func TestClockSkewDoesNotReorderSequentialWrites(t *testing.T) {
	c := newSkewedCluster(t, 3, nil, map[string]time.Duration{"node-1": 10 * time.Second})
	cl := c.client()
	key := keyWithPrimary(t, c.nodes["node-2"], "node-1")
	v1, err := cl.Set(context.Background(), key, []byte("v1"), 0)
	if err != nil || v1.Node != "node-1" {
		t.Fatalf("v1 should be coordinated by the skewed node-1: %v %v", v1, err)
	}
	eventually(t, 3*time.Second, "v1 replicated", func() bool {
		_, ok2 := localValue(c.nodes["node-2"], key)
		_, ok3 := localValue(c.nodes["node-3"], key)
		return ok2 && ok3
	})
	c.kill("node-1")
	eventually(t, 5*time.Second, "node-1 dead", func() bool { return !c.nodes["node-2"].Members().Routable("node-1") })
	v2, err := cl.Set(context.Background(), key, []byte("v2"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("v1=%v v2=%v", v1, v2)
	if v2.Node == "node-1" || !v2.Newer(v1) {
		t.Fatalf("later write %v does not supersede skewed earlier write %v", v2, v1)
	}
	for _, id := range c.ids() {
		if v, _ := localValue(c.nodes[id], key); v != "v2" {
			t.Fatalf("%s holds %q; the later write was lost to clock skew", id, v)
		}
	}
}

// Documents a known limitation: expiry uses each node's own clock. A replica
// whose clock runs ahead treats a short-TTL value as already expired.
func TestClockSkewShortensTTLOnFastReplica(t *testing.T) {
	c := newSkewedCluster(t, 3, nil, map[string]time.Duration{"node-3": 10 * time.Second})
	key := keyWithPrimary(t, c.nodes["node-1"], "node-1")
	resp := call(t, c.nodes["node-1"], &protocol.Message{Op: protocol.OpSet, Key: key, Value: []byte("v"), TTLMs: 5000})
	if resp.Status != protocol.StatusOK {
		t.Fatal(resp.Error)
	}
	eventually(t, 3*time.Second, "replica with correct clock stores it", func() bool {
		_, ok := localValue(c.nodes["node-2"], key)
		return ok
	})
	c.antiEntropy()
	if _, ok := localValue(c.nodes["node-3"], key); ok {
		t.Fatal("a replica 10 s ahead should consider a 5 s TTL already expired")
	}
	if _, ok := localValue(c.nodes["node-1"], key); !ok {
		t.Fatal("coordinator lost the value")
	}
}

// A coordinator must never acknowledge a write that its own store rejected as
// older. If a newer version reached the local store without passing through
// the clock (any such path is a bug), the coordinator still issues a version
// above it.
func TestCoordinatorVersionExceedsLocalCopy(t *testing.T) {
	c := newCluster(t, 1, func(cfg *config.Config) { cfg.ReplicationFactor = 1 })
	n := c.nodes["node-1"]
	future := version.Version{T: uint64(time.Now().Add(time.Hour).UnixMilli()) << 16, Node: "elsewhere"}
	if ok, _ := n.Store().Apply(store.Entry{Key: "k", Value: []byte("old"), Version: future}); !ok {
		t.Fatal("setup")
	}
	resp := call(t, n, &protocol.Message{Op: protocol.OpSet, Key: "k", Value: []byte("new")})
	if resp.Status != protocol.StatusOK {
		t.Fatal(resp.Error)
	}
	if v, _ := localValue(n, "k"); v != "new" {
		t.Fatalf("write acknowledged but discarded: store holds %q", v)
	}
}

// Several membership changes at once, under continuous writes: two nodes join
// while another crashes and is removed. No acknowledged write may be lost and
// every key must end up exactly on its owners in the final ring.
func TestConcurrentMembershipChangesUnderLoad(t *testing.T) {
	c := newCluster(t, 4, func(cfg *config.Config) {
		cfg.ReplicationFactor = 2
		cfg.RemoveDeadAfter = 300 * time.Millisecond
	})
	cl := c.client()
	const keys = 400
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "0")
	}
	log := newWriteLog()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for round := 1; ; round++ {
			for i := 0; i < keys; i += 5 {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprint("k", i), fmt.Sprint(round)
				_, err := cl.Set(context.Background(), k, []byte(v), 0)
				log.record(k, v, err)
			}
		}
	}()
	c.start("node-5")
	c.kill("node-2")
	c.start("node-6")
	eventually(t, 15*time.Second, "node-2 removed everywhere", func() bool {
		for _, n := range c.nodes {
			if n.Ring().Contains("node-2") {
				return false
			}
		}
		return true
	})
	c.waitConverged()
	close(stop)
	<-done
	c.antiEntropy()
	c.antiEntropy() // a second pass drops copies whose new owners confirmed in the first
	r := c.nodes["node-1"].Ring()
	if r.Len() != 5 {
		t.Fatalf("ring %v", r.Nodes())
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprint("k", i)
		got, found := mustGet(t, cl, k)
		if !found {
			t.Fatalf("%s missing", k)
		}
		log.check(t, k, "0", got)
		owners := r.Owners(k, 2)
		for id, n := range c.nodes {
			_, has := localValue(n, k)
			if has != slices.Contains(owners, id) {
				t.Fatalf("%s on %s: has=%v but owners=%v", k, id, has, owners)
			}
		}
	}
}

// A node joins while another is cut off in a minority partition. The joiner
// syncs from the majority and becomes active; the isolated node refuses
// requests meanwhile and resyncs after the heal. No acknowledged write is lost
// and every key ends exactly on its owners in the four-node ring.
func TestJoinDuringPartition(t *testing.T) {
	c := newCluster(t, 3, nil)
	cl := c.client()
	const keys = 300
	for i := 0; i < keys; i++ {
		mustSet(t, cl, fmt.Sprint("k", i), "0")
	}
	n3 := c.nodes["node-3"]
	c.partition("node-3")
	eventually(t, 5*time.Second, "partition detected", func() bool {
		return !c.nodes["node-1"].Members().Routable("node-3") && n3.inMinority()
	})

	log := newWriteLog()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for round := 1; ; round++ {
			for i := 0; i < keys; i += 4 {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprint("k", i), fmt.Sprint(round)
				_, err := cl.Set(context.Background(), k, []byte(v), 0)
				log.record(k, v, err)
			}
		}
	}()

	n4 := c.start("node-4")
	n4.Faults().Block("node-3") // node-4 joins on the majority side
	eventually(t, 10*time.Second, "node-4 active on the majority side", func() bool {
		return n4.Ready() && c.nodes["node-1"].Members().Routable("node-4")
	})
	if resp := call(t, n3, &protocol.Message{Op: protocol.OpGet, Key: "k0"}); resp.Code != protocol.CodeMinority {
		t.Fatalf("isolated node served a request during the join: %+v", resp)
	}

	c.heal()
	c.waitConverged()
	close(stop)
	<-done
	c.antiEntropy()
	c.antiEntropy()

	r := c.nodes["node-1"].Ring()
	if r.Len() != 4 {
		t.Fatalf("ring %v", r.Nodes())
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprint("k", i)
		got, found := mustGet(t, cl, k)
		if !found {
			t.Fatalf("%s missing", k)
		}
		log.check(t, k, "0", got)
		owners := r.Owners(k, 3)
		for id, n := range c.nodes {
			_, has := localValue(n, k)
			if has != slices.Contains(owners, id) {
				t.Fatalf("%s on %s: has=%v but owners=%v", k, id, has, owners)
			}
		}
		// The previously isolated node must hold the newest value, not its stale copy.
		if slices.Contains(owners, "node-3") {
			v3, _ := localValue(n3, k)
			if v3 != got {
				t.Fatalf("%s: node-3 holds %q, cluster value %q", k, v3, got)
			}
		}
	}
}
