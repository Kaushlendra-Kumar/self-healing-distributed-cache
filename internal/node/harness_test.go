package node

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/transport"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/pkg/client"
)

// The harness runs several real nodes in one test process, each with its own
// TCP listener, store, membership table and background loops. Fault injection
// happens at the transport layer, so partitions and delays affect exactly the
// same code paths as real network failures. Waiting is condition-based
// (eventually) rather than fixed sleeps.

func testConfig(id string) config.Config {
	c := config.Default()
	c.NodeID = id
	c.ListenAddr = "127.0.0.1:0"
	c.AdminAddr = ""
	c.HeartbeatInterval = 40 * time.Millisecond
	c.SuspectAfter = 150 * time.Millisecond
	c.DeadAfter = 400 * time.Millisecond
	c.RemoveDeadAfter = 0
	c.AntiEntropyInterval = 0 // tests trigger anti-entropy explicitly
	c.RequestTimeout = 500 * time.Millisecond
	c.JoinTimeout = 5 * time.Second
	c.SweepInterval = 50 * time.Millisecond
	c.EnableFaults = true
	return c
}

func testLogger(t *testing.T) *slog.Logger {
	if os.Getenv("SHC_TEST_LOG") == "" {
		return nil
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type cluster struct {
	t     *testing.T
	nodes map[string]*Node
	seeds []string
	mod   func(*config.Config)
}

func newCluster(t *testing.T, size int, mod func(*config.Config)) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[string]*Node{}, mod: mod}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Kill()
		}
	})
	for i := 1; i <= size; i++ {
		c.start(fmt.Sprintf("node-%d", i))
	}
	c.waitConverged()
	return c
}

func (c *cluster) start(id string) *Node {
	c.t.Helper()
	cfg := testConfig(id)
	cfg.Seeds = append([]string(nil), c.seeds...)
	if c.mod != nil {
		c.mod(&cfg)
	}
	n, err := New(cfg, Options{Logger: testLogger(c.t)})
	if err != nil {
		c.t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
	if len(c.seeds) == 0 {
		c.seeds = []string{n.Addr()}
	}
	return n
}

func (c *cluster) kill(id string) {
	c.nodes[id].Kill()
	delete(c.nodes, id)
}

func (c *cluster) ids() []string {
	var out []string
	for id := range c.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// waitConverged waits until every running node is ready and considers every
// other running node routable.
func (c *cluster) waitConverged() {
	c.t.Helper()
	eventually(c.t, 10*time.Second, "cluster convergence", func() bool {
		for _, n := range c.nodes {
			if !n.Ready() {
				return false
			}
			for id := range c.nodes {
				if !n.Members().Routable(id) {
					return false
				}
			}
		}
		return true
	})
}

func (c *cluster) client() *client.Client {
	c.t.Helper()
	var seeds []string
	for _, n := range c.nodes {
		seeds = append(seeds, n.Addr())
	}
	cl, err := client.New(context.Background(), client.Config{Seeds: seeds, Timeout: time.Second, RefreshInterval: 50 * time.Millisecond, Attempts: 5})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// partition isolates id from every other node in both directions.
func (c *cluster) partition(id string) {
	c.nodes[id].Faults().Block(transport.AllPeers)
	for oid, n := range c.nodes {
		if oid != id {
			n.Faults().Block(id)
		}
	}
}

func (c *cluster) heal() {
	for _, n := range c.nodes {
		n.Faults().Reset()
	}
}

func (c *cluster) antiEntropy() {
	for _, id := range c.ids() {
		c.nodes[id].RebalanceNow(context.Background(), "test")
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// call sends a raw protocol request to one specific node.
func call(t *testing.T, n *Node, req *protocol.Message) *protocol.Message {
	t.Helper()
	cl := transport.NewClient(n.Addr(), transport.ClientConfig{DefaultTimeout: 3 * time.Second})
	defer cl.Close()
	resp, err := cl.Call(context.Background(), req)
	if err != nil {
		t.Fatalf("call %s on %s: %v", req.Op, n.ID(), err)
	}
	return resp
}

func mustSet(t *testing.T, cl *client.Client, key, val string) {
	t.Helper()
	if _, err := cl.Set(context.Background(), key, []byte(val), 0); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}

func mustGet(t *testing.T, cl *client.Client, key string) (string, bool) {
	t.Helper()
	r, err := cl.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return string(r.Value), r.Found
}

// localValue returns the live value stored on node n itself.
func localValue(n *Node, key string) (string, bool) {
	e, ok := n.Store().Lookup(key)
	if !ok || e.Tombstone {
		return "", false
	}
	return string(e.Value), true
}
