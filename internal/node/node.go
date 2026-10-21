// Package node wires the storage engine, ring, membership, transport and
// background loops into one cache node process.
//
// File map:
//
//	node.go        lifecycle (Start, Stop, Kill, Leave), ring publication, dispatch
//	coordinator.go client operations: routing, forwarding, replication, quorum reads
//	gossip.go      heartbeats, failure detection, partition detection, auto-removal
//	sync.go        anti-entropy, rebalancing, join/recovery synchronization, handoff
//	admin.go       HTTP health, readiness, metrics, cluster and admin endpoints
//	metrics.go     metric definitions
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/ring"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/transport"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

// Node is one cache server.
type Node struct {
	cfg     config.Config
	log     *slog.Logger
	store   *store.Store
	clock   *version.Clock
	members *membership.Table
	faults  *transport.Faults
	pool    *transport.Pool
	server  *transport.Server
	admin   *http.Server
	m       *nodeMetrics
	limiter *limiter
	now     func() time.Time

	ring   atomic.Pointer[ring.Ring] // current ownership
	future atomic.Pointer[ring.Ring] // ownership once leaving members are gone

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	rebalanceCh chan string
	recoverCh   chan string
	syncSem     chan struct{}
	ridSeq      atomic.Uint64
	stopOnce    sync.Once
	stopped     atomic.Bool

	minoritySince atomic.Int64 // unix nanos when the node lost sight of a majority; 0 = has majority
	wasIsolated   atomic.Bool

	rebalanceMu sync.Mutex
	statusMu    sync.Mutex
	recovery    RecoveryStatus
	rebalance   RebalanceStatus
}

// Options carries dependencies that are not plain configuration.
type Options struct {
	Logger *slog.Logger
	// Now is the node's wall clock, used for versions (HLC), TTL expiry and
	// failure-detector timeouts. time.Now if nil; tests inject skew here.
	Now func() time.Time
}

// New validates cfg and constructs a node. Call Start to bind and run it.
func New(cfg config.Config, opts Options) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	st, err := store.New(store.Config{
		MaxBytes:      cfg.MaxMemoryBytes,
		MaxEntries:    cfg.MaxEntries,
		Policy:        cfg.Eviction,
		TombstoneTTL:  cfg.TombstoneTTL,
		SweepInterval: cfg.SweepInterval,
		Now:           opts.Now,
	})
	if err != nil {
		return nil, err
	}
	var faults *transport.Faults
	if cfg.EnableFaults {
		faults = transport.NewFaults()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:         cfg,
		log:         opts.Logger.With("node", cfg.NodeID),
		store:       st,
		clock:       version.NewClock(opts.Now),
		faults:      faults,
		pool:        transport.NewPool(transport.ClientConfig{MaxFrame: cfg.MaxFrameBytes, DefaultTimeout: cfg.RequestTimeout}, faults),
		limiter:     newLimiter(cfg.SyncRateKeys),
		now:         opts.Now,
		ctx:         ctx,
		cancel:      cancel,
		rebalanceCh: make(chan string, 1),
		recoverCh:   make(chan string, 1),
		syncSem:     make(chan struct{}, 2),
	}
	n.members = membership.NewTable(membership.Member{ID: cfg.NodeID}, membership.Config{
		SuspectAfter: cfg.SuspectAfter, DeadAfter: cfg.DeadAfter, Now: opts.Now,
	})
	n.m = newNodeMetrics(n)
	n.refreshRing()
	return n, nil
}

// Start binds the listeners, starts all background loops and begins joining
// the cluster. It returns once the node is accepting connections; the node
// becomes ready (serving) after its join synchronization completes.
func (n *Node) Start() error {
	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", n.cfg.ListenAddr, err)
	}
	addr := n.cfg.AdvertiseAddr
	if addr == "" {
		addr = advertiseFor(ln.Addr())
	}
	adminAddr := n.cfg.AdminAdvertise
	var adminLn net.Listener
	if n.cfg.AdminAddr != "" {
		adminLn, err = net.Listen("tcp", n.cfg.AdminAddr)
		if err != nil {
			ln.Close()
			return fmt.Errorf("listen admin %s: %w", n.cfg.AdminAddr, err)
		}
		if adminAddr == "" {
			adminAddr = advertiseFor(adminLn.Addr())
		}
	}
	// A fresh generation per process start: peers can tell a restarted node
	// (which lost its memory) from a node that was merely unreachable.
	self := membership.Member{
		ID:          n.cfg.NodeID,
		Addr:        addr,
		AdminAddr:   adminAddr,
		Generation:  time.Now().UnixNano(),
		Incarnation: 1,
		Phase:       membership.PhaseJoining,
	}
	n.members = membership.NewTable(self, membership.Config{SuspectAfter: n.cfg.SuspectAfter, DeadAfter: n.cfg.DeadAfter, Now: n.now})
	n.refreshRing()

	n.server = transport.NewServer(ln, n.handle, transport.ServerConfig{
		MaxFrame: n.cfg.MaxFrameBytes, Faults: n.faults, Logger: n.log,
	})
	n.goLoop(func() {
		if err := n.server.Serve(); err != nil {
			n.log.Error("data listener stopped", "err", err)
		}
	})
	if adminLn != nil {
		n.admin = &http.Server{Handler: n.adminHandler(), ReadHeaderTimeout: 5 * time.Second}
		n.goLoop(func() {
			if err := n.admin.Serve(adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				n.log.Error("admin listener stopped", "err", err)
			}
		})
	}
	n.goLoop(n.heartbeatLoop)
	n.goLoop(n.rebalanceLoop)
	n.goLoop(n.recoveryLoop)
	n.log.Info("node started", "addr", addr, "admin", adminAddr, "generation", self.Generation,
		"rf", n.cfg.ReplicationFactor, "w", n.cfg.WriteQuorumEffective(), "seeds", n.cfg.Seeds)
	n.triggerRecovery("startup")
	return nil
}

func advertiseFor(a net.Addr) string {
	ta, ok := a.(*net.TCPAddr)
	if !ok || !ta.IP.IsUnspecified() {
		return a.String()
	}
	return net.JoinHostPort("127.0.0.1", fmt.Sprint(ta.Port))
}

func (n *Node) goLoop(f func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

// ID returns the node ID.
func (n *Node) ID() string { return n.cfg.NodeID }

// Addr returns the advertised data-plane address.
func (n *Node) Addr() string { return n.members.Self().Addr }

// AdminAddr returns the advertised admin address.
func (n *Node) AdminAddr() string { return n.members.Self().AdminAddr }

// Members exposes the membership table (read-only use).
func (n *Node) Members() *membership.Table { return n.members }

// Store exposes the local store (for tests and admin endpoints).
func (n *Node) Store() *store.Store { return n.store }

// Faults returns the fault injector, or nil if disabled.
func (n *Node) Faults() *transport.Faults { return n.faults }

// Ring returns the current ownership ring.
func (n *Node) Ring() *ring.Ring { return n.ring.Load() }

// Ready reports whether the node serves requests: it has finished joining and
// can see a majority of the ring.
func (n *Node) Ready() bool {
	return n.members.Self().Phase == membership.PhaseActive && !n.inMinority()
}

// Kill stops the node abruptly, as a crash would: no handoff, no goodbye.
// In-memory data is discarded.
func (n *Node) Kill() { n.shutdown() }

// Stop shuts the node down, first leaving the cluster gracefully if
// configured to do so.
func (n *Node) Stop(ctx context.Context) {
	if n.cfg.LeaveOnShutdown && !n.stopped.Load() {
		if err := n.Leave(ctx); err != nil {
			n.log.Warn("graceful leave failed", "err", err)
		}
	}
	n.shutdown()
}

func (n *Node) shutdown() {
	n.stopOnce.Do(func() {
		n.stopped.Store(true)
		n.cancel()
		if n.admin != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = n.admin.Shutdown(ctx)
			cancel()
		}
		if n.server != nil {
			n.server.Close()
		}
		n.pool.Close()
		n.wg.Wait()
		n.store.Close()
		n.log.Info("node stopped")
	})
}

// refreshRing rebuilds the ownership rings from the membership table and
// schedules a rebalance if ownership changed.
func (n *Node) refreshRing() {
	var all, staying []string
	for _, m := range n.members.Members() {
		if m.Phase == membership.PhaseLeft {
			continue
		}
		all = append(all, m.ID)
		if m.Phase != membership.PhaseLeaving {
			staying = append(staying, m.ID)
		}
	}
	changed := false
	if cur := n.ring.Load(); cur == nil || !slices.Equal(cur.Nodes(), all) {
		n.ring.Store(ring.New(n.cfg.VirtualNodes, all))
		changed = true
	}
	if fut := n.future.Load(); fut == nil || !slices.Equal(fut.Nodes(), staying) {
		if slices.Equal(staying, all) {
			n.future.Store(n.ring.Load())
		} else {
			n.future.Store(ring.New(n.cfg.VirtualNodes, staying))
		}
		changed = true
	}
	if changed {
		n.m.ringChanges.With().Inc()
		n.log.Info("ring updated", "members", all, "staying", staying)
		n.triggerRebalance("ring change")
	}
}

func (n *Node) newRID() string {
	return fmt.Sprintf("%s-%d", n.cfg.NodeID, n.ridSeq.Add(1))
}

// handle dispatches one protocol request.
func (n *Node) handle(ctx context.Context, req *protocol.Message) *protocol.Message {
	if req.RID == "" {
		req.RID = n.newRID()
	}
	start := time.Now()
	var resp *protocol.Message
	switch req.Op {
	case protocol.OpGet:
		resp = n.handleGet(ctx, req)
	case protocol.OpSet, protocol.OpDelete:
		resp = n.handleWrite(ctx, req)
	case protocol.OpMembers:
		resp = n.handleMembers(req)
	case protocol.OpPing:
		resp = protocol.OK(req)
		resp.From = n.cfg.NodeID
	case protocol.OpReplicate:
		resp = n.handleReplicate(req)
	case protocol.OpReadLocal:
		resp = n.handleReadLocal(req)
	case protocol.OpGossip:
		resp = n.handleGossip(req)
	case protocol.OpSyncDigest:
		resp = n.handleSyncDigest(req)
	case protocol.OpSyncPush:
		resp = n.handleSyncPush(req)
	case protocol.OpSyncRequest:
		resp = n.handleSyncRequest(ctx, req)
	default:
		resp = protocol.Err(req, protocol.CodeUnknownOp, "unknown operation %q", req.Op)
	}
	resp.RID = req.RID
	if isClientOp(req.Op) {
		result := resp.Status
		if resp.Status == protocol.StatusError {
			result = resp.Code
		} else if req.Op == protocol.OpGet && !resp.Found {
			result = "not_found"
		}
		n.m.requests.With(req.Op, result).Inc()
		n.m.latency.With(req.Op).ObserveDuration(time.Since(start))
		n.log.Debug("request", "op", req.Op, "rid", req.RID, "result", result, "fwd", req.Forwarded,
			"dur_us", time.Since(start).Microseconds())
	}
	return resp
}

func isClientOp(op string) bool {
	return op == protocol.OpGet || op == protocol.OpSet || op == protocol.OpDelete
}

// Do executes a client request in-process (used by the HTTP gateway).
func (n *Node) Do(ctx context.Context, req *protocol.Message) *protocol.Message {
	return n.handle(ctx, req)
}

func (n *Node) handleMembers(req *protocol.Message) *protocol.Message {
	resp := protocol.OK(req)
	for _, m := range n.members.Members() {
		if m.Phase == membership.PhaseLeft {
			continue
		}
		resp.Members = append(resp.Members, m)
		if n.members.Routable(m.ID) {
			resp.Routable = append(resp.Routable, m.ID)
		}
	}
	resp.RF = n.cfg.ReplicationFactor
	resp.VNodes = n.cfg.VirtualNodes
	resp.From = n.cfg.NodeID
	return resp
}

// Done is closed when the node has begun shutting down.
func (n *Node) Done() <-chan struct{} { return n.ctx.Done() }
