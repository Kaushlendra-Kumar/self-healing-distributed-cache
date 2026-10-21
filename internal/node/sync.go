package node

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
)

// One synchronization primitive serves every data-movement case: a sender
// offers digests (key, version) to a peer, the peer answers with the keys for
// which it holds nothing or an older version, and the sender pushes exactly
// those entries. Because the receiver applies entries with last-writer-wins,
// the exchange is idempotent, safe to interrupt and resume, and can never
// replace newer data with stale data.
//
// It is used for:
//   - join / recovery: a joining node asks each peer to push what it should own
//   - rebalancing: after a ring change, every node pushes keys to their owners
//   - anti-entropy: periodically, to repair replicas that missed writes
//   - graceful leave: a leaving node hands its keys to the future owners
//   - handoff cleanup: a node drops keys it no longer owns once every owner
//     has confirmed holding a version at least as new

// RecoveryStatus describes the most recent join/recovery synchronization.
type RecoveryStatus struct {
	State        string    `json:"state"` // idle, running, complete, incomplete
	Reason       string    `json:"reason,omitempty"`
	Runs         int       `json:"runs"`
	Started      time.Time `json:"started,omitempty"`
	Finished     time.Time `json:"finished,omitempty"`
	Sources      []string  `json:"sources,omitempty"`
	KeysReceived int       `json:"keys_received"`
	Errors       []string  `json:"errors,omitempty"`
}

// RebalanceStatus describes anti-entropy / rebalancing activity.
type RebalanceStatus struct {
	Running      bool      `json:"running"`
	Runs         int       `json:"runs"`
	LastReason   string    `json:"last_reason,omitempty"`
	LastStarted  time.Time `json:"last_started,omitempty"`
	LastDuration string    `json:"last_duration,omitempty"`
	LastOffered  int       `json:"last_keys_offered"`
	LastPushed   int       `json:"last_keys_pushed"`
	LastDropped  int       `json:"last_keys_dropped"`
	LastErrors   []string  `json:"last_errors,omitempty"`
	TotalPushed  int       `json:"total_keys_pushed"`
	TotalDropped int       `json:"total_keys_dropped"`
}

// Status returns recovery and rebalance status.
func (n *Node) Status() (RecoveryStatus, RebalanceStatus) {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	return n.recovery, n.rebalance
}

// limiter is a token bucket that caps migration throughput in keys/second so
// data movement cannot starve client traffic.
type limiter struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newLimiter(perSecond int) *limiter {
	return &limiter{rate: float64(perSecond), tokens: float64(perSecond), last: time.Now()}
}

func (l *limiter) wait(ctx context.Context, n int) error {
	need := min(float64(n), l.rate) // a batch larger than the burst waits for a full bucket
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens = min(l.rate, l.tokens+now.Sub(l.last).Seconds()*l.rate)
		l.last = now
		if l.tokens >= need {
			l.tokens -= need
			l.mu.Unlock()
			return nil
		}
		d := time.Duration((need - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// syncTo offers digests to peer and pushes the entries it needs. It returns
// the number of entries pushed. A nil error means the peer has confirmed it
// now holds every offered key at a version at least as new as the digest.
func (n *Node) syncTo(ctx context.Context, peer string, digests []protocol.Digest) (int, error) {
	if len(digests) == 0 {
		return 0, nil
	}
	if err := n.limiter.wait(ctx, len(digests)); err != nil {
		return 0, err
	}
	resp, err := n.callPeer(ctx, peer, &protocol.Message{Op: protocol.OpSyncDigest, Digests: digests})
	if err == nil {
		err = resp.AsError()
	}
	if err != nil {
		return 0, fmt.Errorf("digest to %s: %w", peer, err)
	}
	n.m.syncDigests.With().Add(uint64(len(digests)))
	pushed := 0
	var batch []store.Entry
	size := 0
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		r, err := n.callPeer(ctx, peer, &protocol.Message{Op: protocol.OpSyncPush, Entries: batch})
		if err == nil {
			err = r.AsError()
		}
		if err != nil {
			return fmt.Errorf("push to %s: %w", peer, err)
		}
		pushed += len(batch)
		n.m.syncPushed.With().Add(uint64(len(batch)))
		batch, size = nil, 0
		return nil
	}
	for _, k := range resp.Keys {
		e, ok := n.store.Lookup(k)
		if !ok {
			continue // expired or evicted since the digest; nothing to hand over
		}
		// JSON base64 inflates values by 4/3; stay well under the frame limit.
		es := len(e.Key) + len(e.Value)*4/3 + 128
		if size+es > n.cfg.MaxFrameBytes/2 {
			if err := send(); err != nil {
				return pushed, err
			}
		}
		batch = append(batch, e)
		size += es
	}
	return pushed, send()
}

func (n *Node) handleSyncDigest(req *protocol.Message) *protocol.Message {
	resp := protocol.OK(req)
	for _, d := range req.Digests {
		n.clock.Observe(d.Version.T)
		cur, ok := n.store.Lookup(d.Key)
		if !ok || d.Version.Newer(cur.Version) {
			resp.Keys = append(resp.Keys, d.Key)
		}
	}
	return resp
}

func (n *Node) handleSyncPush(req *protocol.Message) *protocol.Message {
	resp := protocol.OK(req)
	for _, e := range req.Entries {
		n.clock.Observe(e.Version.T)
		applied, err := n.store.Apply(e)
		if err != nil {
			return protocol.Err(req, protocol.CodeTooLarge, "%v", err)
		}
		if applied {
			resp.Count++
		}
	}
	n.m.syncReceived.With().Add(uint64(resp.Count))
	return resp
}

// handleSyncRequest pushes to a joining node every local key it should own.
func (n *Node) handleSyncRequest(_ context.Context, req *protocol.Message) *protocol.Message {
	if req.Target == nil || req.Target.ID == "" {
		return protocol.Err(req, protocol.CodeBadRequest, "sync request requires a target")
	}
	if res := n.members.Merge([]membership.Member{*req.Target}); res.Changed {
		n.refreshRing()
	}
	n.members.MarkSeen(req.Target.ID)
	select {
	case n.syncSem <- struct{}{}:
		defer func() { <-n.syncSem }()
	case <-n.ctx.Done():
		return protocol.Err(req, protocol.CodeShuttingDown, "node is stopping")
	}
	target := req.Target.ID
	r, fut := n.ring.Load(), n.future.Load()
	if !r.Contains(target) {
		return protocol.Err(req, protocol.CodeBadRequest, "%s is not a ring member in this node's view", target)
	}
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.JoinTimeout)
	defer cancel()
	var batch []protocol.Digest
	pushed := 0
	var err error
	flush := func() bool {
		p, e := n.syncTo(ctx, target, batch)
		pushed += p
		batch = batch[:0]
		if e != nil {
			err = e
			return false
		}
		return true
	}
	n.store.Range(func(e store.Entry) bool {
		if slices.Contains(r.Owners(e.Key, n.cfg.ReplicationFactor), target) ||
			slices.Contains(fut.Owners(e.Key, n.cfg.ReplicationFactor), target) {
			batch = append(batch, protocol.Digest{Key: e.Key, Version: e.Version})
			if len(batch) >= n.cfg.SyncBatch {
				return flush()
			}
		}
		return true
	})
	if err == nil {
		flush()
	}
	if err != nil {
		n.log.Warn("sync to joining node failed", "target", target, "pushed", pushed, "err", err)
		return protocol.Err(req, protocol.CodeInternal, "sync interrupted after %d keys: %v", pushed, err)
	}
	n.log.Info("synced joining node", "target", target, "pushed", pushed)
	resp := protocol.OK(req)
	resp.Count = pushed
	return resp
}

func (n *Node) triggerRecovery(reason string) {
	select {
	case n.recoverCh <- reason:
	default: // a recovery is already queued; it will observe the latest state
	}
}

func (n *Node) triggerRebalance(reason string) {
	select {
	case n.rebalanceCh <- reason:
	default:
	}
}

func (n *Node) recoveryLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case reason := <-n.recoverCh:
			n.runRecovery(reason)
		}
	}
}

func (n *Node) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-n.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// runRecovery brings a joining node up to date before it serves traffic:
// it asks every reachable peer to push the keys this node owns, then
// announces itself active. Until then other nodes do not route reads to it or
// count its write acknowledgements, although they do send it new writes, so
// nothing written during the synchronization is missed.
func (n *Node) runRecovery(reason string) {
	if n.members.Self().Phase != membership.PhaseJoining {
		return
	}
	start := time.Now()
	n.statusMu.Lock()
	n.recovery = RecoveryStatus{State: "running", Reason: reason, Runs: n.recovery.Runs + 1, Started: start}
	n.statusMu.Unlock()
	n.log.Info("recovery started", "reason", reason)

	// Give seeds a chance to answer so we know who to sync from.
	if len(n.cfg.Seeds) > 0 {
		deadline := start.Add(max(10*n.cfg.HeartbeatInterval, 2*time.Second))
		for len(n.members.Peers()) == 0 && time.Now().Before(deadline) {
			if !n.sleep(n.cfg.HeartbeatInterval) {
				return
			}
		}
		if !n.sleep(n.cfg.HeartbeatInterval) { // one more round to learn the full membership
			return
		}
	}

	pending := map[string]bool{}
	for _, p := range n.members.Peers() {
		if n.members.Reachable(p.ID) {
			pending[p.ID] = true
		}
	}
	var sources []string
	for id := range pending {
		sources = append(sources, id)
	}
	slices.Sort(sources)
	received := 0
	var errs []string
	for len(pending) > 0 && time.Since(start) < n.cfg.JoinTimeout && n.ctx.Err() == nil {
		self := n.members.Self()
		var mu sync.Mutex
		var wg sync.WaitGroup
		round := make([]string, 0, len(pending))
		for id := range pending {
			round = append(round, id)
		}
		for _, id := range round {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(n.ctx, n.cfg.JoinTimeout)
				defer cancel()
				resp, err := n.callPeer(ctx, id, &protocol.Message{Op: protocol.OpSyncRequest, Target: &self})
				if err == nil {
					err = resp.AsError()
				}
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, fmt.Sprintf("%s: %v", id, err))
					return
				}
				received += resp.Count
				delete(pending, id)
			}(id)
		}
		wg.Wait()
		for id := range pending {
			if !n.members.Reachable(id) {
				delete(pending, id) // it is down; it cannot have anything for us now
			}
		}
		if len(pending) > 0 && !n.sleep(n.cfg.HeartbeatInterval) {
			return
		}
	}
	if n.ctx.Err() != nil {
		return
	}
	state := "complete"
	if len(pending) > 0 {
		state = "incomplete"
		n.log.Warn("recovery timed out; serving anyway and relying on anti-entropy", "unsynced_peers", len(pending))
	}
	if n.members.Self().Phase == membership.PhaseJoining {
		n.members.SetSelfPhase(membership.PhaseActive)
	}
	n.m.recoveries.With(state).Inc()
	n.statusMu.Lock()
	n.recovery.State = state
	n.recovery.Finished = time.Now()
	n.recovery.Sources = sources
	n.recovery.KeysReceived = received
	if len(errs) > 10 {
		errs = errs[len(errs)-10:]
	}
	n.recovery.Errors = errs
	n.statusMu.Unlock()
	n.log.Info("recovery finished; node active", "state", state, "sources", sources, "keys_received", received,
		"duration", time.Since(start).String())
	n.refreshRing()
	n.triggerRebalance("recovery finished")
}

func (n *Node) rebalanceLoop() {
	var tick <-chan time.Time
	if n.cfg.AntiEntropyInterval > 0 {
		t := time.NewTicker(n.cfg.AntiEntropyInterval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick:
			n.rebalanceOnce(n.ctx, "periodic anti-entropy")
		case reason := <-n.rebalanceCh:
			// Let membership settle so several changes cause one pass.
			if !n.sleep(2 * n.cfg.HeartbeatInterval) {
				return
			}
			select {
			case <-n.rebalanceCh:
			default:
			}
			n.rebalanceOnce(n.ctx, reason)
		}
	}
}

// RebalanceNow runs one synchronous rebalance pass (admin and tests).
func (n *Node) RebalanceNow(ctx context.Context, reason string) RebalanceStatus {
	n.rebalanceOnce(ctx, reason)
	_, rb := n.Status()
	return rb
}

// rebalanceOnce pushes every local key to each reachable owner and drops
// local copies of keys this node no longer owns once all owners confirm.
func (n *Node) rebalanceOnce(ctx context.Context, reason string) {
	n.rebalanceMu.Lock() // serializes the background loop, RebalanceNow and Leave
	defer n.rebalanceMu.Unlock()
	start := time.Now()
	n.statusMu.Lock()
	n.rebalance.Running = true
	n.rebalance.LastReason = reason
	n.rebalance.LastStarted = start
	n.statusMu.Unlock()

	self := n.cfg.NodeID
	r, fut := n.ring.Load(), n.future.Load()
	rf := n.cfg.ReplicationFactor
	type pendingDrop struct {
		e    store.Entry
		need int
	}
	drops := map[string]*pendingDrop{}
	batches := map[string][]protocol.Digest{}
	offered, pushed, dropped := 0, 0, 0
	var errs []string
	flush := func(peer string) {
		ds := batches[peer]
		batches[peer] = nil
		p, err := n.syncTo(ctx, peer, ds)
		pushed += p
		if err != nil {
			n.m.syncErrors.With().Inc()
			if len(errs) < 10 {
				errs = append(errs, err.Error())
			}
			return
		}
		for _, d := range ds {
			pd := drops[d.Key]
			if pd == nil || pd.e.Version != d.Version {
				continue
			}
			if pd.need--; pd.need == 0 {
				if n.store.RemoveIfVersion(d.Key, pd.e.Version) {
					dropped++
				}
				delete(drops, d.Key)
			}
		}
	}
	n.store.Range(func(e store.Entry) bool {
		if ctx.Err() != nil {
			return false
		}
		owners := r.Owners(e.Key, rf)
		if fut != r {
			for _, o := range fut.Owners(e.Key, rf) {
				if !slices.Contains(owners, o) {
					owners = append(owners, o)
				}
			}
		}
		var targets []string
		others := 0
		for _, o := range owners {
			if o == self {
				continue
			}
			others++
			if n.members.Reachable(o) {
				targets = append(targets, o)
			}
		}
		if !slices.Contains(owners, self) && others > 0 && len(targets) == others {
			// Not ours any more: drop after every owner confirms.
			drops[e.Key] = &pendingDrop{e: e, need: others}
		}
		for _, t := range targets {
			offered++
			batches[t] = append(batches[t], protocol.Digest{Key: e.Key, Version: e.Version})
			if len(batches[t]) >= n.cfg.SyncBatch {
				flush(t)
			}
		}
		return true
	})
	for peer, ds := range batches {
		if len(ds) > 0 {
			flush(peer)
		}
	}
	n.m.rebalances.With().Inc()
	n.m.rebalanceDropped.With().Add(uint64(dropped))
	n.statusMu.Lock()
	n.rebalance.Running = false
	n.rebalance.Runs++
	n.rebalance.LastDuration = time.Since(start).String()
	n.rebalance.LastOffered = offered
	n.rebalance.LastPushed = pushed
	n.rebalance.LastDropped = dropped
	n.rebalance.LastErrors = errs
	n.rebalance.TotalPushed += pushed
	n.rebalance.TotalDropped += dropped
	n.statusMu.Unlock()
	if pushed > 0 || dropped > 0 || len(errs) > 0 {
		n.log.Info("rebalance pass", "reason", reason, "offered", offered, "pushed", pushed,
			"dropped", dropped, "errors", len(errs), "duration", time.Since(start).String())
	}
}

// Leave hands this node's data to the members that will own it and removes
// the node from the ring. During the handoff the node keeps serving, and all
// coordinators also send new writes to the future owners.
func (n *Node) Leave(ctx context.Context) error {
	if !n.members.Self().Phase.Serving() {
		return errors.New("node is not active")
	}
	n.members.SetSelfPhase(membership.PhaseLeaving)
	n.refreshRing()
	n.heartbeatRound() // announce leaving so peers start writing to future owners
	n.log.Info("leaving cluster: handing off data")
	var lastErr []string
	for attempt := 0; attempt < 3; attempt++ {
		n.rebalanceOnce(ctx, "graceful leave")
		_, rb := n.Status()
		if lastErr = rb.LastErrors; len(lastErr) == 0 {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	n.members.SetSelfPhase(membership.PhaseLeft)
	n.heartbeatRound() // tell peers we are gone
	if len(lastErr) > 0 {
		return fmt.Errorf("handoff incomplete: %v", lastErr)
	}
	n.log.Info("left cluster")
	return nil
}
