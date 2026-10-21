package node

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

// placement describes where a key lives from this node's point of view.
type placement struct {
	owners   []string // ring preference list (primary first), regardless of liveness
	routable []string // owners that may serve reads and count toward write quorum, in preference order
	extras   []string // future owners (after leaving members depart) that also receive writes
}

func (n *Node) place(key string) placement {
	r := n.ring.Load()
	p := placement{owners: r.Owners(key, n.cfg.ReplicationFactor)}
	for _, o := range p.owners {
		if n.members.Routable(o) {
			p.routable = append(p.routable, o)
		}
	}
	if fut := n.future.Load(); fut != r {
		for _, o := range fut.Owners(key, n.cfg.ReplicationFactor) {
			if !slices.Contains(p.owners, o) {
				p.extras = append(p.extras, o)
			}
		}
	}
	return p
}

func (n *Node) validate(req *protocol.Message) *protocol.Message {
	switch {
	case req.Key == "":
		return protocol.Err(req, protocol.CodeBadRequest, "key must not be empty")
	case len(req.Key) > n.cfg.MaxKeyBytes:
		return protocol.Err(req, protocol.CodeTooLarge, "key is %d bytes; limit is %d", len(req.Key), n.cfg.MaxKeyBytes)
	case len(req.Value) > n.cfg.MaxValueBytes:
		return protocol.Err(req, protocol.CodeTooLarge, "value is %d bytes; limit is %d", len(req.Value), n.cfg.MaxValueBytes)
	case req.TTLMs < 0:
		return protocol.Err(req, protocol.CodeBadRequest, "ttl must not be negative")
	case req.Op == protocol.OpGet && req.Consistency != "" && req.Consistency != config.ReadOne && req.Consistency != config.ReadQuorum:
		return protocol.Err(req, protocol.CodeBadRequest, "consistency must be %q or %q", config.ReadOne, config.ReadQuorum)
	}
	if n.cfg.RejectWhenMinority && n.inMinority() {
		up, total := n.members.ReachableRingCount()
		return protocol.Err(req, protocol.CodeMinority, "node sees %d of %d ring members; refusing requests in a minority partition", up, total)
	}
	return nil
}

// callPeer sends req to the node with the given ID. A nil error means a
// response arrived (which may itself be an error response).
func (n *Node) callPeer(ctx context.Context, id string, req *protocol.Message) (*protocol.Message, error) {
	m, ok := n.members.Member(id)
	if !ok {
		return nil, errors.New("unknown peer " + id)
	}
	out := *req
	out.From = n.cfg.NodeID
	return n.pool.Call(ctx, id, m.Addr, &out)
}

// forward relays a client request to another node that will coordinate it.
func (n *Node) forward(id string, req *protocol.Message) (*protocol.Message, error) {
	ctx, cancel := context.WithTimeout(n.ctx, 2*n.cfg.RequestTimeout)
	defer cancel()
	fwd := *req
	fwd.Forwarded = true
	n.m.forwarded.With(req.Op).Inc()
	resp, err := n.callPeer(ctx, id, &fwd)
	if err != nil {
		n.log.Warn("forward failed", "op", req.Op, "rid", req.RID, "to", id, "err", err)
	}
	return resp, err
}

// route runs local for the first routable owner if it is this node, and
// otherwise forwards to the first routable owner that answers. A forwarded
// request is never forwarded again: if views disagree, the receiving node
// handles it itself when it is a routable owner, or rejects it.
func (n *Node) route(req *protocol.Message, routable []string, local func() *protocol.Message) *protocol.Message {
	self := n.cfg.NodeID
	if req.Forwarded {
		if !slices.Contains(routable, self) {
			return protocol.Err(req, protocol.CodeNotOwner, "forwarded request reached %s, which does not own the key in its view", self)
		}
		return local()
	}
	for _, cand := range routable {
		if cand == self {
			return local()
		}
		if resp, err := n.forward(cand, req); err == nil {
			return resp
		}
		// Transport failure: the outcome at cand is unknown. Fall through to
		// the next owner; for writes this may apply the value twice with
		// different versions, which last-writer-wins resolves.
	}
	return protocol.Err(req, protocol.CodeUnavailable, "no owner of the key is reachable")
}

func (n *Node) handleWrite(_ context.Context, req *protocol.Message) *protocol.Message {
	if r := n.validate(req); r != nil {
		return r
	}
	p := n.place(req.Key)
	if len(p.routable) == 0 {
		return protocol.Err(req, protocol.CodeUnavailable, "no routable owner for key (owners %v)", p.owners)
	}
	return n.route(req, p.routable, func() *protocol.Message { return n.coordinateWrite(req, p) })
}

// coordinateWrite assigns a version, applies the write locally and replicates
// it to the other owners. The write succeeds once W routable owners
// (including this one) have acknowledged it. Replication to the remaining
// owners continues in the background after the client is answered.
func (n *Node) coordinateWrite(req *protocol.Message, p placement) *protocol.Message {
	w := n.cfg.WriteQuorumEffective()
	if len(p.routable) < w {
		// Fail before applying anything: the outcome would be a quorum
		// failure anyway, and refusing early avoids a partial write.
		return protocol.Err(req, protocol.CodeUnavailable,
			"only %d of %d owners routable; write quorum is %d", len(p.routable), len(p.owners), w)
	}
	now := time.Now()
	e := store.Entry{Key: req.Key, Version: version.Version{T: n.clock.Next(), Node: n.cfg.NodeID}}
	if req.Op == protocol.OpDelete {
		e.Tombstone = true
		e.ExpireAt = now.Add(n.cfg.TombstoneTTL).UnixMilli()
	} else {
		e.Value = req.Value
		if req.TTLMs > 0 {
			e.ExpireAt = now.UnixMilli() + req.TTLMs
		}
	}
	if _, err := n.store.Apply(e); err != nil {
		if errors.Is(err, store.ErrTooLarge) {
			return protocol.Err(req, protocol.CodeTooLarge, "%v", err)
		}
		return protocol.Err(req, protocol.CodeInternal, "%v", err)
	}

	acks := 1 // this node is a routable owner
	counted := map[string]bool{}
	for _, o := range p.routable {
		counted[o] = true
	}
	var targets []string
	for _, o := range append(append([]string{}, p.owners...), p.extras...) {
		if o == n.cfg.NodeID {
			continue
		}
		if !n.members.Reachable(o) {
			n.m.replication.With("skipped_unreachable").Inc()
			continue // anti-entropy will repair it once it is back
		}
		targets = append(targets, o)
	}
	// results is buffered for every target, so replication goroutines never
	// block even after the client has been answered.
	results := make(chan replicaResult, len(targets))
	rctx, cancel := context.WithTimeout(n.ctx, n.cfg.RequestTimeout)
	var wg sync.WaitGroup
	pendingCounted := 0
	for _, t := range targets {
		if counted[t] {
			pendingCounted++
		}
		wg.Add(1)
		n.m.replPending.Add(1)
		go func(t string) {
			defer wg.Done()
			defer n.m.replPending.Add(-1)
			resp, err := n.callPeer(rctx, t, &protocol.Message{Op: protocol.OpReplicate, RID: req.RID, Entry: &e})
			ok := err == nil && resp.Status == protocol.StatusOK
			if ok {
				n.m.replication.With("ok").Inc()
			} else {
				n.m.replication.With("error").Inc()
				n.log.Debug("replication failed", "rid", req.RID, "to", t, "err", err)
			}
			results <- replicaResult{t, ok}
		}(t)
	}
	go func() {
		wg.Wait() // replication outlives the client request, bounded by rctx
		cancel()
	}()
	acks += collectAcks(results, counted, pendingCounted, w-acks)
	resp := protocol.OK(req)
	if acks < w {
		resp = protocol.Err(req, protocol.CodeQuorumFailed,
			"write acknowledged by %d of %d required owners; it may be visible on some replicas", acks, w)
	}
	resp.Version = &e.Version
	resp.Acks = acks
	return resp
}

type replicaResult struct {
	id string
	ok bool
}

// collectAcks reads replication results until `needed` acknowledgements from
// counted replicas arrived or no counted replica is still pending, and returns
// the number of acknowledgements.
//
// It deliberately does not watch a context. Every replication goroutine sends
// exactly one result and each call is bounded by the request timeout, so the
// loop always ends. An earlier version also selected on the replication
// context, which is cancelled as soon as the last call returns; select then
// chose randomly between that and acks already buffered, and occasionally
// reported a quorum failure on a healthy cluster.
func collectAcks(results <-chan replicaResult, counted map[string]bool, pending, needed int) int {
	acks := 0
	for acks < needed && pending > 0 {
		r := <-results
		if counted[r.id] {
			pending--
			if r.ok {
				acks++
			}
		}
	}
	return acks
}

func (n *Node) handleReplicate(req *protocol.Message) *protocol.Message {
	if req.Entry == nil || req.Entry.Key == "" {
		return protocol.Err(req, protocol.CodeBadRequest, "replicate requires an entry")
	}
	n.clock.Observe(req.Entry.Version.T)
	applied, err := n.store.Apply(*req.Entry)
	if err != nil {
		return protocol.Err(req, protocol.CodeTooLarge, "%v", err)
	}
	resp := protocol.OK(req)
	if applied {
		resp.Count = 1
	}
	return resp
}

func (n *Node) handleGet(_ context.Context, req *protocol.Message) *protocol.Message {
	if r := n.validate(req); r != nil {
		return r
	}
	p := n.place(req.Key)
	if len(p.routable) == 0 {
		return protocol.Err(req, protocol.CodeUnavailable, "no routable owner for key (owners %v)", p.owners)
	}
	cl := req.Consistency
	if cl == "" {
		cl = n.cfg.ReadConsistency
	}
	if cl == config.ReadQuorum {
		return n.quorumRead(req, p)
	}
	return n.route(req, p.routable, func() *protocol.Message { return n.localRead(req) })
}

func (n *Node) localRead(req *protocol.Message) *protocol.Message {
	resp := protocol.OK(req)
	e, ok := n.store.Get(req.Key)
	if !ok {
		return resp
	}
	fillRead(resp, e)
	return resp
}

func fillRead(resp *protocol.Message, e store.Entry) {
	resp.Found = true
	resp.Value = e.Value
	v := e.Version
	resp.Version = &v
	if e.ExpireAt > 0 {
		resp.TTLMs = max(e.ExpireAt-time.Now().UnixMilli(), 0)
	}
}

func (n *Node) handleReadLocal(req *protocol.Message) *protocol.Message {
	resp := protocol.OK(req)
	if e, ok := n.store.Lookup(req.Key); ok {
		resp.Entry = &e
		resp.Found = true
	}
	return resp
}

// quorumRead asks every routable owner for its copy, waits for R answers,
// returns the newest version (a tombstone counts as a version, so a delete
// is not undone by an older copy) and repairs owners that answered with an
// older copy. It can run on any node; no single coordinator is needed
// because reads do not create versions.
func (n *Node) quorumRead(req *protocol.Message, p placement) *protocol.Message {
	r := n.cfg.ReadQuorum()
	if len(p.routable) < r {
		return protocol.Err(req, protocol.CodeUnavailable, "only %d owners routable; read quorum is %d", len(p.routable), r)
	}
	type answer struct {
		id    string
		entry *store.Entry
		err   error
	}
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RequestTimeout)
	defer cancel()
	answers := make(chan answer, len(p.routable))
	for _, o := range p.routable {
		go func(o string) {
			if o == n.cfg.NodeID {
				if e, ok := n.store.Lookup(req.Key); ok {
					answers <- answer{id: o, entry: &e}
				} else {
					answers <- answer{id: o}
				}
				return
			}
			resp, err := n.callPeer(ctx, o, &protocol.Message{Op: protocol.OpReadLocal, Key: req.Key, RID: req.RID})
			if err == nil {
				err = resp.AsError()
			}
			if err != nil {
				answers <- answer{id: o, err: err}
				return
			}
			answers <- answer{id: o, entry: resp.Entry}
		}(o)
	}
	var got []answer
	failures := 0
	for len(got) < r && len(got)+failures < len(p.routable) {
		a := <-answers // every goroutine sends exactly once and is bounded by ctx
		if a.err != nil {
			failures++
			continue
		}
		got = append(got, a)
	}
	if len(got) < r {
		return protocol.Err(req, protocol.CodeQuorumFailed, "read answered by %d of %d required owners", len(got), r)
	}
	var newest *store.Entry
	for _, a := range got {
		if a.entry != nil && (newest == nil || a.entry.Version.Newer(newest.Version)) {
			newest = a.entry
		}
	}
	if newest != nil {
		for _, a := range got {
			if a.entry == nil || newest.Version.Newer(a.entry.Version) {
				n.readRepair(a.id, *newest, req.RID)
			}
		}
	}
	resp := protocol.OK(req)
	if newest != nil && !newest.Tombstone {
		fillRead(resp, *newest)
	}
	return resp
}

func (n *Node) readRepair(id string, e store.Entry, rid string) {
	n.m.readRepairs.With().Inc()
	if id == n.cfg.NodeID {
		_, _ = n.store.Apply(e)
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RequestTimeout)
		defer cancel()
		_, _ = n.callPeer(ctx, id, &protocol.Message{Op: protocol.OpReplicate, Entry: &e, RID: rid})
	}()
}
