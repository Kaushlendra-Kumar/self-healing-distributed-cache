package node

import (
	"context"
	"sync"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
)

// heartbeatLoop sends a GOSSIP message to every known peer (and to seeds not
// yet identified) each heartbeat interval. The message carries the full
// membership table, so heartbeats double as membership dissemination. This
// all-to-all pattern costs O(n²) messages per interval, which is fine for the
// small clusters this project targets; SWIM-style random probing is the
// documented upgrade path.
func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	n.heartbeatRound()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.heartbeatRound()
		}
	}
}

type gossipTarget struct{ id, addr string }

func (n *Node) gossipTargets() []gossipTarget {
	self := n.members.Self()
	known := map[string]bool{self.Addr: true}
	var out []gossipTarget
	for _, p := range n.members.Peers() {
		known[p.Addr] = true
		out = append(out, gossipTarget{p.ID, p.Addr})
	}
	for _, s := range n.cfg.Seeds {
		if !known[s] {
			known[s] = true
			out = append(out, gossipTarget{"", s})
		}
	}
	return out
}

func (n *Node) gossipMessage() *protocol.Message {
	m := &protocol.Message{Op: protocol.OpGossip, From: n.cfg.NodeID, Members: n.members.Members()}
	// A node that cannot see a majority mostly observes its own isolation;
	// its dead marks would wrongly force healthy majority nodes to resync.
	if !n.inMinority() {
		m.DeadMarks = n.members.DeadMarks()
		m.Down = n.members.DownIDs()
	}
	return m
}

func (n *Node) heartbeatRound() {
	msg := n.gossipMessage()
	ctx, cancel := context.WithTimeout(n.ctx, 2*n.cfg.HeartbeatInterval)
	defer cancel()
	var wg sync.WaitGroup
	for _, t := range n.gossipTargets() {
		wg.Add(1)
		go func(t gossipTarget) {
			defer wg.Done()
			resp, err := n.pool.Call(ctx, t.id, t.addr, msg)
			if err != nil || resp.Status != protocol.StatusOK {
				if t.id != "" {
					n.m.heartbeats.With("fail").Inc()
				}
				return
			}
			n.m.heartbeats.With("ok").Inc()
			n.absorbGossip(resp)
		}(t)
	}
	wg.Wait()
	if n.ctx.Err() != nil {
		return
	}
	n.recordEvents(n.members.Tick())
	n.checkPartition()
	n.autoRemove()
}

func (n *Node) handleGossip(req *protocol.Message) *protocol.Message {
	n.absorbGossip(req)
	resp := n.gossipMessage()
	resp.Op = ""
	resp.ID = req.ID
	resp.Status = protocol.StatusOK
	return resp
}

// absorbGossip merges a peer's view (from a request or a response) and
// reacts to what it says about this node.
func (n *Node) absorbGossip(m *protocol.Message) {
	res := n.members.Merge(m.Members)
	if m.From != "" {
		n.members.RecordReport(m.From, m.Down)
		if ev := n.members.MarkSeen(m.From); ev != nil {
			n.recordEvents([]membership.Event{*ev})
		}
	}
	n.recordEvents(res.Events)
	if res.SelfRemoved {
		self := n.members.Rejoin(time.Now().UnixNano())
		n.log.Warn("this node was removed from the cluster; rejoining as a new generation",
			"reported_by", m.From, "generation", self.Generation)
		n.triggerRecovery("removed from cluster")
		res.Changed = true
	} else if n.members.SelfMarkedDead(m.DeadMarks) && n.members.Self().Phase.Serving() {
		n.members.SetSelfPhase(membership.PhaseJoining)
		n.log.Warn("peer declared this node dead; resynchronizing before serving again", "reported_by", m.From)
		n.triggerRecovery("marked dead by " + m.From)
	}
	if res.Changed {
		n.refreshRing()
	}
}

func (n *Node) recordEvents(evs []membership.Event) {
	for _, ev := range evs {
		n.m.transitions.With(ev.Kind, ev.To).Inc()
		lvl := n.log.Info
		if ev.To == membership.Dead.String() || ev.To == string(membership.PhaseLeft) {
			lvl = n.log.Warn
		}
		lvl("member transition", "member", ev.ID, "kind", ev.Kind, "from", ev.From, "to", ev.To)
	}
}

// inMinority reports whether this node currently sees at most half of the
// ring. Such a node cannot tell a partition from mass failure, so (when
// RejectWhenMinority is set) it refuses requests rather than serve data that
// the majority may be overwriting.
func (n *Node) inMinority() bool { return n.minoritySince.Load() != 0 }

func (n *Node) checkPartition() {
	up, total := n.members.ReachableRingCount()
	minority := total > 1 && up*2 <= total
	since := n.minoritySince.Load()
	switch {
	case minority && since == 0:
		n.minoritySince.Store(time.Now().UnixNano())
		n.log.Warn("lost sight of a majority of the ring", "reachable", up, "ring", total)
	case minority && time.Since(time.Unix(0, since)) >= n.cfg.DeadAfter:
		// Long enough that the majority has likely declared us dead and kept
		// accepting writes without us.
		if !n.wasIsolated.Swap(true) {
			n.log.Warn("isolated in a minority partition; will resynchronize when it heals")
		}
	case !minority && since != 0:
		// Observations made while in the minority describe our own
		// connectivity, not the peers' health: forget them before they are
		// gossiped. Clear first, then leave the minority state.
		n.members.ClearDeadMarks()
		n.minoritySince.Store(0)
		n.log.Info("majority visible again", "reachable", up, "ring", total)
		if n.wasIsolated.Swap(false) {
			// We were cut off long enough for the majority to write without us.
			if n.members.Self().Phase.Serving() {
				n.members.SetSelfPhase(membership.PhaseJoining)
			}
			n.triggerRecovery("healed from minority partition")
		}
	}
}

// autoRemove takes members that have been dead for RemoveDeadAfter out of the
// ring so their ranges get new owners and the replication factor is restored.
// Only a node that sees a strict majority of the ring does this, so the
// minority side of a partition never reassigns ownership on its own, and only
// when a strict majority of ring members (this node plus fresh gossiped
// reports) consider the member dead, so a broken link between two nodes is not
// mistaken for a dead node.
func (n *Node) autoRemove() {
	if n.cfg.RemoveDeadAfter <= 0 || n.inMinority() {
		return
	}
	changed := false
	for _, id := range n.members.DeadLongerThan(n.cfg.RemoveDeadAfter) {
		up, total := n.members.ReachableRingCount()
		if up*2 <= total {
			break
		}
		if votes, ringSize := n.members.RemovalVotes(id, n.cfg.DeadAfter); votes*2 <= ringSize {
			continue // not corroborated by a majority: possibly only our link to it is broken
		}
		if n.members.Remove(id) {
			changed = true
			n.m.removals.With("auto").Inc()
			n.log.Warn("removed member after prolonged failure; restoring replication on remaining nodes",
				"member", id, "dead_for", n.cfg.RemoveDeadAfter.String())
		}
	}
	if changed {
		n.refreshRing()
	}
}

// RemoveMember removes a member by operator request.
func (n *Node) RemoveMember(id string) bool {
	if !n.members.Remove(id) {
		return false
	}
	n.m.removals.With("operator").Inc()
	n.log.Warn("member removed by operator", "member", id)
	n.refreshRing()
	return true
}
