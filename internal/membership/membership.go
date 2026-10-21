// Package membership maintains one node's view of the cluster.
//
// The view has two independent dimensions:
//
//   - Lifecycle phase (joining, active, leaving, left) is owned by each node
//     about itself and disseminated by gossip. Records are ordered by
//     (Generation, Incarnation); Generation changes when a process restarts
//     and Incarnation when the node changes its own phase. "left" is terminal
//     for a generation, so an operator (or the majority's automatic removal)
//     can remove a member and that member cannot refute it without restarting
//     as a new generation.
//
//   - Liveness (alive, suspect, dead) is each observer's local opinion, derived
//     from heartbeat timeouts. It is never gossiped as fact, because a timeout
//     cannot prove a crash: the peer may be slow or partitioned from us only.
//
// When an observer declares a peer dead it records a DeadMark at the peer's
// current (generation, incarnation) and stops routing to it. The mark is
// gossiped to the peer once it is reachable again; the peer must then
// re-enter the joining phase and resynchronize before anyone routes to it,
// which prevents a node that missed writes from serving stale data.
package membership

import (
	"sort"
	"sync"
	"time"
)

// Phase is a node's self-declared lifecycle phase.
type Phase string

// Lifecycle phases.
const (
	PhaseJoining Phase = "joining" // in the ring, receiving data, not serving reads
	PhaseActive  Phase = "active"  // in the ring and serving
	PhaseLeaving Phase = "leaving" // handing data off before departing
	PhaseLeft    Phase = "left"    // removed from the ring
)

// Serving reports whether a member in phase p may coordinate requests.
func (p Phase) Serving() bool { return p == PhaseActive || p == PhaseLeaving }

// Member is the gossiped record describing one node.
type Member struct {
	ID          string `json:"id"`
	Addr        string `json:"addr"`
	AdminAddr   string `json:"admin_addr,omitempty"`
	Generation  int64  `json:"gen"`
	Incarnation uint64 `json:"inc"`
	Phase       Phase  `json:"phase"`
}

// Supersedes reports whether record a should replace record b for the same ID.
func Supersedes(a, b Member) bool {
	if a.Generation != b.Generation {
		return a.Generation > b.Generation
	}
	if (a.Phase == PhaseLeft) != (b.Phase == PhaseLeft) {
		return a.Phase == PhaseLeft
	}
	return a.Incarnation > b.Incarnation
}

// Liveness is the local failure detector's opinion of a peer.
type Liveness int

// Liveness values.
const (
	Alive Liveness = iota
	Suspect
	Dead
)

func (l Liveness) String() string {
	switch l {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	default:
		return "dead"
	}
}

// DeadMark records that an observer declared a member dead at a given record.
type DeadMark struct {
	ID  string `json:"id"`
	Gen int64  `json:"gen"`
	Inc uint64 `json:"inc"`
}

func (d DeadMark) covers(m Member) bool {
	return d.ID == m.ID && d.Gen == m.Generation && d.Inc >= m.Incarnation
}

// Event describes a state transition, for logging and metrics.
type Event struct {
	ID   string
	Kind string // "liveness" or "phase"
	From string
	To   string
}

// Config controls failure detection timeouts.
type Config struct {
	SuspectAfter time.Duration
	DeadAfter    time.Duration
	Now          func() time.Time
}

type peer struct {
	lastSeen  time.Time
	liveness  Liveness
	deadSince time.Time
	mark      *DeadMark
}

// Table is safe for concurrent use.
type Table struct {
	mu      sync.RWMutex
	cfg     Config
	selfID  string
	members map[string]Member
	peers   map[string]*peer
}

// NewTable creates a table containing only self.
func NewTable(self Member, cfg Config) *Table {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SuspectAfter <= 0 {
		cfg.SuspectAfter = 1500 * time.Millisecond
	}
	if cfg.DeadAfter <= cfg.SuspectAfter {
		cfg.DeadAfter = cfg.SuspectAfter * 3
	}
	return &Table{
		cfg:     cfg,
		selfID:  self.ID,
		members: map[string]Member{self.ID: self},
		peers:   map[string]*peer{},
	}
}

// SelfID returns the local node ID.
func (t *Table) SelfID() string { return t.selfID }

// Self returns the local record.
func (t *Table) Self() Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.members[t.selfID]
}

// SetSelfPhase changes the local phase and bumps the incarnation so the new
// record supersedes the old one everywhere.
func (t *Table) SetSelfPhase(p Phase) Member {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.members[t.selfID]
	m.Phase = p
	m.Incarnation++
	t.members[t.selfID] = m
	return m
}

// Rejoin starts a new generation for the local node (used after it learns it
// was removed from the cluster).
func (t *Table) Rejoin(gen int64) Member {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.members[t.selfID]
	if gen <= m.Generation {
		gen = m.Generation + 1
	}
	m.Generation, m.Incarnation, m.Phase = gen, 1, PhaseJoining
	t.members[t.selfID] = m
	return m
}

// MergeResult reports what a Merge changed.
type MergeResult struct {
	Changed     bool // ring-relevant change (new member, phase, address or generation)
	SelfRemoved bool // a peer reports that our current generation was removed
	Events      []Event
}

// Merge folds gossiped records into the table.
func (t *Table) Merge(ms []Member) MergeResult {
	var res MergeResult
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.cfg.Now()
	for _, m := range ms {
		if m.ID == "" {
			continue
		}
		if m.ID == t.selfID {
			self := t.members[t.selfID]
			if m.Generation == self.Generation && m.Phase == PhaseLeft && self.Phase != PhaseLeft {
				res.SelfRemoved = true
			}
			continue // our own record is authoritative otherwise
		}
		cur, ok := t.members[m.ID]
		if ok && !Supersedes(m, cur) {
			continue
		}
		t.members[m.ID] = m
		if !ok || cur.Phase != m.Phase || cur.Addr != m.Addr || cur.Generation != m.Generation {
			res.Changed = true
			from := ""
			if ok {
				from = string(cur.Phase)
			}
			res.Events = append(res.Events, Event{ID: m.ID, Kind: "phase", From: from, To: string(m.Phase)})
		}
		if _, ok := t.peers[m.ID]; !ok && m.Phase != PhaseLeft {
			// Grace period: a newly learned member is presumed alive until it
			// fails to answer within the normal timeouts.
			t.peers[m.ID] = &peer{lastSeen: now}
		}
		if m.Phase == PhaseLeft {
			delete(t.peers, m.ID)
		}
	}
	return res
}

// MarkSeen records direct evidence that id is reachable.
func (t *Table) MarkSeen(id string) *Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, ok := t.peers[id]
	if !ok {
		return nil
	}
	p.lastSeen = t.cfg.Now()
	if p.liveness != Alive {
		ev := &Event{ID: id, Kind: "liveness", From: p.liveness.String(), To: Alive.String()}
		p.liveness = Alive
		return ev
	}
	return nil
}

// Tick re-evaluates liveness from heartbeat timestamps.
func (t *Table) Tick() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.cfg.Now()
	var evs []Event
	for id, p := range t.peers {
		since := now.Sub(p.lastSeen)
		next := Alive
		switch {
		case since >= t.cfg.DeadAfter:
			next = Dead
		case since >= t.cfg.SuspectAfter:
			next = Suspect
		}
		if next == p.liveness || next < p.liveness && p.liveness == Dead {
			// Leaving Dead requires direct evidence (MarkSeen), not just the
			// passage of time.
			continue
		}
		evs = append(evs, Event{ID: id, Kind: "liveness", From: p.liveness.String(), To: next.String()})
		p.liveness = next
		if next == Dead {
			m := t.members[id]
			p.deadSince = now
			p.mark = &DeadMark{ID: id, Gen: m.Generation, Inc: m.Incarnation}
		}
	}
	return evs
}

// DeadMarks returns the marks that still cover the marked member's current
// record, i.e. members that have not yet recovered since we declared them dead.
func (t *Table) DeadMarks() []DeadMark {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []DeadMark
	for id, p := range t.peers {
		if p.mark != nil && p.mark.covers(t.members[id]) {
			out = append(out, *p.mark)
		}
	}
	return out
}

// SelfMarkedDead reports whether any of marks applies to our current record.
func (t *Table) SelfMarkedDead(marks []DeadMark) bool {
	self := t.Self()
	for _, d := range marks {
		if d.covers(self) {
			return true
		}
	}
	return false
}

// Remove marks id as left (operator removal or automatic removal of a member
// that has been dead too long). It reports whether the table changed.
func (t *Table) Remove(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.members[id]
	if !ok || m.Phase == PhaseLeft || id == t.selfID {
		return false
	}
	m.Phase = PhaseLeft
	m.Incarnation++
	t.members[id] = m
	delete(t.peers, id)
	return true
}

// Member returns the record for id.
func (t *Table) Member(id string) (Member, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	m, ok := t.members[id]
	return m, ok
}

// Members returns all records, including left ones, sorted by ID.
func (t *Table) Members() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// RingNodes returns the IDs that participate in key ownership: every member
// that has not left. Liveness does not affect ring membership; a dead member
// keeps its ranges (served by the remaining replicas) until it is removed.
func (t *Table) RingNodes() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for id, m := range t.members {
		if m.Phase != PhaseLeft {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (t *Table) routableLocked(id string) bool {
	m, ok := t.members[id]
	if !ok || !m.Phase.Serving() {
		return false
	}
	if id == t.selfID {
		return true
	}
	p := t.peers[id]
	if p == nil || p.liveness == Dead {
		return false
	}
	return p.mark == nil || !p.mark.covers(m)
}

// Routable reports whether requests may be sent to id for reads and counted
// writes: it is serving, not believed dead, and has resynchronized since it
// was last declared dead.
func (t *Table) Routable(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.routableLocked(id)
}

// Reachable reports whether id is believed to be up (not dead), regardless of
// phase. Joining members are reachable and receive replication traffic.
func (t *Table) Reachable(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id == t.selfID {
		return true
	}
	m, ok := t.members[id]
	p := t.peers[id]
	return ok && m.Phase != PhaseLeft && p != nil && p.liveness != Dead
}

// Liveness returns the local opinion of id.
func (t *Table) Liveness(id string) Liveness {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id == t.selfID {
		return Alive
	}
	if p := t.peers[id]; p != nil {
		return p.liveness
	}
	return Dead
}

// ReachableRingCount returns how many ring members (including self) are
// believed up, and the ring size.
func (t *Table) ReachableRingCount() (up, total int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for id, m := range t.members {
		if m.Phase == PhaseLeft {
			continue
		}
		total++
		if id == t.selfID {
			up++
		} else if p := t.peers[id]; p != nil && p.liveness != Dead {
			up++
		}
	}
	return up, total
}

// DeadLongerThan returns ring members that have been dead for at least d.
func (t *Table) DeadLongerThan(d time.Duration) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.cfg.Now()
	var out []string
	for id, p := range t.peers {
		if p.liveness == Dead && now.Sub(p.deadSince) >= d && t.members[id].Phase != PhaseLeft {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Peers returns every non-left member other than self.
func (t *Table) Peers() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []Member
	for id, m := range t.members {
		if id != t.selfID && m.Phase != PhaseLeft {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MemberStatus is the externally visible view of one member.
type MemberStatus struct {
	Member
	Liveness      string `json:"liveness"`
	Routable      bool   `json:"routable"`
	LastSeenAgoMs int64  `json:"last_seen_ago_ms,omitempty"`
	MarkedDead    bool   `json:"marked_dead,omitempty"`
}

// Status returns a snapshot for admin endpoints and clients.
func (t *Table) Status() []MemberStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.cfg.Now()
	out := make([]MemberStatus, 0, len(t.members))
	for id, m := range t.members {
		st := MemberStatus{Member: m, Liveness: Alive.String(), Routable: t.routableLocked(id)}
		if id != t.selfID {
			if p := t.peers[id]; p != nil {
				st.Liveness = p.liveness.String()
				st.LastSeenAgoMs = now.Sub(p.lastSeen).Milliseconds()
				st.MarkedDead = p.mark != nil && p.mark.covers(m)
			} else {
				st.Liveness = "n/a"
			}
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
