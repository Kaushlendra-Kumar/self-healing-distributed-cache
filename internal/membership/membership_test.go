package membership

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestTable(c *clock) *Table {
	return NewTable(Member{ID: "a", Addr: "a:1", Generation: 1, Incarnation: 1, Phase: PhaseActive},
		Config{SuspectAfter: time.Second, DeadAfter: 3 * time.Second, Now: c.now})
}

func TestSupersedes(t *testing.T) {
	base := Member{ID: "x", Generation: 5, Incarnation: 3, Phase: PhaseActive}
	cases := []struct {
		name string
		m    Member
		want bool
	}{
		{"higher incarnation", Member{ID: "x", Generation: 5, Incarnation: 4, Phase: PhaseActive}, true},
		{"lower incarnation", Member{ID: "x", Generation: 5, Incarnation: 2, Phase: PhaseActive}, false},
		{"newer generation", Member{ID: "x", Generation: 6, Incarnation: 1, Phase: PhaseJoining}, true},
		{"older generation", Member{ID: "x", Generation: 4, Incarnation: 99, Phase: PhaseActive}, false},
		{"left is terminal within generation", Member{ID: "x", Generation: 5, Incarnation: 1, Phase: PhaseLeft}, true},
	}
	for _, c := range cases {
		if got := Supersedes(c.m, base); got != c.want {
			t.Errorf("%s: got %v", c.name, got)
		}
	}
	left := Member{ID: "x", Generation: 5, Incarnation: 1, Phase: PhaseLeft}
	if Supersedes(Member{ID: "x", Generation: 5, Incarnation: 100, Phase: PhaseActive}, left) {
		t.Error("a node must not refute its removal without a new generation")
	}
}

func TestMergeAndFailureDetection(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	tb := newTestTable(c)
	res := tb.Merge([]Member{{ID: "b", Addr: "b:1", Generation: 1, Incarnation: 1, Phase: PhaseActive}})
	if !res.Changed || !tb.Routable("b") {
		t.Fatal("new active member should be routable during its grace period")
	}
	c.advance(1500 * time.Millisecond)
	evs := tb.Tick()
	if len(evs) != 1 || evs[0].To != "suspect" || !tb.Routable("b") {
		t.Fatalf("expected suspect (still routable), got %+v", evs)
	}
	c.advance(2 * time.Second)
	tb.Tick()
	if tb.Liveness("b") != Dead || tb.Routable("b") || tb.Reachable("b") {
		t.Fatal("expected dead and unroutable")
	}
	if marks := tb.DeadMarks(); len(marks) != 1 || marks[0].Inc != 1 {
		t.Fatalf("marks %+v", marks)
	}
	// b answers again: reachable, but not routable until it resynchronizes.
	if ev := tb.MarkSeen("b"); ev == nil || ev.To != "alive" {
		t.Fatal("expected transition to alive")
	}
	if !tb.Reachable("b") || tb.Routable("b") {
		t.Fatal("returning node must not be routable before recovery")
	}
	// b learns it was marked dead, rejoins, then becomes active again.
	tb.Merge([]Member{{ID: "b", Addr: "b:1", Generation: 1, Incarnation: 2, Phase: PhaseJoining}})
	if tb.Routable("b") {
		t.Fatal("joining member must not be routable")
	}
	tb.Merge([]Member{{ID: "b", Addr: "b:1", Generation: 1, Incarnation: 3, Phase: PhaseActive}})
	if !tb.Routable("b") || len(tb.DeadMarks()) != 0 {
		t.Fatal("recovered member should be routable and the mark cleared")
	}
}

func TestSelfMarkedDeadAndRemoved(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	tb := newTestTable(c)
	if !tb.SelfMarkedDead([]DeadMark{{ID: "a", Gen: 1, Inc: 1}}) {
		t.Fatal("mark at current incarnation applies")
	}
	tb.SetSelfPhase(PhaseJoining)
	if tb.SelfMarkedDead([]DeadMark{{ID: "a", Gen: 1, Inc: 1}}) {
		t.Fatal("stale mark must not apply after incarnation bump")
	}
	res := tb.Merge([]Member{{ID: "a", Generation: 1, Incarnation: 9, Phase: PhaseLeft}})
	if !res.SelfRemoved {
		t.Fatal("expected SelfRemoved")
	}
	m := tb.Rejoin(1)
	if m.Generation != 2 || m.Phase != PhaseJoining || m.Incarnation != 1 {
		t.Fatalf("rejoin record %+v", m)
	}
}

func TestRemoveAndRing(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	tb := newTestTable(c)
	tb.Merge([]Member{
		{ID: "b", Generation: 1, Incarnation: 1, Phase: PhaseActive},
		{ID: "c", Generation: 1, Incarnation: 1, Phase: PhaseJoining},
	})
	if got := tb.RingNodes(); len(got) != 3 {
		t.Fatalf("joining members are part of the ring: %v", got)
	}
	c.advance(10 * time.Second)
	tb.Tick()
	if up, total := tb.ReachableRingCount(); up != 1 || total != 3 {
		t.Fatalf("up=%d total=%d", up, total)
	}
	if got := tb.DeadLongerThan(5 * time.Second); len(got) != 0 {
		t.Fatal("deadSince is when it was declared, not last seen")
	}
	c.advance(5 * time.Second)
	if got := tb.DeadLongerThan(5 * time.Second); len(got) != 2 {
		t.Fatalf("dead longer: %v", got)
	}
	if !tb.Remove("b") || tb.Remove("b") || tb.Remove("a") {
		t.Fatal("remove semantics")
	}
	if got := tb.RingNodes(); len(got) != 2 {
		t.Fatalf("ring after remove: %v", got)
	}
	// A stale gossip record cannot bring b back.
	tb.Merge([]Member{{ID: "b", Generation: 1, Incarnation: 50, Phase: PhaseActive}})
	if m, _ := tb.Member("b"); m.Phase != PhaseLeft {
		t.Fatal("removed member resurrected by stale gossip")
	}
	// A restart (new generation) can.
	tb.Merge([]Member{{ID: "b", Generation: 2, Incarnation: 1, Phase: PhaseJoining}})
	if m, _ := tb.Member("b"); m.Phase != PhaseJoining {
		t.Fatal("new generation should rejoin")
	}
	if len(tb.Status()) != 3 || len(tb.Peers()) != 2 {
		t.Fatal("status/peers")
	}
}
