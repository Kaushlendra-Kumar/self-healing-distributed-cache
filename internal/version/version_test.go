package version

import (
	"testing"
	"time"
)

func TestCompare(t *testing.T) {
	a := Version{T: 10, Node: "a"}
	b := Version{T: 10, Node: "b"}
	c := Version{T: 11, Node: "a"}
	if a.Compare(b) != -1 || b.Compare(a) != 1 || a.Compare(a) != 0 {
		t.Fatal("tie-break by node ID is wrong")
	}
	if !c.Newer(b) || b.Newer(c) {
		t.Fatal("higher T must win regardless of node")
	}
	if !(Version{}).IsZero() || a.IsZero() {
		t.Fatal("IsZero")
	}
}

func TestClockMonotonicWithFrozenTime(t *testing.T) {
	fixed := time.UnixMilli(1_000_000)
	c := NewClock(func() time.Time { return fixed })
	prev := c.Next()
	for i := 0; i < 1000; i++ {
		n := c.Next()
		if n <= prev {
			t.Fatalf("clock went backwards: %d <= %d", n, prev)
		}
		prev = n
	}
}

func TestClockObserveAndBackwardsWallClock(t *testing.T) {
	now := time.UnixMilli(5_000)
	c := NewClock(func() time.Time { return now })
	remote := uint64(9_000) << logicalBits
	c.Observe(remote)
	if got := c.Next(); got <= remote {
		t.Fatalf("Next after Observe must exceed observed: %d <= %d", got, remote)
	}
	now = time.UnixMilli(1) // wall clock jumps backwards
	before := c.Next()
	if after := c.Next(); after <= before {
		t.Fatal("clock must not go backwards when wall clock does")
	}
	if PhysicalTime(remote).UnixMilli() != 9_000 {
		t.Fatal("PhysicalTime")
	}
}
