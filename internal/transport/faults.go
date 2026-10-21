package transport

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrInjected is returned for calls blocked by fault injection.
var ErrInjected = errors.New("transport: connection blocked by fault injection")

// AllPeers matches every peer in Faults.Block / SetDelay.
const AllPeers = "*"

// Faults injects network failures for testing and demonstrations: blocking
// traffic to and from specific peers (a partition) and delaying outbound
// calls (a slow network or slow peer). Blocking is enforced on both the
// sending side (Pool) and the receiving side (Server, by the From field), so
// blocking a peer on one node is enough to cut traffic in both directions.
type Faults struct {
	mu      sync.RWMutex
	blocked map[string]bool
	delay   map[string]time.Duration
}

// NewFaults returns an empty fault set.
func NewFaults() *Faults {
	return &Faults{blocked: map[string]bool{}, delay: map[string]time.Duration{}}
}

// Block cuts traffic with the given peers.
func (f *Faults) Block(peers ...string) {
	f.mu.Lock()
	for _, p := range peers {
		f.blocked[p] = true
	}
	f.mu.Unlock()
}

// Unblock restores traffic with the given peers.
func (f *Faults) Unblock(peers ...string) {
	f.mu.Lock()
	for _, p := range peers {
		delete(f.blocked, p)
	}
	f.mu.Unlock()
}

// SetDelay delays every outbound call to peer by d (0 removes the delay).
func (f *Faults) SetDelay(peer string, d time.Duration) {
	f.mu.Lock()
	if d <= 0 {
		delete(f.delay, peer)
	} else {
		f.delay[peer] = d
	}
	f.mu.Unlock()
}

// Reset removes all faults.
func (f *Faults) Reset() {
	f.mu.Lock()
	f.blocked = map[string]bool{}
	f.delay = map[string]time.Duration{}
	f.mu.Unlock()
}

// FaultState is a snapshot of injected faults.
type FaultState struct {
	Blocked []string         `json:"blocked"`
	DelayMs map[string]int64 `json:"delay_ms"`
}

// Snapshot returns the current faults.
func (f *Faults) Snapshot() FaultState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	st := FaultState{Blocked: []string{}, DelayMs: map[string]int64{}}
	for p := range f.blocked {
		st.Blocked = append(st.Blocked, p)
	}
	sort.Strings(st.Blocked)
	for p, d := range f.delay {
		st.DelayMs[p] = d.Milliseconds()
	}
	return st
}

// Blocked reports whether traffic with peer is cut.
func (f *Faults) Blocked(peer string) bool {
	if f == nil || peer == "" {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.blocked[peer] || f.blocked[AllPeers]
}

func (f *Faults) beforeSend(ctx context.Context, peer string) error {
	if f == nil {
		return nil
	}
	if f.Blocked(peer) {
		return ErrInjected
	}
	f.mu.RLock()
	d, ok := f.delay[peer]
	if !ok {
		d = f.delay[AllPeers]
	}
	f.mu.RUnlock()
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
