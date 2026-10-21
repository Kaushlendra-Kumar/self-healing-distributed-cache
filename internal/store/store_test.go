package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.UnixMilli(1_700_000_000_000)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func v(t uint64) version.Version { return version.Version{T: t, Node: "n1"} }

func mustStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestSetGetDelete(t *testing.T) {
	s := mustStore(t, Config{})
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected miss")
	}
	if ok, err := s.Apply(Entry{Key: "a", Value: []byte("1"), Version: v(1)}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	e, ok := s.Get("a")
	if !ok || string(e.Value) != "1" {
		t.Fatalf("got %q %v", e.Value, ok)
	}
	// Delete is a newer tombstone.
	if ok, _ := s.Apply(Entry{Key: "a", Tombstone: true, Version: v(2)}); !ok {
		t.Fatal("tombstone not applied")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("deleted key visible")
	}
	if e, ok := s.Lookup("a"); !ok || !e.Tombstone {
		t.Fatal("Lookup must expose tombstone for version comparison")
	}
	// An older write delivered late must not resurrect the key.
	if ok, _ := s.Apply(Entry{Key: "a", Value: []byte("old"), Version: v(1)}); ok {
		t.Fatal("stale write resurrected deleted key")
	}
	if _, err := s.Apply(Entry{Key: ""}); err != ErrEmptyKey {
		t.Fatal("expected ErrEmptyKey")
	}
}

func TestVersionOrderingIsIdempotent(t *testing.T) {
	s := mustStore(t, Config{})
	newer := Entry{Key: "k", Value: []byte("new"), Version: v(5)}
	older := Entry{Key: "k", Value: []byte("old"), Version: v(3)}
	s.Apply(newer)
	if ok, _ := s.Apply(older); ok {
		t.Fatal("out-of-order older update applied")
	}
	if ok, _ := s.Apply(newer); ok {
		t.Fatal("duplicate update reported as change")
	}
	e, _ := s.Get("k")
	if string(e.Value) != "new" {
		t.Fatalf("got %q", e.Value)
	}
}

func TestTTLLazyAndSweep(t *testing.T) {
	clk := newFakeClock()
	s := mustStore(t, Config{Now: clk.Now, Shards: 1})
	exp := clk.Now().Add(time.Second).UnixMilli()
	s.Apply(Entry{Key: "lazy", Value: []byte("x"), Version: v(1), ExpireAt: exp})
	s.Apply(Entry{Key: "swept", Value: []byte("x"), Version: v(1), ExpireAt: exp})
	s.Apply(Entry{Key: "forever", Value: []byte("x"), Version: v(1)})
	if _, ok := s.Get("lazy"); !ok {
		t.Fatal("expired too early")
	}
	clk.Advance(time.Second) // ExpireAt is inclusive: expired at exactly exp
	if _, ok := s.Get("lazy"); ok {
		t.Fatal("lazy expiration failed")
	}
	if n := s.SweepExpired(20); n != 1 {
		t.Fatalf("sweep removed %d, want 1", n)
	}
	if st := s.Stats(); st.Entries != 1 || st.Expirations != 2 {
		t.Fatalf("stats %+v", st)
	}
	// Arriving already expired: not stored.
	if ok, _ := s.Apply(Entry{Key: "dead", Value: []byte("x"), Version: v(9), ExpireAt: exp}); ok {
		t.Fatal("expired entry stored")
	}
}

func TestTombstoneExpires(t *testing.T) {
	clk := newFakeClock()
	s := mustStore(t, Config{Now: clk.Now, TombstoneTTL: time.Minute})
	s.Apply(Entry{Key: "k", Tombstone: true, Version: v(2)})
	clk.Advance(61 * time.Second)
	if _, ok := s.Lookup("k"); ok {
		t.Fatal("tombstone should have expired")
	}
}

func TestLRUEviction(t *testing.T) {
	s := mustStore(t, Config{Shards: 1, MaxEntries: 3, Policy: PolicyLRU})
	for i := 0; i < 3; i++ {
		s.Apply(Entry{Key: fmt.Sprint(i), Version: v(1)})
	}
	s.Get("0") // 1 is now least recently used
	s.Apply(Entry{Key: "3", Version: v(1)})
	if _, ok := s.Get("1"); ok {
		t.Fatal("LRU victim should have been key 1")
	}
	for _, k := range []string{"0", "2", "3"} {
		if _, ok := s.Get(k); !ok {
			t.Fatalf("key %s evicted unexpectedly", k)
		}
	}
	if s.Stats().Evictions != 1 {
		t.Fatal("eviction not counted")
	}
}

func TestLFUEviction(t *testing.T) {
	s := mustStore(t, Config{Shards: 1, MaxEntries: 3, Policy: PolicyLFU})
	for i := 0; i < 3; i++ {
		s.Apply(Entry{Key: fmt.Sprint(i), Version: v(1)})
	}
	for i := 0; i < 5; i++ {
		s.Get("0")
		s.Get("2")
	}
	s.Get("1")
	s.Get("1")
	s.Apply(Entry{Key: "3", Version: v(1)}) // evicts 1 (freq 3) over 0 and 2 (freq 6)
	if _, ok := s.Lookup("1"); ok {
		t.Fatal("LFU victim should have been key 1")
	}
	s.Apply(Entry{Key: "4", Version: v(1)}) // evicts 3 (freq 1)
	if _, ok := s.Lookup("3"); ok {
		t.Fatal("LFU victim should have been key 3")
	}
}

func TestMemoryBound(t *testing.T) {
	s := mustStore(t, Config{Shards: 4, MaxBytes: 64 << 10})
	val := make([]byte, 1000)
	for i := 0; i < 1000; i++ {
		if _, err := s.Apply(Entry{Key: fmt.Sprint("k", i), Value: val, Version: v(1)}); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.Bytes > 64<<10 {
		t.Fatalf("memory budget exceeded: %d", st.Bytes)
	}
	if st.Evictions == 0 {
		t.Fatal("expected evictions")
	}
	if _, err := s.Apply(Entry{Key: "huge", Value: make([]byte, 32<<10), Version: v(1)}); err != ErrTooLarge {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

func TestRemoveIfVersion(t *testing.T) {
	s := mustStore(t, Config{})
	s.Apply(Entry{Key: "k", Value: []byte("a"), Version: v(1)})
	s.Apply(Entry{Key: "k", Value: []byte("b"), Version: v(2)})
	if s.RemoveIfVersion("k", v(1)) {
		t.Fatal("removed despite newer concurrent write")
	}
	if !s.RemoveIfVersion("k", v(2)) {
		t.Fatal("matching remove failed")
	}
}

func TestBadPolicy(t *testing.T) {
	if _, err := New(Config{Policy: "mru"}); err == nil {
		t.Fatal("expected error")
	}
}

// TestConcurrentAccess is meaningful under -race: it exercises every public
// method from many goroutines while the sweeper runs.
func TestConcurrentAccess(t *testing.T) {
	for _, pol := range []string{PolicyLRU, PolicyLFU} {
		t.Run(pol, func(t *testing.T) {
			s := mustStore(t, Config{MaxEntries: 500, Policy: pol, SweepInterval: time.Millisecond})
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 2000; i++ {
						k := fmt.Sprint(i % 700)
						switch i % 5 {
						case 0, 1:
							s.Apply(Entry{Key: k, Value: []byte("v"), Version: v(uint64(i)), ExpireAt: time.Now().Add(time.Duration(i%3) * time.Millisecond).UnixMilli()})
						case 2:
							s.Get(k)
						case 3:
							s.Apply(Entry{Key: k, Tombstone: true, Version: v(uint64(i))})
						case 4:
							s.Range(func(Entry) bool { return i%2 == 0 })
						}
					}
				}(g)
			}
			wg.Wait()
			if st := s.Stats(); st.Entries > 500+16 {
				t.Fatalf("entry bound violated: %d", st.Entries)
			}
		})
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s, _ := New(Config{})
	defer s.Close()
	for i := 0; i < 10000; i++ {
		s.Apply(Entry{Key: fmt.Sprint(i), Value: make([]byte, 100), Version: v(1)})
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprint(i % 10000))
			i++
		}
	})
}

func BenchmarkApplyParallel(b *testing.B) {
	for _, pol := range []string{PolicyLRU, PolicyLFU} {
		b.Run(pol, func(b *testing.B) {
			s, _ := New(Config{MaxEntries: 5000, Policy: pol})
			defer s.Close()
			val := make([]byte, 100)
			var n uint64
			var mu sync.Mutex
			b.RunParallel(func(pb *testing.PB) {
				mu.Lock()
				n++
				base := n << 32
				mu.Unlock()
				i := uint64(0)
				for pb.Next() {
					s.Apply(Entry{Key: fmt.Sprint(i % 20000), Value: val, Version: v(base + i)})
					i++
				}
			})
		})
	}
}
