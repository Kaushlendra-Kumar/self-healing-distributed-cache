// Package store implements the in-memory storage engine of a single cache
// node: a sharded hash map with per-key versions, TTL expiration, tombstones
// for deletes and bounded memory enforced by LRU or LFU eviction.
//
// The store knows nothing about the network or the cluster. It enforces one
// distributed-systems rule locally: an entry is only replaced by an entry with
// a strictly newer version (last-writer-wins), which makes replication
// messages idempotent and safe to deliver late, twice or out of order.
package store

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

// Errors returned by the store.
var (
	ErrTooLarge = errors.New("entry larger than the per-shard memory budget")
	ErrEmptyKey = errors.New("empty key")
)

// itemOverhead approximates the bookkeeping bytes per entry (map slot, item
// struct, list links). Memory accounting is an estimate, not an exact count of
// heap usage; see docs/ARCHITECTURE.md.
const itemOverhead = 96

// Entry is the unit of storage and replication.
type Entry struct {
	Key       string          `json:"key"`
	Value     []byte          `json:"value,omitempty"`
	Version   version.Version `json:"version"`
	ExpireAt  int64           `json:"expire_at,omitempty"` // absolute unix milliseconds; 0 = never
	Tombstone bool            `json:"tombstone,omitempty"`
}

// Expired reports whether the entry is expired at nowMs.
func (e *Entry) Expired(nowMs int64) bool { return e.ExpireAt != 0 && e.ExpireAt <= nowMs }

func (e *Entry) size() int64 {
	return int64(len(e.Key)+len(e.Value)+len(e.Version.Node)) + itemOverhead
}

// Config configures a Store.
type Config struct {
	Shards        int           // number of independently locked shards (default 16)
	MaxBytes      int64         // approximate memory budget; 0 = unlimited
	MaxEntries    int           // entry budget; 0 = unlimited
	Policy        string        // "lru" (default) or "lfu"
	TombstoneTTL  time.Duration // lifetime of delete markers without an explicit expiry (default 60s)
	SweepInterval time.Duration // background expiration period; 0 disables the sweeper
	Now           func() time.Time
}

// Stats is a point-in-time snapshot of store counters.
type Stats struct {
	Entries     int64
	Tombstones  int64
	Bytes       int64
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Expirations uint64
}

type item struct {
	e          Entry
	size       int64
	prev, next *item
	freq       uint64
}

type shard struct {
	mu         sync.Mutex
	items      map[string]*item
	policy     evictionPolicy
	bytes      int64
	tombstones int64
	maxBytes   int64
	maxEntries int
}

// Store is safe for concurrent use.
type Store struct {
	shards       []*shard
	now          func() time.Time
	tombstoneTTL time.Duration

	hits, misses, evictions, expirations atomic.Uint64

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// New creates a store and starts its background sweeper if configured.
func New(cfg Config) (*Store, error) {
	if cfg.Shards <= 0 {
		cfg.Shards = 16
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.TombstoneTTL <= 0 {
		cfg.TombstoneTTL = 60 * time.Second
	}
	s := &Store{
		shards:       make([]*shard, cfg.Shards),
		now:          cfg.Now,
		tombstoneTTL: cfg.TombstoneTTL,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	for i := range s.shards {
		p, err := newPolicy(cfg.Policy)
		if err != nil {
			return nil, err
		}
		sh := &shard{items: make(map[string]*item), policy: p}
		if cfg.MaxBytes > 0 {
			sh.maxBytes = max(cfg.MaxBytes/int64(cfg.Shards), 1)
		}
		if cfg.MaxEntries > 0 {
			sh.maxEntries = max((cfg.MaxEntries+cfg.Shards-1)/cfg.Shards, 1)
		}
		s.shards[i] = sh
	}
	if cfg.SweepInterval > 0 {
		go s.sweepLoop(cfg.SweepInterval)
	} else {
		close(s.done)
	}
	return s, nil
}

// Close stops the background sweeper. It is idempotent.
func (s *Store) Close() {
	s.once.Do(func() { close(s.stop) })
	<-s.done
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()%uint32(len(s.shards))]
}

func (s *Store) nowMs() int64 { return s.now().UnixMilli() }

// removeLocked deletes it from sh. Caller holds sh.mu.
func (sh *shard) removeLocked(it *item) {
	sh.policy.remove(it)
	delete(sh.items, it.e.Key)
	sh.bytes -= it.size
	if it.e.Tombstone {
		sh.tombstones--
	}
}

// Get returns the live value for key. Tombstones and expired entries are
// reported as misses. A hit updates the eviction metadata, which is why Get
// takes the shard's exclusive lock rather than a read lock.
func (s *Store) Get(key string) (Entry, bool) {
	sh := s.shardFor(key)
	now := s.nowMs()
	sh.mu.Lock()
	it, ok := sh.items[key]
	if ok && it.e.Expired(now) {
		sh.removeLocked(it)
		s.expirations.Add(1)
		ok = false
	}
	if !ok || it.e.Tombstone {
		sh.mu.Unlock()
		s.misses.Add(1)
		return Entry{}, false
	}
	sh.policy.touch(it)
	e := it.e
	sh.mu.Unlock()
	s.hits.Add(1)
	return e, true
}

// Lookup returns the current entry for key, including tombstones, without
// touching eviction metadata or hit/miss counters. It is used by replication
// and synchronization to compare versions.
func (s *Store) Lookup(key string) (Entry, bool) {
	sh := s.shardFor(key)
	now := s.nowMs()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it, ok := sh.items[key]
	if !ok {
		return Entry{}, false
	}
	if it.e.Expired(now) {
		sh.removeLocked(it)
		s.expirations.Add(1)
		return Entry{}, false
	}
	return it.e, true
}

// Apply stores e if it is newer than the current entry for the same key.
// It returns true if the store changed. An entry that is not newer, or that is
// already expired, is not stored; that is not an error, because the store
// already holds state at least as new (or the value's lifetime has ended).
func (s *Store) Apply(e Entry) (bool, error) {
	if e.Key == "" {
		return false, ErrEmptyKey
	}
	now := s.nowMs()
	if e.Tombstone {
		e.Value = nil
		if e.ExpireAt == 0 {
			e.ExpireAt = now + s.tombstoneTTL.Milliseconds()
		}
	}
	sh := s.shardFor(e.Key)
	sz := e.size()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.maxBytes > 0 && sz > sh.maxBytes {
		return false, ErrTooLarge
	}
	if cur, ok := sh.items[e.Key]; ok {
		if cur.e.Expired(now) {
			sh.removeLocked(cur)
			s.expirations.Add(1)
		} else if !e.Version.Newer(cur.e.Version) {
			return false, nil
		} else {
			sh.removeLocked(cur)
		}
	}
	if e.Expired(now) {
		return false, nil
	}
	for (sh.maxBytes > 0 && sh.bytes+sz > sh.maxBytes) || (sh.maxEntries > 0 && len(sh.items) >= sh.maxEntries) {
		v := sh.policy.victim()
		if v == nil {
			break
		}
		sh.removeLocked(v)
		s.evictions.Add(1)
	}
	it := &item{e: e, size: sz}
	sh.items[e.Key] = it
	sh.policy.add(it)
	sh.bytes += sz
	if e.Tombstone {
		sh.tombstones++
	}
	return true, nil
}

// RemoveIfVersion deletes key only if its current version equals v. It is used
// to drop a local copy after the data has been handed off to its new owners,
// without racing a concurrent newer write.
func (s *Store) RemoveIfVersion(key string, v version.Version) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it, ok := sh.items[key]
	if !ok || it.e.Version.Compare(v) != 0 {
		return false
	}
	sh.removeLocked(it)
	return true
}

// Range calls fn for a snapshot of every unexpired entry (tombstones
// included), one shard at a time. fn runs without any lock held, so it may
// call back into the store. Returning false stops the iteration.
func (s *Store) Range(fn func(Entry) bool) {
	for _, sh := range s.shards {
		now := s.nowMs()
		sh.mu.Lock()
		batch := make([]Entry, 0, len(sh.items))
		for _, it := range sh.items {
			if !it.e.Expired(now) {
				batch = append(batch, it.e)
			}
		}
		sh.mu.Unlock()
		for _, e := range batch {
			if !fn(e) {
				return
			}
		}
	}
}

// Stats returns current counters.
func (s *Store) Stats() Stats {
	st := Stats{
		Hits:        s.hits.Load(),
		Misses:      s.misses.Load(),
		Evictions:   s.evictions.Load(),
		Expirations: s.expirations.Load(),
	}
	for _, sh := range s.shards {
		sh.mu.Lock()
		st.Entries += int64(len(sh.items))
		st.Tombstones += sh.tombstones
		st.Bytes += sh.bytes
		sh.mu.Unlock()
	}
	return st
}

// SweepExpired performs one round of active expiration over every shard and
// returns the number of entries removed. Like Redis, it samples: it inspects
// up to sampleSize entries per shard and repeats while more than a quarter of
// the sample was expired, bounding the time spent holding a shard lock.
func (s *Store) SweepExpired(sampleSize int) int {
	removed := 0
	for _, sh := range s.shards {
		for round := 0; round < 16; round++ {
			now := s.nowMs()
			sh.mu.Lock()
			seen, expired := 0, 0
			for _, it := range sh.items { // map iteration order is randomized
				if seen >= sampleSize {
					break
				}
				seen++
				if it.e.Expired(now) {
					sh.removeLocked(it)
					expired++
				}
			}
			sh.mu.Unlock()
			removed += expired
			s.expirations.Add(uint64(expired))
			if seen == 0 || expired*4 <= seen {
				break
			}
		}
	}
	return removed
}

func (s *Store) sweepLoop(every time.Duration) {
	defer close(s.done)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.SweepExpired(20)
		}
	}
}
