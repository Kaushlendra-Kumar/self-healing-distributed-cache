package store

import "fmt"

// Policy names accepted by Config.Policy.
const (
	PolicyLRU = "lru"
	PolicyLFU = "lfu"
)

// evictionPolicy tracks recency/frequency metadata for the items of one shard.
// All methods are called with the shard lock held, so implementations need no
// synchronization of their own.
type evictionPolicy interface {
	add(it *item)
	touch(it *item)
	remove(it *item)
	victim() *item
}

func newPolicy(name string) (evictionPolicy, error) {
	switch name {
	case "", PolicyLRU:
		return newLRU(), nil
	case PolicyLFU:
		return newLFU(), nil
	}
	return nil, fmt.Errorf("unknown eviction policy %q (want %q or %q)", name, PolicyLRU, PolicyLFU)
}

// ilist is an intrusive circular doubly linked list of items. Using intrusive
// links avoids a separate allocation per entry.
type ilist struct {
	root item
	n    int
}

func (l *ilist) init() *ilist {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.n = 0
	return l
}

func (l *ilist) pushFront(it *item) {
	it.prev = &l.root
	it.next = l.root.next
	l.root.next.prev = it
	l.root.next = it
	l.n++
}

func (l *ilist) unlink(it *item) {
	it.prev.next = it.next
	it.next.prev = it.prev
	it.prev, it.next = nil, nil
	l.n--
}

func (l *ilist) back() *item {
	if l.n == 0 {
		return nil
	}
	return l.root.prev
}

// lru evicts the least recently used item. touch moves an item to the front.
type lru struct{ l ilist }

func newLRU() *lru {
	p := &lru{}
	p.l.init()
	return p
}

func (p *lru) add(it *item)    { p.l.pushFront(it) }
func (p *lru) remove(it *item) { p.l.unlink(it) }
func (p *lru) victim() *item   { return p.l.back() }
func (p *lru) touch(it *item) {
	p.l.unlink(it)
	p.l.pushFront(it)
}

// lfu evicts the least frequently used item, breaking ties by recency (the
// least recently used item among the lowest frequency). All operations are
// O(1): items are kept in one list per access count and minFreq tracks the
// lowest non-empty bucket.
type lfu struct {
	buckets map[uint64]*ilist
	minFreq uint64
}

func newLFU() *lfu { return &lfu{buckets: make(map[uint64]*ilist)} }

func (p *lfu) bucket(f uint64) *ilist {
	b, ok := p.buckets[f]
	if !ok {
		b = (&ilist{}).init()
		p.buckets[f] = b
	}
	return b
}

func (p *lfu) add(it *item) {
	it.freq = 1
	p.bucket(1).pushFront(it)
	p.minFreq = 1
}

func (p *lfu) detach(it *item) {
	b := p.buckets[it.freq]
	b.unlink(it)
	if b.n == 0 {
		delete(p.buckets, it.freq)
	}
}

func (p *lfu) touch(it *item) {
	old := it.freq
	p.detach(it)
	it.freq++
	p.bucket(it.freq).pushFront(it)
	if p.minFreq == old && p.buckets[old] == nil {
		p.minFreq = it.freq
	}
}

func (p *lfu) remove(it *item) {
	p.detach(it)
	if it.freq == p.minFreq && p.buckets[it.freq] == nil {
		p.recomputeMin()
	}
}

// recomputeMin is O(number of distinct frequencies). It runs only when the
// minimum bucket empties through an explicit removal, never on the hot path.
func (p *lfu) recomputeMin() {
	p.minFreq = 0
	for f := range p.buckets {
		if p.minFreq == 0 || f < p.minFreq {
			p.minFreq = f
		}
	}
}

func (p *lfu) victim() *item {
	if b := p.buckets[p.minFreq]; b != nil {
		return b.back()
	}
	if len(p.buckets) == 0 {
		return nil
	}
	p.recomputeMin()
	return p.buckets[p.minFreq].back()
}
