// Package ring implements consistent hashing with virtual nodes.
//
// A Ring is immutable: membership changes build a new Ring, which lets
// readers use it without locks (callers publish it through an atomic
// pointer). Two processes that build a ring from the same node set and the
// same vnode count compute identical ownership, which is what allows clients
// and servers to route independently.
package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// Hash maps a string to a 64-bit ring position. FNV-1a alone has weak
// avalanche behaviour for short, similar strings such as "node-1#17", so the
// result is passed through the splitmix64 finalizer to spread it uniformly.
func Hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return mix64(h.Sum64())
}

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

type point struct {
	hash uint64
	node string
}

// Ring is a consistent-hash ring. The zero value is an empty ring.
type Ring struct {
	vnodes int
	points []point
	nodes  []string
}

// New builds a ring in which every node owns vnodes points. Empty and
// duplicate node IDs are ignored.
func New(vnodes int, nodes []string) *Ring {
	if vnodes <= 0 {
		vnodes = 1
	}
	seen := make(map[string]bool, len(nodes))
	r := &Ring{vnodes: vnodes}
	for _, n := range nodes {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		r.nodes = append(r.nodes, n)
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, point{hash: Hash(n + "#" + strconv.Itoa(i)), node: n})
		}
	}
	sort.Strings(r.nodes)
	// Sorting by (hash, node) makes collisions deterministic: if two virtual
	// nodes hash to the same position every process orders them identically.
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].node < r.points[j].node
	})
	return r
}

// Nodes returns the sorted member IDs.
func (r *Ring) Nodes() []string { return append([]string(nil), r.nodes...) }

// Len returns the number of distinct nodes.
func (r *Ring) Len() int { return len(r.nodes) }

// VNodes returns the configured number of virtual nodes per node.
func (r *Ring) VNodes() int { return r.vnodes }

// Contains reports whether node is a member of the ring.
func (r *Ring) Contains(node string) bool {
	i := sort.SearchStrings(r.nodes, node)
	return i < len(r.nodes) && r.nodes[i] == node
}

// Owners returns up to n distinct nodes responsible for key, in preference
// order: the first is the primary, the rest are replicas. It walks clockwise
// from the key's position, skipping virtual nodes of already selected nodes.
func (r *Ring) Owners(key string, n int) []string {
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	h := Hash(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	out := make([]string, 0, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		dup := false
		for _, o := range out {
			if o == p.node {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, p.node)
		}
	}
	return out
}

// Primary returns the first owner of key, or "" for an empty ring.
func (r *Ring) Primary(key string) string {
	o := r.Owners(key, 1)
	if len(o) == 0 {
		return ""
	}
	return o[0]
}

// OwnershipFractions returns, for each node, the fraction of the hash space
// for which it is primary. It measures balance analytically, independent of
// any particular key set.
func (r *Ring) OwnershipFractions() map[string]float64 {
	out := make(map[string]float64, len(r.nodes))
	if len(r.points) == 0 {
		return out
	}
	const space = float64(1<<63) * 2
	for i, p := range r.points {
		prev := r.points[(i-1+len(r.points))%len(r.points)].hash
		arc := p.hash - prev // wraps correctly in uint64 arithmetic
		if len(r.points) == 1 {
			out[p.node] = 1
			return out
		}
		out[p.node] += float64(arc) / space
	}
	return out
}
