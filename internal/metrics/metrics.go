// Package metrics is a small, dependency-free implementation of counters,
// gauges and histograms rendered in the Prometheus text exposition format.
//
// Label values must come from small, bounded sets (operation names, peer IDs,
// result codes). Never use keys or request IDs as label values: every distinct
// value creates a new time series.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Registry holds metric families.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// WritePrometheus renders every family.
func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
}

func labelString(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v := ""
		if i < len(values) {
			v = values[i]
		}
		fmt.Fprintf(&b, "%s=%q", n, v)
	}
	b.WriteByte('}')
	return b.String()
}

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the current value.
func (c *Counter) Value() uint64 { return c.v.Load() }

// CounterVec is a counter family partitioned by labels.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.RWMutex
	children   map[string]*Counter
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{name: name, help: help, labels: labels, children: map[string]*Counter{}}
	r.add(v)
	return v
}

// With returns the counter for the given label values.
func (v *CounterVec) With(values ...string) *Counter {
	k := labelString(v.labels, values)
	v.mu.RLock()
	c, ok := v.children[k]
	v.mu.RUnlock()
	if ok {
		return c
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok = v.children[k]; !ok {
		c = &Counter{}
		v.children[k] = c
	}
	return c
}

func (v *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", v.name, v.help, v.name)
	v.mu.RLock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %d\n", v.name, k, v.children[k].Value())
	}
	v.mu.RUnlock()
}

// GaugeFunc reports a value computed at scrape time.
type gaugeFunc struct {
	name, help string
	fn         func() float64
}

// NewGaugeFunc registers a gauge evaluated on every scrape.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.add(&gaugeFunc{name: name, help: help, fn: fn})
}

func (g *gaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", g.name, g.help, g.name, g.name, formatFloat(g.fn()))
}

// gaugeVecFunc reports labelled values computed at scrape time.
type gaugeVecFunc struct {
	name, help string
	labels     []string
	fn         func() map[string]float64 // key: single label value
}

// NewGaugeVecFunc registers a single-label gauge family evaluated on scrape.
func (r *Registry) NewGaugeVecFunc(name, help, label string, fn func() map[string]float64) {
	r.add(&gaugeVecFunc{name: name, help: help, labels: []string{label}, fn: fn})
}

func (g *gaugeVecFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
	vals := g.fn()
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %s\n", g.name, labelString(g.labels, []string{k}), formatFloat(vals[k]))
	}
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return fmt.Sprintf("%g", f)
}

// DefaultLatencyBuckets covers 50µs to 5s.
var DefaultLatencyBuckets = []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// Histogram tracks a distribution in cumulative buckets.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // len(bounds)+1, last is +Inf
	sumNs  atomic.Uint64
	count  atomic.Uint64
}

// ObserveDuration records d.
func (h *Histogram) ObserveDuration(d time.Duration) {
	s := d.Seconds()
	i := sort.SearchFloat64s(h.bounds, s)
	h.counts[i].Add(1)
	h.sumNs.Add(uint64(d.Nanoseconds()))
	h.count.Add(1)
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// HistogramVec is a histogram family partitioned by labels.
type HistogramVec struct {
	name, help string
	labels     []string
	bounds     []float64
	mu         sync.RWMutex
	children   map[string]*Histogram
}

// NewHistogramVec registers a histogram family (bucket bounds in seconds).
func (r *Registry) NewHistogramVec(name, help string, bounds []float64, labels ...string) *HistogramVec {
	v := &HistogramVec{name: name, help: help, labels: labels, bounds: bounds, children: map[string]*Histogram{}}
	r.add(v)
	return v
}

// With returns the histogram for the given label values.
func (v *HistogramVec) With(values ...string) *Histogram {
	k := labelString(v.labels, values)
	v.mu.RLock()
	h, ok := v.children[k]
	v.mu.RUnlock()
	if ok {
		return h
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if h, ok = v.children[k]; !ok {
		h = &Histogram{bounds: v.bounds, counts: make([]atomic.Uint64, len(v.bounds)+1)}
		v.children[k] = h
	}
	return h
}

func (v *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", v.name, v.help, v.name)
	v.mu.RLock()
	defer v.mu.RUnlock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := v.children[k]
		inner := strings.TrimSuffix(strings.TrimPrefix(k, "{"), "}")
		sep := ""
		if inner != "" {
			sep = ","
		}
		var cum uint64
		for i, b := range h.bounds {
			cum += h.counts[i].Load()
			fmt.Fprintf(w, "%s_bucket{%s%sle=\"%g\"} %d\n", v.name, inner, sep, b, cum)
		}
		cum += h.counts[len(h.bounds)].Load()
		fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", v.name, inner, sep, cum)
		fmt.Fprintf(w, "%s_sum%s %g\n", v.name, k, float64(h.sumNs.Load())/1e9)
		fmt.Fprintf(w, "%s_count%s %d\n", v.name, k, h.count.Load())
	}
}

type counterFunc struct {
	name, help string
	fn         func() uint64
}

// NewCounterFunc registers a counter whose value is read at scrape time (for
// counters maintained elsewhere, such as inside the store).
func (r *Registry) NewCounterFunc(name, help string, fn func() uint64) {
	r.add(&counterFunc{name: name, help: help, fn: fn})
}

func (c *counterFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", c.name, c.help, c.name, c.name, c.fn())
}
