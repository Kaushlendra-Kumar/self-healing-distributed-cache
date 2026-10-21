package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("shc_requests_total", "Requests.", "op", "result")
	c.With("GET", "hit").Inc()
	c.With("GET", "hit").Add(2)
	r.NewGaugeFunc("shc_entries", "Entries.", func() float64 { return 42 })
	r.NewGaugeVecFunc("shc_peer_up", "Peer up.", "peer", func() map[string]float64 { return map[string]float64{"b": 1} })
	h := r.NewHistogramVec("shc_latency_seconds", "Latency.", []float64{0.001, 0.01}, "op")
	h.With("GET").ObserveDuration(500 * time.Microsecond)
	h.With("GET").ObserveDuration(5 * time.Millisecond)
	h.With("GET").ObserveDuration(time.Second)

	var b strings.Builder
	r.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		`shc_requests_total{op="GET",result="hit"} 3`,
		`shc_entries 42`,
		`shc_peer_up{peer="b"} 1`,
		`shc_latency_seconds_bucket{op="GET",le="0.001"} 1`,
		`shc_latency_seconds_bucket{op="GET",le="0.01"} 2`,
		`shc_latency_seconds_bucket{op="GET",le="+Inf"} 3`,
		`shc_latency_seconds_count{op="GET"} 3`,
		"# TYPE shc_latency_seconds histogram",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
