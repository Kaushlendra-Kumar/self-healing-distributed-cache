// Command cachebench drives a closed-loop workload against a running cluster
// and reports throughput and latency percentiles. Each worker issues one
// request at a time, so throughput is bounded by concurrency / latency.
//
//	cachebench -seeds 127.0.0.1:7001 -c 32 -d 10s -keys 10000 -size 128 -reads 0.9
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/pkg/client"
)

type result struct {
	Label       string  `json:"label,omitempty"`
	Op          string  `json:"op"`
	Concurrency int     `json:"concurrency"`
	ValueBytes  int     `json:"value_bytes"`
	Keys        int     `json:"keys"`
	ReadRatio   float64 `json:"read_ratio"`
	Duration    string  `json:"duration"`
	Ops         int64   `json:"ops"`
	Errors      int64   `json:"errors"`
	Hits        int64   `json:"hits"`
	Misses      int64   `json:"misses"`
	OpsPerSec   float64 `json:"ops_per_sec"`
	P50us       int64   `json:"p50_us"`
	P95us       int64   `json:"p95_us"`
	P99us       int64   `json:"p99_us"`
	MaxUs       int64   `json:"max_us"`
	FirstError  string  `json:"first_error,omitempty"`
	Dist        string  `json:"dist"`
}

func main() {
	seeds := flag.String("seeds", "127.0.0.1:7001", "comma-separated node addresses")
	conc := flag.Int("c", 16, "concurrent workers")
	dur := flag.Duration("d", 10*time.Second, "measurement duration")
	keys := flag.Int("keys", 10000, "key space size")
	size := flag.Int("size", 128, "value size in bytes")
	reads := flag.Float64("reads", 0.9, "fraction of GETs in mixed mode")
	op := flag.String("op", "mixed", "mixed | get | set | delete")
	prefill := flag.Bool("prefill", true, "write every key once before measuring")
	asJSON := flag.Bool("json", false, "print the result as JSON")
	label := flag.String("label", "", "label stored with the result")
	dist := flag.String("dist", "uniform", "key distribution: uniform | zipf (s=1.1, a few hot keys)")
	timeline := flag.Bool("timeline", false, "print ops and errors per second (for failure experiments)")
	out := flag.String("out", "", "append the JSON result to this file")
	flag.Parse()

	ctx := context.Background()
	c, err := client.New(ctx, client.Config{Seeds: strings.Split(*seeds, ","), Timeout: 2 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	val := make([]byte, *size)
	rand.New(rand.NewSource(1)).Read(val)
	key := func(i int) string { return fmt.Sprintf("bench:%d", i) }

	if *prefill && *op != "delete" {
		var wg sync.WaitGroup
		var prefillErrs atomic.Int64
		next := atomic.Int64{}
		for w := 0; w < *conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := int(next.Add(1)) - 1; i < *keys; i = int(next.Add(1)) - 1 {
					if _, err := c.Set(ctx, key(i), val, 0); err != nil {
						if prefillErrs.Add(1) == 1 {
							fmt.Fprintln(os.Stderr, "prefill error:", err)
						}
					}
				}
			}()
		}
		wg.Wait()
		if n := prefillErrs.Load(); n > 0 {
			fmt.Fprintf(os.Stderr, "warning: %d of %d prefill writes failed\n", n, *keys)
		}
	}

	var ops, errs, hits, misses atomic.Int64
	secs := int(dur.Seconds()) + 2
	secOps, secErrs := make([]atomic.Int64, secs), make([]atomic.Int64, secs)
	var firstErr atomic.Value
	lat := make([][]time.Duration, *conc)
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 7))
			var zipf *rand.Zipf
			if *dist == "zipf" {
				zipf = rand.NewZipf(rng, 1.1, 1, uint64(*keys-1))
			}
			for time.Now().Before(deadline) {
				var k string
				if zipf != nil {
					k = key(int(zipf.Uint64()))
				} else {
					k = key(rng.Intn(*keys))
				}
				kind := *op
				if kind == "mixed" {
					kind = "set"
					if rng.Float64() < *reads {
						kind = "get"
					}
				}
				t0 := time.Now()
				var err error
				switch kind {
				case "get":
					var r client.GetResult
					r, err = c.Get(ctx, k)
					if err == nil {
						if r.Found {
							hits.Add(1)
						} else {
							misses.Add(1)
						}
					}
				case "set":
					_, err = c.Set(ctx, k, val, 0)
				case "delete":
					err = c.Delete(ctx, k)
				}
				lat[w] = append(lat[w], time.Since(t0))
				ops.Add(1)
				sec := min(int(time.Since(start)/time.Second), secs-1)
				secOps[sec].Add(1)
				if err != nil {
					secErrs[sec].Add(1)
					errs.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) int64 {
		if len(all) == 0 {
			return 0
		}
		return all[min(int(p*float64(len(all))), len(all)-1)].Microseconds()
	}
	r := result{
		Label: *label, Op: *op, Concurrency: *conc, ValueBytes: *size, Keys: *keys, ReadRatio: *reads,
		Duration: elapsed.Round(time.Millisecond).String(), Ops: ops.Load(), Errors: errs.Load(),
		Hits: hits.Load(), Misses: misses.Load(), OpsPerSec: float64(ops.Load()) / elapsed.Seconds(),
		Dist: *dist, P50us: pct(0.50), P95us: pct(0.95), P99us: pct(0.99), MaxUs: pct(1),
	}
	if e, ok := firstErr.Load().(string); ok {
		r.FirstError = e
	}
	if *timeline {
		for i := 0; i < secs && i < int(elapsed.Seconds())+1; i++ {
			fmt.Printf("t=%2ds ops=%6d errors=%d\n", i, secOps[i].Load(), secErrs[i].Load())
		}
	}
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			_ = json.NewEncoder(f).Encode(r)
			f.Close()
		}
	}
	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	if *label != "" {
		fmt.Printf("%-28s %9.0f ops/s  p50=%5dµs p95=%5dµs p99=%6dµs errors=%d\n", r.Label, r.OpsPerSec, r.P50us, r.P95us, r.P99us, r.Errors)
		if r.FirstError != "" {
			fmt.Println("    first error:", r.FirstError)
		}
		return
	}
	fmt.Printf("op=%s c=%d size=%dB keys=%d duration=%s\n", r.Op, r.Concurrency, r.ValueBytes, r.Keys, r.Duration)
	fmt.Printf("ops=%d errors=%d throughput=%.0f ops/s\n", r.Ops, r.Errors, r.OpsPerSec)
	fmt.Printf("latency p50=%dµs p95=%dµs p99=%dµs max=%dµs\n", r.P50us, r.P95us, r.P99us, r.MaxUs)
	if r.Hits+r.Misses > 0 {
		fmt.Printf("hit ratio=%.3f\n", float64(r.Hits)/float64(r.Hits+r.Misses))
	}
	if r.FirstError != "" {
		fmt.Println("first error:", r.FirstError)
	}
}
