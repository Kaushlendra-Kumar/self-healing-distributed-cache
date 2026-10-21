# Benchmarks

All numbers below were measured, not estimated. Raw results are in
[bench/results.jsonl](bench/results.jsonl), [bench/results.txt](bench/results.txt),
[bench/failover.txt](bench/failover.txt) and [bench/micro.txt](bench/micro.txt).

## Environment

| | |
|---|---|
| Machine | Cloud VM, 4 vCPU (Intel Xeon), 15 GiB RAM, Linux 6.18 |
| Go | 1.24.7 |
| Topology | 3 `cachenode` processes **and** the load generator on the same VM, over loopback |
| Node config | defaults: RF=3, W=2, 128 vnodes, LRU, 64 MiB, `SHC_LOG_LEVEL=warn` |
| Workload | `cachebench`: closed loop (each worker waits for its reply), 10,000 keys prefilled, uniform random keys, 128 B values unless noted, 5 s per run |
| Client | `pkg/client`: routes to the key's first owner, so there is no forwarding hop |

**Caveats.** The client and all three nodes share 4 vCPUs, so absolute throughput is
CPU-bound and lower than separate machines would give. Loopback hides network latency.
Run-to-run variation was about ±5%. Use these figures to compare configurations, not as
capacity planning numbers. To reproduce: `./scripts/bench.sh && ./scripts/failover-timing.sh`.

## Throughput and latency

| Workload | RF=3, W=2: ops/s | p50 | p95 | p99 | RF=1: ops/s | p50 | p99 |
|---|---|---|---|---|---|---|---|
| GET c=1 | 7,808 | 124 µs | 163 µs | 189 µs | 8,154 | 117 µs | 194 µs |
| GET c=8 | 36,701 | 173 µs | 458 µs | 819 µs | 37,609 | 170 µs | 833 µs |
| GET c=32 | 57,615 | 421 µs | 1.34 ms | 2.90 ms | 59,598 | 417 µs | 2.23 ms |
| SET c=1 | 3,678 | 264 µs | 321 µs | 374 µs | 7,838 | 124 µs | 200 µs |
| SET c=8 | 15,132 | 408 µs | 1.19 ms | 2.74 ms | 37,040 | 170 µs | 885 µs |
| SET c=32 | 22,599 | 1.03 ms | 3.90 ms | 6.19 ms | 56,882 | 421 µs | 2.95 ms |
| DELETE c=8 | 16,054 | 378 µs | 1.15 ms | 2.62 ms | 39,473 | 163 µs | 754 µs |
| 90% GET / 10% SET c=32 | 50,021 | 473 µs | 1.66 ms | 3.19 ms | 59,233 | 412 µs | 2.41 ms |
| SET 1 KiB c=16 | 17,658 | 716 µs | 2.10 ms | 4.08 ms | 39,500 | 318 µs | 1.60 ms |
| GET 1 KiB c=16 | 41,967 | 301 µs | 869 µs | 1.52 ms | 40,735 | 303 µs | 1.74 ms |

Every run had **0 errors**. Hit ratio was 1.0 for GET runs after the prefill.

### Reading the numbers

- **Reads are unaffected by replication.** A `one` read touches one node with either RF.
- **Replication costs writes about 2× latency at low concurrency and 2.5× throughput at
  high concurrency.** A replicated SET needs a coordinator→replica round trip, and it
  needs two extra messages per write (to both replicas, waiting for one). On a CPU-bound
  shared host, those extra messages consume the throughput. The background replica
  update still costs CPU even though the client doesn't wait for it.
- **Tail latency grows with concurrency** because client and nodes compete for the same
  4 cores (closed-loop queueing). p99 stays within a few milliseconds.

## Failure detection, recovery and rebalancing

Measured with `scripts/failover-timing.sh`: 50,000 keys, RF=3, default timeouts.

| Measurement | Result | Expected from config |
|---|---|---|
| SIGKILL → peer marks node **suspect** | 1.66 s | 1.5 s + ≤ 1 heartbeat (0.5 s) |
| SIGKILL → peer marks node **dead** | 4.17 s | 4.0 s + ≤ 1 heartbeat |
| Restart (empty) → ready, holding all 50,000 keys | 2.63 s | ≈ 1 s seed discovery + 1 heartbeat + sync |
| Entries received during that recovery | 53,692 (some keys offered by both sources) | ≥ 50,000 |
| 4th node joins → ready | 4.66 s, 30,719 entries received | throttle: 20,000 keys/s per source |
| Copies after rebalance + handoff cleanup | **150,000 exactly** (38.5k / 38.1k / 34.7k / 38.7k per node) | 50,000 × 3 |

Detection time is dominated by the configured timeouts. That's a deliberate trade-off:
lowering `SHC_DEAD_AFTER` speeds failover but raises false positives under GC pauses or
network jitter. Each false positive costs a full resync of that node.

The join is deliberately throttled. Raising `SHC_SYNC_RATE_KEYS` shortens it, at the
cost of competing with client traffic.

## Key distribution

From the ring tests (`go test -v ./internal/ring/`), 100k keys over 5 nodes:

| Virtual nodes | Relative std-dev of keys per node |
|---|---|
| 1 | 64.2% |
| 256 | 9.7% |

Going from 4 to 5 nodes, consistent hashing remapped **19.9%** of keys (ideal 20%), and
modulo hashing remapped **80.0%**. In the 4-node rebalance above, per-node copy counts
were within ±8% of the mean with 128 vnodes. `shc_key_ownership_ratio` on `/metrics`
reports each node's share of the hash space.

## Micro-benchmarks (`go test -bench`)

| Benchmark | Result |
|---|---|
| `store.Get`, parallel (4 threads, 10k keys) | 233 ns/op, 1 alloc |
| `store.Apply` LRU, parallel (bounded 5k entries) | 454 ns/op, 2 allocs |
| `store.Apply` LFU, parallel | 526 ns/op, 2 allocs |
| `ring.Owners(key, 3)`, 10 nodes × 128 vnodes | 119 ns/op, 1 alloc |

The store and ring are ~2 orders of magnitude faster than a network round trip. They
are not the bottleneck.

## Where the time goes (profile)

CPU profile of node1 during `SET c=32` (`SHC_ENABLE_PPROF=true`, 8 s):

| Share of CPU | Activity |
|---|---|
| ~25% | `read`/`write` syscalls |
| ~20% | `encoding/json` marshal + unmarshal |
| ~10–15% | GC (`scanobject`, allocation) |
| small | store, ring, membership |

One optimization was made from that measurement: **buffered reads** (`bufio.Reader`), so
pipelined frames don't each cost two `read` syscalls. On SET c=32 throughput went from
about 20.8k to 22.6k ops/s, and on GET c=32 from about 53–55k to 57.6k. That's roughly
+5–10%, which is near the run-to-run noise, so treat it as a small gain.

Next steps, in measured order of payoff:

1. **A binary encoding** instead of JSON (removes the ~20% of CPU and base64 inflation).
2. **Write coalescing.** A per-connection writer goroutine would batch responses into one
   `write` syscall.
3. **Reuse buffers** to cut allocations and GC.

None of these change the design. The protocol's version byte exists for the first one.

## Comparing with Redis

No Redis comparison was run. A fair comparison would need the same host, the same
closed-loop client and value sizes, and Redis configured for comparable replication
(e.g. `WAIT 1` after each write for W=2). Even then, the systems differ:

- Redis Cluster replicates asynchronously by default.
- Redis uses a binary protocol and a single-threaded event loop.
- This project offers quorum reads, and it refuses writes that cannot reach W.

A side-by-side number without those caveats would mislead.
