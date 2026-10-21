# ADR 0001: Go, standard library only

## Context
The project must show distributed-systems understanding: networking, concurrency,
failure handling. A large share of the work is concurrent network I/O, timeouts,
cancellation and testing failure scenarios.

## Options
- **Go:** goroutines and channels make per-connection and per-request concurrency cheap.
  `context` gives uniform deadlines and cancellation. `net`, `net/http`, `log/slog`,
  `testing` (with fuzzing and a race detector) and `pprof` are all built in. Fast
  iteration and static binaries.
- **C++:** full control of memory layout and allocation, and deeper systems-programming
  signal (RAII, explicit ownership, lock-free structures). But it needs an async I/O
  framework (asio) or a hand-written epoll loop, more code per feature, sanitizers
  instead of a built-in race detector, and slower iteration on the distributed parts.

## Decision
Go 1.22+, **no third-party dependencies**. Metrics exposition, the protocol and the
client are written by hand, so the mechanisms stay visible.

## Consequences
- Distributed behaviour gets most of the effort. Garbage-collector pauses become a real
  failure-detection concern, which is documented. GC was ~10–15% of CPU under write load.
- No Prometheus client library: a minimal exposition implementation lives in
  `internal/metrics`.
- C++ remains an option for a future storage engine if profiling ever shows the store
  to be the bottleneck. It isn't now: 233 ns per GET versus a ~120 µs round trip.
