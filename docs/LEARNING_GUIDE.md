# Learning guide

The topics are ordered so that each one builds only on topics before it. Each has a
short explanation, where it lives in this codebase, and an exercise to do before or
while reading that code.

## 1. Networking fundamentals

TCP gives you a reliable, ordered **byte stream** between two endpoints. It does not
give you messages, request/response pairing, or any guarantee that the other
*application* processed what you sent. A connection can fail at any moment. After a
failure you cannot know which of your bytes the peer actually acted on.

- **Code:** `internal/transport`
- **Exercise:** write a TCP echo server and client with `net.Listen`/`net.Dial`. Kill the
  server mid-conversation and observe the error the client gets on its next read and on
  its next write. (Writes often still "succeed" because they only fill the local kernel
  buffer.)

## 2. Message framing

Since TCP has no message boundaries, a protocol must add them. The common options are
delimiters (Redis RESP lines) or length prefixes (this project). A reader must handle
*partial reads*: one `Read` may return half a header.

- **Code:** `internal/protocol/protocol.go` (`WriteFrame`, `ReadFrame`)
- **Exercise:** feed `ReadFrame` with `iotest.OneByteReader`. Then remove `io.ReadFull`
  and watch the test break. Add a maximum frame size and explain why it must be checked
  *before* allocating.

## 3. Concurrent programming in Go

Goroutines are cheap; channels and mutexes coordinate them; `context.Context` carries
cancellation and deadlines; `sync.WaitGroup` waits for goroutines to finish so shutdown
is clean.

- **Code:** `internal/node/node.go` (`goLoop`, `shutdown`), `internal/transport/client.go`
  (multiplexing)
- **Exercise:** build a multiplexed RPC client in which many goroutines share one
  connection. Use a map from request ID to a response channel, one reader goroutine,
  and a write mutex.

## 4. Thread safety and race conditions

A data race is two goroutines touching the same memory, at least one writing, without
synchronization. `go test -race` detects races that actually happen during a run.
Logical races, such as check-then-act, can exist even without data races.

- **Code:** `internal/store` (sharded locks; `Get` needs a write lock because it updates
  LRU order), `RemoveIfVersion` (compare-and-delete). Also the `collectAcks` comment, a
  real bug: a `select` on a context that is cancelled when the work finishes raced with
  buffered results.
- **Exercise:** write a counter incremented by 100 goroutines without a lock and run it
  with `-race`. Then reproduce the `collectAcks` bug: buffer results, cancel the context,
  and `select` on both a thousand times.

## 5. Hashing and consistent hashing

`hash(key) % n` remaps almost every key when n changes. Consistent hashing places nodes
and keys on a ring, and a key belongs to the next node clockwise, so adding a node only
moves the keys it takes over (~1/n). **Virtual nodes** (many points per node) smooth out
the uneven arcs a single point per node produces.

- **Code:** `internal/ring`
- **Exercise:** run `go test -v ./internal/ring/` and read the logged remap ratios. Then
  change the vnode count and plot the std-dev of keys per node. Also try rendezvous
  (highest-random-weight) hashing for comparison.

## 6. Replication

Keeping N copies lets you survive N−1 losses, but the copies must be kept in sync.

- **Primary-replica:** one node orders writes. It is simple, but failover needs care.
- **Leaderless:** any replica accepts writes. It is more available, but needs conflict
  resolution.

This project is in between: per key, the first routable owner coordinates. Each write
gets a version, so replicas can apply updates in any order and ignore duplicates
(**idempotence**).

- **Code:** `internal/node/coordinator.go`, `internal/version`
- **Exercise:** send the same replication message twice and an older one after a newer
  one (`TestDuplicateAndOutOfOrderReplication`). Explain why the store's
  "strictly newer" rule makes this safe.

## 7. Failure detection

You cannot distinguish a crashed node from a slow or unreachable one, so detectors use
timeouts. That trades detection speed against false positives. A **suspect** state
delays drastic action.

- **Code:** `internal/membership` (`Tick`), `internal/node/gossip.go`
- **Exercise:** run `scripts/failover-timing.sh`, then halve `SHC_DEAD_AFTER` and repeat.
  Next, use fault injection to add a 1 s delay to a peer and explain what the detector
  concludes.

## 8. Distributed membership

Nodes need a shared view of who is in the cluster.

- **Gossip** spreads records peer to peer.
- **Precedence rules** decide which record wins: here, generation (restarts), then
  incarnation (self-declared changes), with `left` terminal within a generation.
- Liveness is a *local* opinion. Phase is *self-declared*.
- **Dead marks** force a node that was declared dead to resynchronize before others trust
  it again.

- **Code:** `internal/membership`, `absorbGossip`
- **Exercise:** read `TestSelfMarkedDeadAndRemoved`. Draw a timeline of a node being
  declared dead, coming back, rejoining and becoming routable.

## 9. Consistency models

| Model | Meaning |
|---|---|
| **Linearizable** | Every operation appears to happen instantly at one point between its start and its end |
| **Eventual** | If writes stop, replicas converge |
| **Read-your-writes** | A client sees its own completed writes |
| **Monotonic reads** | A client never sees an older value after a newer one |

- **Code / doc:** [CONSISTENCY_MODEL.md](CONSISTENCY_MODEL.md)
- **Exercise:** describe an execution with N=3, W=2, R=1 in which a client reads a value
  older than one it just wrote. Hint: the coordinator changes between the write and the
  read.

## 10. Quorums

With N replicas, waiting for W on writes and R on reads gives R + W > N, so every read
quorum overlaps every write quorum. That intersection is necessary for fresh reads but
not sufficient for linearizability. Membership changes, LWW and the lack of a
write-back phase all break the guarantee.

- **Code:** `coordinateWrite`, `quorumRead`
- **Exercise:** set `SHC_WRITE_QUORUM=1` and explain which tests' guarantees weaken.
  Then sketch the extra round trip ABD-style register reads need for linearizability.

## 11. Timeouts and retries

Every remote call needs a deadline. A timeout means **unknown outcome**, not failure, so
retrying a non-idempotent operation can apply it twice. Back-off avoids hammering an
overloaded peer.

- **Code:** `pkg/client` (`do`), `route`
- **Exercise:** add a 3 s delay to a coordinator with fault injection and run a SET with
  a 1 s client timeout. Was the write applied? Check with a quorum read.

## 12. TTL and eviction

- **TTL** is a value's intended lifetime. Expiry is *lazy* (checked on read) plus
  *active* (sampled sweeps, as Redis does).
- **Eviction** discards live data under memory pressure: LRU evicts by recency, LFU by
  frequency.
- Both differ from **replica loss** (a failure) and from **durability** (a cache has none).
- Replicas must share an *absolute* expiry, or replication would extend lifetimes.

- **Code:** `internal/store`
- **Exercise:** implement a FIFO policy behind the `evictionPolicy` interface. Then
  explain why a tombstone that gets evicted can let a deleted value come back.

## 13. Rebalancing

When ownership changes, data must move without serving partially migrated state.

- **Code:** `internal/node/sync.go`, [REBALANCING.md](REBALANCING.md)
- **Exercise:** list every place a write can go while node4 joins (owners, joining owner,
  extras) and argue that no acknowledged write can be missed.

## 14. Observability

You can only operate what you can see:

- structured logs with correlation IDs;
- metrics with bounded labels (never keys);
- health checks that mean something (`/readyz` is not just "the process is up").

- **Code:** `internal/metrics`, `internal/node/metrics.go`
- **Exercise:** run Compose with `--profile monitoring`, kill a node, and write the
  PromQL that would alert on it.

## 15. Fault injection

Bugs in distributed systems hide in failure paths. Inject the failures on purpose:
crash, partition, delay, duplicate, reorder. Assert invariants, not just status codes.

- **Code:** `internal/transport/faults.go`, `internal/node/cluster_test.go`
- **Exercise:** write a test for an *asymmetric* partition (A can reach B, B cannot reach
  A). It's an untested gap: predict the outcome, then run it.

## 16. Benchmarking

Measure before optimizing:

- use a closed-loop generator;
- report percentiles, not averages;
- document the environment;
- profile to find the bottleneck;
- change one thing at a time.

- **Code:** `cmd/cachebench`, `scripts/bench.sh`, [BENCHMARKS.md](BENCHMARKS.md)
- **Exercise:** profile a node under `SET c=32` (`SHC_ENABLE_PPROF=true`). Pick the top
  item and estimate the best possible gain before changing anything.

---

## Interview explanations

**Consistent hashing.** "Keys and nodes hash onto a ring, and a key belongs to the next
N distinct nodes clockwise. Adding a node only takes keys from its neighbours, about 1/n
of the data, where modulo hashing moves almost all of it. I measured 19.9% vs 80%. Each
node gets 128 virtual points so arcs even out: per-node std-dev dropped from 64% to under
10%. The ring is immutable and swapped atomically, so routing never takes a lock."

**Replication.** "Each key has N=3 owners. The first owner that is currently routable
coordinates the write. It stamps a hybrid-logical-clock version, applies the write
locally, sends it to the others, and acknowledges after W=2 have applied it. Replicas
accept only strictly newer versions, so duplicates and reordering are harmless. Deletes
are versioned tombstones so they can't be undone by late writes."

**CAP trade-offs.** "During a partition I chose consistency over availability for the
minority. A node that sees half the ring or less refuses requests, and strict owner
quorums stop a minority holding fewer than W owners from writing. The majority stays
available. It isn't a CP system in the formal sense, because there is no consensus and
LWW can lose concurrent writes. But it avoids the common case of both sides serving
divergent data."

**Failure detection.** "Heartbeats with timeouts: suspect after 1.5 s, dead after 4 s.
A timeout can't prove a crash, so 'dead' only means 'stop routing, and make it resync
before trusting it again'. Data isn't deleted. Liveness is a local opinion. Only the
majority may remove a member permanently, after 30 s."

**Split-brain.** "Without consensus, two sides of a partition can both think they're
right. I limit it in three ways: strict owner quorums, minority refusal, and
majority-only membership removal. I document the residual window: before an isolated
node notices it's isolated, it can still serve stale R=1 reads. Removing the window
entirely would need consensus or leases. That's the next step if the use case needed
linearizability, but it isn't free."

**Rebalancing.** "One idempotent primitive does everything. The sender offers (key,
version) digests, the receiver asks for what it lacks or holds older, and the sender
pushes those entries. A joining node sits in the ring but isn't routable until it has
pulled its ranges, and meanwhile it receives every new write. So acknowledged writes
can't be missed, and old owners keep serving until the handoff. Old copies are dropped
only after every new owner confirms, with a compare-and-delete. It's throttled by a
token bucket, and after joining a fourth node the cluster held exactly keys × N copies."

**Consistency model.** "Per-key eventual consistency with last-writer-wins.
Read-your-writes holds while the coordinator is stable, because the coordinator applies
every write before acknowledging it. Quorum reads give R+W>N intersection plus read
repair. It is not linearizable, and I can show why: LWW drops concurrent writes, there is
no read write-back, and quorums move during membership changes."
