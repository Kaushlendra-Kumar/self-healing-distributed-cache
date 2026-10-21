# Requirements and specification

## 1. Problem statement

Applications need a shared, low-latency key-value cache that:

- outgrows one machine's memory;
- survives the loss of a machine without losing its contents or availability;
- recovers to a fully replicated state without manual work.

The goal is to build that cache from first principles: placement, replication,
membership, failure detection, recovery and rebalancing. The resulting behaviour must
be documented precisely, including where it is weaker than a consensus-based system.

## 2. Non-goals

- Durability. This is a cache; there is no disk persistence in this version.
- Linearizability or transactions; multi-key operations.
- Consensus-based membership (Raft/Paxos). See [ADR 0008](ADR/0008-no-consensus.md).
- Authentication, TLS and multi-tenancy (documented as deployment requirements instead).
- Very large clusters (hundreds of nodes) or WAN replication.
- Redis protocol compatibility.

## 3. System requirements

| | |
|---|---|
| Runtime | Go ≥ 1.22, Linux/macOS; no third-party dependencies |
| Deployment | Single static binary; Docker image; Docker Compose for local clusters |
| Cluster size | 1–20 nodes (all-to-all heartbeats) |
| Network | Private network with stable, resolvable node addresses |

## 4. Functional requirements

| ID | Requirement |
|---|---|
| F1 | `SET key value [ttl]`, `GET key`, `DELETE key` through any node |
| F2 | Keys are partitioned by consistent hashing with configurable virtual nodes |
| F3 | Each key is stored on N distinct nodes (configurable) |
| F4 | A write succeeds only after W of the key's owners applied it (configurable 1..N) |
| F5 | Reads at `one` or `quorum`; quorum reads return the newest version and repair stale replicas |
| F6 | Per-key TTL with an absolute expiry that replicas cannot extend |
| F7 | Bounded memory and/or entry count with LRU or LFU eviction |
| F8 | Nodes discover members from seeds and disseminate membership |
| F9 | Failure detection with alive / suspect / dead states and configurable timeouts |
| F10 | Requests are not routed to dead nodes; the remaining owners serve |
| F11 | Restarted or returning nodes resynchronize before serving |
| F12 | Members dead longer than a threshold are removed by the majority, and RF is restored |
| F13 | Join, graceful leave, removal and replacement trigger controlled data movement |
| F14 | Migration is throttled, observable and safe to interrupt |
| F15 | Health, readiness, metrics, cluster status and admin endpoints |
| F16 | Fault injection (partition, delay) for testing and demonstrations |
| F17 | A cluster-aware client library and CLI |

## 5. Non-functional requirements

| ID | Requirement |
|---|---|
| N1 | No data races (race detector clean) |
| N2 | Never replace a newer version of a key with an older one, on any path |
| N3 | Bounded resource use per request: frame size, key/value size, in-flight requests per connection, timeouts on every network call |
| N4 | Explicit, typed errors; an unknown outcome is reported as such, never as success |
| N5 | Metrics without high-cardinality labels |
| N6 | Deterministic, condition-based tests |
| N7 | Documented consistency guarantees, with no claims beyond what is implemented |

## 6. Architecture, protocol, data model

See [ARCHITECTURE](ARCHITECTURE.md) (components, concurrency) and [PROTOCOL](PROTOCOL.md)
(wire format, operations, errors, HTTP API).

Data model: `Entry{key, value, version{t (HLC), node}, expire_at (unix ms), tombstone}`.
Member records: `{id, addr, admin_addr, generation, incarnation, phase}`.

## 7. Key ownership and replication rules

- Ring members: phase ≠ `left`. Owners: the first N distinct members clockwise from `hash(key)`.
- Coordinator: the first **routable** owner (phase `active`/`leaving`, not dead, not dead-marked).
- Other nodes forward to it once. A forwarded request is never forwarded again.
- Writes are versioned by the coordinator's HLC and sent to every reachable owner and
  future owner. Only routable owners count toward W.
- If fewer than W owners are routable, the write is refused before anything is applied.

## 8. Membership model

- Each node is the authority on its own phase. Records are ordered by (generation,
  incarnation), and `left` is terminal within a generation.
- Liveness is local and never gossiped as fact. Dead marks are gossiped only to make the
  marked node resync.
- Removal requires a strict majority view.

See [FAILURE_MODES](FAILURE_MODES.md).

## 9. Consistency guarantees

See [CONSISTENCY_MODEL](CONSISTENCY_MODEL.md).

## 10. Failure detection and recovery

See [FAILURE_MODES](FAILURE_MODES.md) and [REBALANCING](REBALANCING.md).

## 11. Concurrency model

See [ARCHITECTURE §5](ARCHITECTURE.md#5-concurrency-model).

## 12. Error handling

Every network call has a deadline. Remote errors carry a machine-readable code
([PROTOCOL](PROTOCOL.md#error-codes)). Transport failures surface as "unknown outcome";
the client library retries them (at-least-once). Malformed input never crashes a node
(fuzzed).

## 13. Security considerations

- No authentication or encryption. **Both ports must stay on a private network.** Anyone
  who can reach port 7000 can read and write any key and inject `GOSSIP` or `SYNC_PUSH`
  messages, which can corrupt membership or data.
- The admin port can remove members and shut nodes down. Fault injection and pprof are
  disabled by default.
- Inputs are size-bounded to limit memory-exhaustion attacks. That's not a substitute
  for authentication.
- Production requirements: mTLS between nodes, client authentication, network policies.
  See [DEPLOYMENT](DEPLOYMENT.md#security) and [ROADMAP](ROADMAP.md).

## 14. Operational requirements

Configuration via environment variables with validation; graceful shutdown on SIGTERM
(optionally with handoff); container health checks via `cachenode -probe`; runbooks in
[OPERATIONS](OPERATIONS.md).

## 15. Test strategy

See [TESTING](TESTING.md).

## 16. Acceptance criteria

Each criterion was defined before implementation and is marked with how it was verified.

| # | Criterion | Status | Evidence |
|---|---|---|---|
| A1 | 3 nodes start as separate processes and converge to all-routable | ✅ | e2e test, demo |
| A2 | A value written through any node is readable through every node | ✅ | `TestThreeNodeRoutingAndReplication`, demo step 2 |
| A3 | With N=3, each key ends on exactly its 3 owners; with N=2 on 5 nodes, exactly 2 | ✅ | `TestReplicaPlacementMatchesRing` |
| A4 | Adding a 5th node remaps ≤ 30% of keys (modulo baseline ~80%) | ✅ 19.9% | ring test |
| A5 | 256 vnodes reduce per-node key std-dev below 15% and below 1 vnode | ✅ 9.7% vs 64% | ring test |
| A6 | A SIGKILLed node is declared dead within `DEAD_AFTER` + 1 heartbeat | ✅ 4.17 s ≤ 4.5 s | failover timing |
| A7 | With one of three nodes down, 100% of reads and writes succeed | ✅ | crash test, e2e, demo |
| A8 | A node restarted empty is not routed to until it holds every key it owns, including writes made while it was down | ✅ | crash test, e2e |
| A9 | A partitioned node refuses requests once it detects the minority; after heal it holds the majority's writes, and deletes made during the partition stay deleted | ✅ | partition test, demo step 10 |
| A10 | No acknowledged write is lost when a node joins under continuous writes | ✅ | join test |
| A11 | After join + handoff, total copies = keys × N | ✅ 150,000 = 50k × 3 | failover timing |
| A12 | A gracefully leaving node loses no keys | ✅ | leave test |
| A13 | Losing all replicas yields a miss, not an error or stale data | ✅ | total loss test |
| A14 | Writes without W routable owners are refused and not applied | ✅ | quorum test |
| A15 | Older or duplicate replication never overwrites newer data | ✅ | store + node tests |
| A16 | TTL expiry is absolute and identical on all replicas | ✅ | TTL test |
| A17 | Memory never exceeds the configured budget (store accounting) | ✅ | store test |
| A18 | Race detector clean on the full suite | ✅ | CI |
| A19 | Healthy-cluster benchmarks complete with 0 errors | ✅ | [BENCHMARKS](BENCHMARKS.md) |
| A20 | Clock skew does not reorder sequential writes; non-transitive partitions cause no removals; concurrent joins + removal lose no acknowledged write | ✅ | skew, non-transitive and concurrent-membership tests |
| A21 | A node crash and restart under load cause zero client-visible errors | ✅ 0 errors / 748k ops | [BENCHMARKS](BENCHMARKS.md#behaviour-under-node-failure) |
| A22 | A node joining during a partition loses no acknowledged write; the isolated node resyncs | ✅ | `TestJoinDuringPartition` |
| A23 | Behaviour on real separate machines (packet loss, latency) and a live cloud deployment | ⚠️ pending: needs cloud VMs | [DEPLOYMENT](DEPLOYMENT.md) |
