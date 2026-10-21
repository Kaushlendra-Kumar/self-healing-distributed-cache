# Roadmap

Each feature lists the problem it solves, the concepts it needs, its complexity, what
it depends on, and how it is tested.

## MVP: a small but correct multi-node cache ✅

| Feature | Problem solved | Concepts | Complexity | Depends on | Tested by |
|---|---|---|---|---|---|
| Single-node store (SET/GET/DELETE, TTL, LRU/LFU, bounds) | Fast bounded in-memory storage | Hash maps, locking, eviction policies, lazy vs active expiry | Low | – | store unit tests, `-race` |
| Wire protocol + transport | Talking over TCP reliably | Framing, multiplexing, deadlines, back-pressure | Medium | – | protocol/transport tests, fuzzing |
| Consistent-hash ring | Spreading keys and limiting movement | Hashing, vnodes, preference lists | Low | – | property tests, remap ratio |
| Routing + forwarding, client library | Any node or client reaches the owner | Ownership vs placement | Low | ring, transport | cluster tests |

## Intermediate: replication and automatic recovery ✅

| Feature | Problem solved | Concepts | Complexity | Depends on | Tested by |
|---|---|---|---|---|---|
| Coordinated replication, W-of-N acks | Surviving node loss without losing acknowledged writes | Quorums, acks, unknown outcomes | Medium | MVP | placement, quorum, slow-replica tests |
| HLC versions, LWW, tombstones | Ordering, idempotence, deletes | Logical clocks, LWW | Medium | replication | out-of-order, concurrent-write tests |
| Heartbeat failure detection | Knowing when to stop routing | Timeouts, false positives/negatives | Medium | transport | flapping, crash tests |
| Joining phase + resync | Restarted or stale nodes must not serve | Generations, incarnations | Medium | sync, detection | crash/restart, e2e |

## Advanced: membership, rebalancing, partitions, consistency ✅

| Feature | Problem solved | Concepts | Complexity | Depends on | Tested by |
|---|---|---|---|---|---|
| Gossip membership, seeds | Dynamic membership without a coordinator | Gossip, record precedence | Medium | detection | membership tests |
| Dead marks | Returning partitioned nodes must resync | Epochs/fencing-lite | Medium | membership | partition, flapping tests |
| Minority refusal, majority-only removal | Limiting split-brain | Partitions, CAP | Medium | membership | partition, total-loss tests |
| Digest/push sync engine, throttle | Join, leave, removal, anti-entropy, handoff | Anti-entropy, idempotence, rate limiting | High | replication | join, leave, interruption tests |
| Quorum reads + read repair | Fresher reads, passive repair | R+W>N, repair | Medium | replication | read-repair test |

## Stretch goals (not implemented)

| Feature | Why | Notes |
|---|---|---|
| Binary protocol encoding | JSON is ~20% of CPU (measured) | The frame version byte allows a protocol v2 alongside v1 |
| Write coalescing, buffer pools | Syscalls ~25%, GC ~10–15% | Per-connection writer goroutine |
| Merkle-tree anti-entropy | O(keys) digest passes → O(differences) | Per ring range; compare roots first |
| SWIM-style failure detection | O(n²) heartbeats; better accuracy | Indirect probes (ping-req), suspicion with refutation, piggybacked dissemination |
| Phi-accrual detector | Adaptive timeouts | Replace fixed thresholds |
| Hinted handoff | Faster repair for briefly-down replicas | Store hints for unreachable owners |
| Persistence (snapshot/AOF) | Restart without full resync | Only the joining phase's starting state changes |
| Consensus-managed membership (Raft) | Eliminate membership split-brain | Only if a stronger model is required; see ADR 0008 |
| Linearizable mode | Strong reads/writes per key | Per-key leases or a per-range Raft group |
| mTLS, client auth, ACLs | Required for any shared network | Go `crypto/tls`, cert rotation |
| Online reconfiguration of N, W, vnodes | Today all nodes must agree at startup | Versioned cluster config |
| History checker (Porcupine) | Quantify stale reads under faults | Record histories in fault tests |
| Hot-key handling | Skewed workloads | Client-side near cache, key splitting |
