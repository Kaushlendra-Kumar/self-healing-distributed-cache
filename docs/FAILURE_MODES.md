# Failure modes

For each failure: what the cluster observes, what it does, what is guaranteed, and which
test exercises it. Timings use the defaults (heartbeat 500 ms, suspect 1.5 s, dead 4 s,
remove 30 s).

## Failure detection basics

Every node sends `GOSSIP` to every peer each heartbeat interval and records direct
evidence (a request from the peer, or a response). Liveness is a **local opinion**:

| State | Condition | Routed to? |
|---|---|---|
| alive | heard from within `SUSPECT_AFTER` | yes |
| suspect | silent for ≥ `SUSPECT_AFTER` | yes (avoids flapping on a single lost heartbeat) |
| dead | silent for ≥ `DEAD_AFTER` | **no**; a dead mark is recorded at the peer's `(generation, incarnation)` |

A timeout **cannot** prove that a node crashed. The peer may be:

- paused (GC, VM stall);
- overloaded;
- reachable from some nodes but not from us.

That is why "dead" only means "stop routing to it and make it resync before trusting it
again". It does not mean "delete its data" (that requires majority removal), and it is
never gossiped as fact.

- **False positives** (declaring a healthy node dead) cost a resync of that node.
- **False negatives** (an undetected crash) last at most `DEAD_AFTER`. During that time,
  requests routed to the crashed node fail and the client retries another owner.

## Scenarios

### A node crashes and stays down

- **Detection:** peers suspect it after ~1.5 s and declare it dead after ~4 s. Measured:
  4.17 s (see [BENCHMARKS](BENCHMARKS.md)).
- **Behaviour:** the next routable owner coordinates its keys. With N=3 and W=2, writes
  continue on the 2 survivors. Clients see errors only for requests that were in flight
  to the dead node. The client library retries those on another node.
- **Healing:** after `REMOVE_DEAD_AFTER` (30 s), a node that sees a strict majority, and
  whose opinion is corroborated (a strict majority of ring members report the member
  dead in their gossip), marks the member `left`. The ring shrinks, new owners take its ranges, and rebalancing
  copies the data to them, restoring N copies.
- **Tests:** `TestNodeCrashFailoverAndRecovery`, `TestAutoRemovalRestoresReplicationFactor`,
  the e2e `TestThreeProcessCrashAndRecovery` (real SIGKILL).

### A node restarts with an empty memory

- The process starts a **new generation** (start time in ns) in phase `joining`.
  Peers adopt the new record (a higher generation supersedes the old one). They keep it
  in the ring, so ownership does not change, but they don't route to it.
- It pulls its ranges from every reachable peer and becomes `active`. Writes made during
  the sync are dual-written to it.
- **Measured:** restart → ready with 50k keys: 2.6 s.
- **Tests:** `TestNodeCrashFailoverAndRecovery` (verifies writes made while it was down
  are present after restart), e2e.

### A node crashes but its data survives on disk

Not applicable: this version has no persistence. A restart is always "empty memory".
Adding a snapshot or AOF would change only the starting state of the joining phase. The
version-checked sync would still bring it up to date (see [ROADMAP](ROADMAP.md)).

### A node returns with stale data (pause or partition, no restart)

- Peers declared it dead and recorded dead marks. It returns at the same generation and
  incarnation, so those marks still cover it, and peers do **not** route to it.
- The marks reach it through gossip. Its own minority detection may also have fired. It
  switches to `joining` (incarnation+1), resyncs, and becomes `active` (incarnation+1).
  The marks no longer cover it, and it is routable again.
- While isolated with ≤ half the ring visible, it refuses requests instead of serving its
  stale copy. Before it notices (≤ `DEAD_AFTER`) it can still serve `one` reads; see the
  [staleness windows](CONSISTENCY_MODEL.md#staleness-windows-where-one-reads-can-be-stale).
- The **majority side is not disrupted.** Dead marks made while a node was in the
  minority are never gossiped and are cleared when it heals. Without this rule, the
  isolated node would force every healthy node to resync. An earlier version had exactly
  this bug; the partition test caught it and now asserts the majority recovered only once.
- **Tests:** `TestPartitionedNodeRejectsRequestsAndResyncsOnHeal` (stale value replaced, a
  delete made during the partition is not undone, majority nodes not disrupted),
  `TestFlappingNodeIsNotRoutedUntilResynced`.

### Repeated flapping

Each dead → alive cycle forces a resync before the node is routed to again. That
throttles the impact of a flapping node: it cannot serve stale data between flaps.
`shc_membership_transitions_total{kind="liveness",to="dead"}` counts the flaps. The suspect
state absorbs single missed heartbeats. Remaining gap: there is no exponential back-off
for nodes that flap continuously; an operator should remove one with `/admin/remove`.

### A slow node or slow network

A replica slower than `REQUEST_TIMEOUT` does not delay writes, as long as W other owners
answer. Its update is retried by anti-entropy. A slow coordinator delays its own
requests; the client times out and retries another node.

- **Test:** `TestSlowReplicaDoesNotBlockQuorumWrite` (2 s injected delay; the write
  completes in < 400 ms and anti-entropy repairs the slow replica).

### Too few replicas to write

If fewer than W owners are routable, writes fail **before** being applied (`unavailable`),
and reads still work from any routable owner.

- **Test:** `TestWriteFailsWithoutQuorum`.

### Every replica of a key is lost

There is no durable source. Once the dead owners are removed and the range has new
owners, reads return a plain **miss** (`found=false`): no error, no invented data. The
key can be written again.

- **Test:** `TestLossOfAllReplicasIsAMiss` (5 nodes, N=2, both owners killed).

### Network partition

- **Majority side** (sees > half the ring): keeps serving. It removes minority members
  after `REMOVE_DEAD_AFTER` and restores N on its side.
- **Minority side** (≤ half): refuses all requests (`minority`) once it notices. It never
  removes members or reassigns ownership. Its writes would fail anyway when it holds
  fewer than W owners of a key.
- **Heal:** minority nodes clear their dead marks, re-enter `joining`, resync, and become
  `active`. If they were removed meanwhile, they see themselves `left` and rejoin as a
  new generation.
- **Not prevented:** see the split-brain discussion in
  [CONSISTENCY_MODEL](CONSISTENCY_MODEL.md#partitions-and-split-brain). If refusal is
  disabled, or during the detection window, writes accepted on both sides are reconciled
  by LWW, and one of them is lost.
- **Non-transitive partitions** (A ↮ C, but both reach B): A and C each see a majority and
  each other as dead. They stop routing to each other, but neither is removed, because
  removal needs a strict majority of the ring to *report* the member dead (B reports both
  alive). Writes keep succeeding through B. On heal, A and C each resync once, because
  of the other's dead mark. Tested by `TestNonTransitivePartitionDoesNotRemoveMembers`.
  Before that rule existed, A and C removed each other in a loop.

### Migration interrupted

The sync protocol is idempotent: digests are compared by version and pushes apply only
newer entries. Retrying or resuming after a crash is therefore safe, and there is no
migration log to recover.

- A joining node retries sources that fail but are still reachable, and drops sources
  that become dead.
- After `JOIN_TIMEOUT` it becomes active anyway, with state `incomplete` (metric and
  status), and relies on anti-entropy.
- A source that crashes mid-push leaves its keys to be fetched from other owners.
- **Test:** `TestJoinSurvivesSourceCrashMidSync` (a source killed mid-sync; the joining
  node still ends with every key it owns).

### Duplicate, delayed or out-of-order messages

Strictly-newer versioning makes them harmless.

- **Tests:** `TestDuplicateAndOutOfOrderReplication`, `store.TestVersionOrderingIsIdempotent`.

### Malformed or oversized messages

- A frame larger than `MAX_FRAME_BYTES` is rejected before allocation, and the connection
  is closed after an error reply.
- A malformed payload inside a valid frame gets a `bad_request` reply, and the connection
  stays usable.
- **Tests:** `TestMalformedPayloadKeepsConnection`, the `FuzzReadFrame` fuzz test.

### Memory exhaustion

The store evicts (LRU or LFU) to stay within `MAX_MEMORY_BYTES` and `MAX_ENTRIES`. A
single entry larger than one shard's budget (total ÷ 16) is rejected with `too_large`.
Accounting is approximate (key + value + ~96 B per entry), not real heap usage. Leave
headroom for the Go runtime: set `SHC_MAX_MEMORY_BYTES` to at most about 50–60% of the
container memory limit.
