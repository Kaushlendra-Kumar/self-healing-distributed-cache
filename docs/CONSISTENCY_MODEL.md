# Consistency model

This document states what the cache guarantees, under which assumptions, and where the
guarantees end. The short version: **per-key eventual consistency with last-writer-wins
ordering, majority write acknowledgement, and optional quorum reads.** It is **not**
linearizable and makes no claim to be.

## Parameters

| Symbol | Meaning | Default | Config |
|---|---|---|---|
| N (RF) | Owners per key (ring preference list) | 3 | `SHC_REPLICATION_FACTOR` |
| W | Routable owners that must apply a write before it succeeds | ⌊N/2⌋+1 = 2 | `SHC_WRITE_QUORUM` (0 = majority) |
| R | Owners consulted by a `quorum` read | ⌊N/2⌋+1 = 2 | fixed |
| default read | `one` or `quorum` | `one` | `SHC_READ_CONSISTENCY`, or per request |

## Versions and conflict resolution

- Every write gets a version `(T, node)` from the **coordinator**. `T` is a hybrid logical
  clock: wall-clock milliseconds shifted left 16 bits, plus a logical counter.
- The clock never goes backwards locally. Every node advances its clock past any version
  it receives (replication, sync digests, sync pushes), so a coordinator never issues a
  version lower than one it has already seen.
- Replicas apply an entry **only if it is strictly newer** than what they hold. Ties on
  `T` are broken by node ID, so every replica resolves the same conflict the same way.
- Consequences:
  - Replication is **idempotent**: duplicates are ignored.
  - **Order-insensitive**: a delayed old update cannot overwrite a newer one.
  - Sync is **safe to interrupt and repeat**.
- **Deletes are tombstones** with a version. A delete therefore beats older writes
  arriving late from a stale replica, and is not "undone" by anti-entropy. Tombstones
  expire after `SHC_TOMBSTONE_TTL` (60 s).

### Clock-skew limitation

LWW orders *concurrent* writes by clocks, not by real time. Suppose two different
coordinators write the same key at nearly the same moment. That can only happen when
they disagree about who the first routable owner is, for example during a membership
change or a partition. Then the write with the larger HLC wins, even if it happened
"first" in real time. With skew Δ, a write can lose to an earlier write from a node
whose clock is up to Δ ahead.

Tested with injected skew: a coordinator 10 s ahead writes, then dies, and the next
coordinator's later write still wins on every replica, because the next coordinator's
HLC had already advanced past the version it received
(`TestClockSkewDoesNotReorderSequentialWrites`). Disabling clock observation makes that
test fail.

In the common case there is no conflict: a single coordinator (the first routable owner)
serializes all writes to a key, and its HLC is monotonic.

## Write semantics

| Outcome | Meaning |
|---|---|
| `ok` (+ version, acks) | At least W routable owners applied the write. |
| `unavailable` | Fewer than W owners were routable in the coordinator's view. **Nothing was applied.** |
| `quorum_failed` (+ version, acks) | The coordinator applied it, but fewer than W acks arrived within `SHC_REQUEST_TIMEOUT`. The value **may** be visible and may later spread through anti-entropy. Treat it as "unknown outcome". |
| `minority` | The receiving node sees at most half of the ring and refuses (default). |
| transport error / timeout | Unknown outcome. The client library retries, so writes are **at-least-once**. A retried SET writes the same value again with a newer version. |

What happens if the primary crashes after applying a write locally but before replicating it?

- **Before it reached W:** the client got an error or timeout (unknown outcome). The value
  may exist only on the crashed node and is lost with its memory. If the node restarts,
  it comes back empty.
- **After it reached W:** at least W−1 other owners hold it. The next routable owner
  becomes coordinator and serves it. Anti-entropy copies it to any replica that missed it.

What if a replica receives an older update after a newer one? It ignores it (strictly-newer
rule). The `TestDuplicateAndOutOfOrderReplication` test covers this.

## Read semantics

| Mode | Behaviour | Guarantee |
|---|---|---|
| `one` | Served by the first routable owner, usually the coordinator that wrote the key. | **Read-your-writes and monotonic reads while the first routable owner is stable**, because that owner applied every acknowledged write before acknowledging it. Not guaranteed across a coordinator change: the next owner might have missed a write that reached W through a different replica set. |
| `quorum` | Asks every routable owner, waits for R answers, returns the newest version (a tombstone counts), and repairs older answers in the background (**read repair**). | With **static membership**, R + W > N means every quorum read intersects every successful write's replica set, so it returns that write or a newer one. |

### Why R + W > N alone is *not* linearizability

Quorum intersection is necessary but not sufficient:

- **No write-back before returning.** A quorum read that sees a new value on one replica
  and old values on others returns the new value. A concurrent second reader may still
  see the old value from a different quorum.
- **Membership changes move quorums.** During a join, removal or partition, two nodes
  can compute different owner sets for the same key.
- **LWW silently drops one of two concurrent writes.** There is no conflict detection
  (no vector clocks, no siblings).
- **`quorum_failed` writes can become visible later** through anti-entropy.

## Staleness windows (where `one` reads can be stale)

1. **Partition, before detection.** An isolated node has not yet declared its peers dead
   (up to `SHC_DEAD_AFTER`, 4 s). Until then it still believes it is in the majority and
   may answer `one` reads from its local copy. Once it sees at most half of the ring, it
   refuses all requests (`minority`). The demo shows both phases.
2. **Membership propagation.** Ownership and phase changes spread by gossip (one
   heartbeat, 500 ms). A recovered node gossips its new phase immediately, but other
   changes can take one round to propagate.
3. **Replica lag.** The (N−W) replicas not waited on are updated asynchronously.
   `shc_replication_inflight` shows writes still awaiting acknowledgement. These replicas
   only serve `one` reads after a coordinator change, or as part of a quorum read
   (which repairs them).

## Partitions and split-brain

There is no consensus protocol (see [ADR 0008](ADR/0008-no-consensus.md)). The design
reduces split-brain with three mechanisms:

- **Strict owners.** Writes need W acks from the key's *ring owners*. A minority partition
  holding fewer than W owners of a key cannot write it. Unlike sloppy quorums, nodes
  outside the preference list never stand in.
- **Minority refusal.** A node that sees ≤ half the ring refuses reads and writes
  (`SHC_REJECT_WHEN_MINORITY=true`).
- **Majority-only removal.** Only a node that sees a strict majority may remove dead
  members and reassign their ranges.

**Remaining risk (documented, not solved).** In a 5-node cluster split 3 | 2 with N=3, W=2,
some key may have 2 owners on the minority side. If minority refusal is disabled, both
sides can accept writes for that key until the majority removes the minority members.
With refusal enabled (the default), the minority side refuses, and the risk shrinks to
the detection window before it notices it is in the minority. On heal, LWW keeps the
higher version and the other write is lost.

## Recovery from stale replicas

A node can rejoin after any of the following:

- a crash and restart (new generation);
- being declared dead (a dead mark at its current incarnation);
- being isolated in a minority;
- being removed (rejoins as a new generation).

In each case it enters `joining`. It is not routed to and its acks don't count, but it
receives new writes. It pulls every key it owns from all reachable peers with
version-checked sync, then becomes `active` with a higher incarnation. A stale value can
never overwrite a newer one in either direction, because sync applies strictly-newer
versions only.

## TTL semantics

- TTL is converted by the coordinator into an **absolute** expiry (`expire_at`, unix ms),
  which replicates unchanged. Replicas and sync **cannot extend** a key's lifetime
  (`TestTTLIsAbsoluteAcrossReplicas`).
- Expiry is evaluated against each node's own wall clock. Clock skew Δ makes a key
  expire up to Δ earlier or later on different nodes. `TestClockSkewShortensTTLOnFastReplica`
  pins this down: a replica 10 s ahead treats a 5 s TTL as already expired.
- An expired entry is treated as absent everywhere: reads, digests and pushes.

## What eviction means

Eviction under memory pressure deletes a local copy without any version or tombstone,
so it is invisible to the protocol.

- If one replica evicts a key, a `one` read on that node misses, and a quorum read
  repairs it from another replica.
- If every replica evicts it, the key is gone. Eviction is a capacity decision, not a
  replication failure, and the cache is not a durable store.
- Evicting a *tombstone* can let an old value come back through anti-entropy from a
  replica that still holds it. Tombstones are small and short-lived, so this is rare,
  but it is possible.
