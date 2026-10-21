# ADR 0004: Per-key coordinated replication with W-of-N acknowledgement

## Context
Copies must survive node loss. Acknowledged writes should survive the loss of any
single node. The behaviour has to stay explainable.

## Options
- **Primary/replica per shard, asynchronous:** the primary acknowledges immediately (like
  Redis default). It has the lowest latency, but acknowledged writes can be lost on
  failover, and failover needs an election.
- **Primary/replica, synchronous to all:** strong durability, but any slow replica blocks
  writes.
- **Leaderless (Dynamo):** any replica coordinates. It is highly available but needs
  conflict resolution for every concurrent write (vector clocks or LWW).
- **Per-key preferred coordinator + W-of-N quorum (chosen):** the first routable owner
  versions and fans out, and acknowledges after W owners applied the write.

## Decision
- **Coordinator:** the first routable owner in the preference list. Other nodes forward
  to it once.
- **Writes:** the coordinator stamps an HLC version, applies locally, sends `REPLICATE`
  to all reachable owners (and future owners during a leave), and acknowledges after
  W = ⌊N/2⌋+1 routable owners (configurable) have applied it.
- **Refusal:** if fewer than W owners are routable, the write is refused before being
  applied.
- **Replicas:** they apply only strictly newer versions.

## Consequences
- An acknowledged write survives the loss of any N−W owners.
- A slow replica does not delay writes (measured: unaffected by a 2 s delay). Its copy is
  repaired by anti-entropy.
- Failover needs no election: the next routable owner simply becomes the coordinator.
  The price is that two nodes with different membership views may coordinate the same
  key concurrently, and LWW then resolves the conflict (ADR 0006).
- Replicated writes cost ~2× latency at c=1 and ~2.5× throughput at c=32 on a shared
  host (measured).
