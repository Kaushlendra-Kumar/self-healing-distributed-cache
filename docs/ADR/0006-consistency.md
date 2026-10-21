# ADR 0006: Eventual consistency with HLC + LWW and optional quorum reads

## Context
A cache usually favours latency and availability, but stale or resurrected data must
stay bounded and explainable.

## Options
- **Linearizable per key** (Raft per range, or leases): the strongest model, but
  consensus on every write.
- **Quorum + vector clocks + siblings:** detects concurrent writes but pushes merging to
  the application.
- **Quorum + LWW** with a monotonic per-node clock: simple, deterministic convergence;
  silently drops one of two concurrent writes.

## Decision
- Versions are `(HLC timestamp, coordinator ID)`. The HLC never goes backwards and
  advances past every version a node receives. Conflicts resolve by last-writer-wins.
- Deletes are versioned tombstones.
- TTL is an absolute expiry set by the coordinator.
- Reads: `one` by default (read-your-writes while the coordinator is stable), or
  `quorum` (R = ⌊N/2⌋+1, newest wins, with read repair).

## Consequences
- Duplicate, delayed and reordered messages are harmless. Every sync path is idempotent.
- Not linearizable. Concurrent writes from two coordinators (only possible during
  membership changes or partitions) are ordered by clocks, and one is lost. Documented
  in CONSISTENCY_MODEL.
- Clock skew affects conflict order and TTL precision, which makes NTP an operational
  requirement.
