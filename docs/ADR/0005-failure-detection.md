# ADR 0005: All-to-all heartbeats, suspect/dead states, dead marks

## Context
Nodes must stop routing to failed peers quickly, without overreacting to brief stalls,
and must not trust a peer that may have missed writes.

## Options
- **Fixed-timeout heartbeats, all-to-all:** simple, with predictable detection time.
  O(n²) messages; every node forms its own opinion.
- **Gossip-based detection (SWIM):** O(n) per round; indirect probes reduce false
  positives; suspicion with refutation. More moving parts.
- **Phi-accrual:** adaptive thresholds based on observed heartbeat inter-arrival times.
- **Central monitor / consensus leases:** a single authority. It must be highly
  available itself.

## Decision
Every 500 ms each node sends `GOSSIP` (its full membership table) to every peer. Liveness
is local:

- **suspect** after 1.5 s; still routed, which absorbs single lost heartbeats;
- **dead** after 4 s; not routed.

Declaring a peer dead records a **dead mark** at its `(generation, incarnation)`. The
mark is gossiped to the marked peer, which must resync and bump its incarnation before
anyone routes to it again. A node that sees ≤ half the ring neither gossips its marks
nor keeps them after healing.

## Consequences
- Measured detection: 1.66 s to suspect and 4.17 s to dead after a SIGKILL.
- A returning node with stale data is never routed to before it resyncs, which handles
  pauses, partitions and flapping.
- O(n²) heartbeats limit the design to small clusters (≤ ~20). SWIM is on the roadmap.
- Timeouts can be wrong. A false positive costs one resync; it never causes data loss,
  because data is never deleted on suspicion.
