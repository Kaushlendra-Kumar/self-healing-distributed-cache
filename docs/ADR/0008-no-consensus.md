# ADR 0008: No consensus protocol in this version

## Context
Raft or Paxos would give a single agreed membership view, and eliminate membership
split-brain. Adding it only to look sophisticated would be the wrong reason.

## Options
- **Raft-managed membership / configuration:** agreed epochs; every membership change is
  a committed log entry. Needs a stable voter set, and loses availability without a
  majority of voters.
- **Raft per key range (like CockroachDB or TiKV):** linearizable data. A very large step
  in complexity.
- **Gossip membership + quorum safeguards (chosen):** each node is the authority on its
  own phase. Removals need a majority view. A minority refuses requests.

## Decision
No consensus. Split-brain is reduced by:

- strict owner quorums (no sloppy quorums);
- minority refusal;
- majority-only removal;
- dead marks that force returning nodes to resync.

## Consequences
- The design stays small enough to understand and test thoroughly.
- Residual risks are documented, not hidden:
  - the detection window before an isolated node notices it is in the minority;
  - divergent writes when minority refusal is disabled;
  - concurrent membership changes are not totally ordered.
- Revisit this decision if a use case needs linearizable reads and writes, or online
  reconfiguration of N, W and vnodes.
