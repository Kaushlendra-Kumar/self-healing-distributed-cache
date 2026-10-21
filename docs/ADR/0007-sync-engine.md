# ADR 0007: One digest/push sync engine for all data movement

## Context
Data has to move in five situations: a node joins, recovers, leaves, is removed (restore
N), or a replica missed writes (repair). Five bespoke protocols would be five sets of
edge cases.

## Options
- **Per-case protocols** with explicit ownership-transfer states and migration logs.
- **Merkle-tree anti-entropy** (Dynamo/Cassandra): efficient for large, mostly-equal
  data sets, but more complex.
- **Digest/push with version checks (chosen):** offer `(key, version)`, receive the list
  of needed keys, push those entries.

## Decision
A single exchange (`SYNC_DIGEST` → needed keys → `SYNC_PUSH`), applied with strictly-newer
semantics and throttled by a token bucket. The trigger decides only *what* to offer and
*to whom*:

- **join/recovery:** peers push the target's ranges on `SYNC_REQUEST`;
- **ring change and periodic anti-entropy:** every key goes to every reachable owner;
- **leave:** everything goes to the future owners;
- **cleanup:** non-owned keys are dropped after all owners confirm.

## Consequences
- Interrupting or repeating any migration is safe. No resume state is needed.
  `TestJoinSurvivesSourceCrashMidSync` kills a source mid-sync.
- Each pass is O(local keys) digests. That's fine at the measured scale (50k keys
  rebalance in seconds) but wasteful for large stores, which is why Merkle trees are on
  the roadmap.
- Joining nodes stay non-routable until their sync finishes, and receive all new writes
  meanwhile, so no special "migration in progress" read path is needed.
