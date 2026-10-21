# Architecture Decision Records

| # | Decision | Status |
|---|---|---|
| [0001](0001-language-go.md) | Go, standard library only | Accepted |
| [0002](0002-protocol.md) | Length-prefixed TCP with JSON payloads for the data plane; HTTP for admin | Accepted |
| [0003](0003-placement.md) | Consistent hashing with virtual nodes | Accepted |
| [0004](0004-replication.md) | Per-key coordinated replication with W-of-N acknowledgement | Accepted |
| [0005](0005-failure-detection.md) | All-to-all heartbeats with suspect/dead states and dead marks | Accepted |
| [0006](0006-consistency.md) | Eventual consistency, HLC + last-writer-wins, optional quorum reads | Accepted |
| [0007](0007-sync-engine.md) | One digest/push sync engine for all data movement | Accepted |
| [0008](0008-no-consensus.md) | No consensus protocol in this version | Accepted |

Format: context → options → decision → consequences.
