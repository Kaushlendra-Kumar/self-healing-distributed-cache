# ADR 0002: Length-prefixed TCP with JSON payloads; HTTP for admin

## Context
Clients and nodes exchange small, latency-sensitive messages: gets, sets, replication,
heartbeats, sync batches. Operators need easy inspection.

## Options
| | Custom TCP framing | HTTP/JSON | gRPC |
|---|---|---|---|
| Framing | Must design (length prefix) | HTTP | HTTP/2 + protobuf |
| Multiplexing | Must build (request IDs) | Connection pool, or HTTP/2 | Built in |
| Per-request overhead | Lowest | Headers, text | Low |
| Learning value | Highest: framing, partial reads, correlation | Low | Low; hides the mechanics |
| Dependencies | None | None | protobuf toolchain + grpc-go |
| Debuggability | Needs a tool | curl | grpcurl |

## Decision
- **Data plane:** a custom framed protocol (4-byte length + version byte + payload) with
  request IDs for multiplexing many in-flight requests over one connection per peer.
  The payload is **JSON** for now.
- **Admin plane:** plain HTTP/JSON (health, metrics, cluster status, a KV gateway for
  demos).

## Consequences
- Framing, bounded frames, partial reads, malformed input and dropped connections are
  handled explicitly, tested, and fuzzed.
- JSON costs ~20% of node CPU (measured) and inflates values by 4/3 (base64). The
  version byte lets a binary encoding be introduced as protocol v2 without breaking v1
  peers.
- No TLS yet (see DEPLOYMENT).
