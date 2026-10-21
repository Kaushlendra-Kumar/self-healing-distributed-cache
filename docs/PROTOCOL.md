# Protocol

Nodes expose two interfaces:

| Port (default) | Protocol | Used by |
|---|---|---|
| 7000 | framed TCP (this document) | clients (`pkg/client`, `cachectl`) and peer nodes |
| 8000 | HTTP/1.1 + JSON | operators, Prometheus, load balancer health checks, `curl` |

## Framing

```
+-------------------+-----------+--------------------------+
| length (uint32 BE)| version u8| payload (JSON Message)   |
+-------------------+-----------+--------------------------+
  length = 1 + len(payload)       version = 1
```

- TCP is a byte stream with no message boundaries. The length prefix tells the reader
  exactly how many bytes form the next message, however the kernel splits them across
  reads. `io.ReadFull` handles partial reads; this is tested with a one-byte-at-a-time
  reader.
- `length` is checked against `SHC_MAX_FRAME_BYTES` (default 4 MiB) **before** allocating.
  A larger frame closes the connection after an error reply.
- An unknown version byte is rejected, which leaves room for a future binary encoding.
- A frame whose JSON does not parse gets a `bad_request` reply, and the connection stays
  open, because framing is still intact.
- The decoder is fuzzed (`FuzzReadFrame`).

JSON payloads keep the protocol debuggable. Values (`[]byte`) are base64-encoded, which
adds about 33% overhead. The profile in [BENCHMARKS](BENCHMARKS.md) shows JSON at
about 20% of node CPU, which makes it the main candidate for a binary encoding
(see [ADR 0002](ADR/0002-protocol.md)).

## Multiplexing and correlation

Each message carries a connection-scoped `id`. A client may send many requests on one
connection without waiting, and the server answers each in any order with the same `id`.
Each request is handled on its own goroutine, up to 256 per connection. Beyond that,
reading pauses, which applies TCP back-pressure to the sender. `rid` is an end-to-end
correlation ID: it is copied into forwarded and replicated messages and appears in logs.

A successful socket write means nothing about the remote outcome. Callers wait for the
correlated response or their deadline. If the connection drops, every in-flight call
fails with the outcome **unknown**.

## Message

One envelope type with optional fields:

```json
{
  "id": 17, "op": "SET", "rid": "node1-42", "from": "node2",
  "key": "user:42", "value": "YWxpY2U=", "ttl_ms": 60000, "cl": "quorum", "fwd": true,
  "entry": {"key": "...", "value": "...", "version": {"t": 1170, "n": "node1"}, "expire_at": 1760000000000, "tombstone": false},
  "entries": [], "digests": [{"k": "...", "v": {"t": 1, "n": "node1"}}], "keys": [],
  "members": [{"id": "node1", "addr": "node1:7000", "admin_addr": "node1:8000", "gen": 1, "inc": 3, "phase": "active"}],
  "dead": [{"id": "node2", "gen": 1, "inc": 3}],
  "target": {"id": "node4", "...": "..."},
  "status": "ok|error", "code": "quorum_failed", "error": "human text",
  "found": true, "version": {"t": 1170, "n": "node1"}, "count": 0, "acks": 2,
  "rf": 3, "vnodes": 128, "routable": ["node1", "node3"]
}
```

## Operations

### Client operations (any node accepts them)

| op | Request fields | Response | Notes |
|---|---|---|---|
| `GET` | `key`, optional `cl` = `one`/`quorum` | `found`, `value`, `version`, `ttl_ms` (remaining) | A miss is `status=ok, found=false` |
| `SET` | `key`, `value`, optional `ttl_ms` | `version`, `acks` | Forwarded to the first routable owner |
| `DELETE` | `key` | `version`, `acks` | Writes a tombstone |
| `MEMBERS` | – | `members`, `routable`, `rf`, `vnodes` | Used by clients to build the ring |
| `PING` | – | `from` | |

### Internal operations (node to node)

| op | Purpose |
|---|---|
| `REPLICATE` | Apply one versioned entry (`entry`); the ack is `ok` whether or not it was newer |
| `READ_LOCAL` | Return the local entry, including tombstones, for quorum reads |
| `GOSSIP` | Heartbeat carrying the sender's full `members` table and its `dead` marks; the reply carries the receiver's |
| `SYNC_REQUEST` | Joining node `target` asks a peer to push every key it owns |
| `SYNC_DIGEST` | `digests` offered; the reply lists the `keys` needed |
| `SYNC_PUSH` | Full `entries`; the reply has the `count` applied |

## Limits and validation

| Limit | Default | Error code |
|---|---|---|
| key length 1..`SHC_MAX_KEY_BYTES` | 512 B | `bad_request` (empty) / `too_large` |
| value length ≤ `SHC_MAX_VALUE_BYTES` | 1 MiB | `too_large` |
| entry must fit one store shard (memory budget ÷ 16) | 4 MiB | `too_large` |
| `ttl_ms` ≥ 0 | | `bad_request` |
| frame ≤ `SHC_MAX_FRAME_BYTES` | 4 MiB | connection closed |

## Error codes

| Code | Meaning | Retry? |
|---|---|---|
| `bad_request` | Invalid input | no |
| `too_large` | Key, value or entry over a limit | no |
| `unavailable` | No routable owner, or fewer than W routable owners (nothing applied) | yes, after a delay |
| `quorum_failed` | Write applied on fewer than W owners; outcome unknown | at your discretion (at-least-once) |
| `minority` | The node sees ≤ half of the ring | yes, on another node |
| `not_owner` | A forwarded request reached a node that does not own the key in its view | yes, after refreshing |
| `unknown_op`, `internal`, `shutting_down` | | |

## HTTP admin API

| Method & path | Description |
|---|---|
| `GET /healthz` | 200 while the process runs |
| `GET /readyz` | 200 only when `active` and not in a minority; 503 otherwise, with the reason |
| `GET /metrics` | Prometheus text format |
| `GET /cluster` | Members with phase, liveness, routability and last-seen; ring; ownership fractions; quorum settings |
| `GET /debug/ring?key=K` | Owners, routable owners and future owners of K |
| `GET /admin/status` | Recovery and rebalance progress, store statistics |
| `POST /admin/rebalance` | Run an anti-entropy pass now (synchronous) |
| `POST /admin/remove?id=X` | Remove member X from the ring (operator replacement) |
| `POST /admin/leave` | Hand off data, leave the ring, stop the process |
| `GET/POST /admin/faults` | Only with `SHC_ENABLE_FAULT_INJECTION=true`. Body: `{"block":["node2" or "*"],"unblock":[],"delay_ms":{"node2":200},"reset":true}` |
| `GET /debug/pprof/` | Only with `SHC_ENABLE_PPROF=true` |
| `GET/PUT/DELETE /v1/kv/{key}` | Convenience gateway. `PUT` body = value, `?ttl=30s`; `GET ?consistency=quorum` |

The admin port has **no authentication**. Keep it on a private network (see
[DEPLOYMENT](DEPLOYMENT.md)).
