# Operations guide

## Configuration

Every option is an environment variable. A node refuses to start with an invalid
combination and prints every problem it found. `cachenode -print-config` shows the
effective values.

| Variable | Default | Notes |
|---|---|---|
| `SHC_NODE_ID` | – (required) | Stable, unique; no spaces, `#` or `,` |
| `SHC_LISTEN_ADDR` | `:7000` | Data plane (clients and peers) |
| `SHC_ADVERTISE_ADDR` | listener address | What peers and clients dial; set it to a resolvable DNS name/IP in containers |
| `SHC_ADMIN_ADDR` / `SHC_ADMIN_ADVERTISE_ADDR` | `:8000` | HTTP admin; empty disables |
| `SHC_SEEDS` | – | Comma-separated data addresses of some members (including yourself is fine) |
| `SHC_REPLICATION_FACTOR` | 3 | N |
| `SHC_WRITE_QUORUM` | 0 (= ⌊N/2⌋+1) | W, 1..N |
| `SHC_READ_CONSISTENCY` | `one` | Default for requests without `cl` |
| `SHC_VNODES` | 128 | Must be equal on every node and client |
| `SHC_MAX_MEMORY_BYTES` | 64 MiB | Store budget (approximate); leave ~40% headroom in the container |
| `SHC_MAX_ENTRIES` | 0 (unlimited) | |
| `SHC_EVICTION` | `lru` | or `lfu` |
| `SHC_TOMBSTONE_TTL` | 60s | How long deletes are remembered; keep it longer than any partition you expect to heal from |
| `SHC_SWEEP_INTERVAL` | 1s | Active expiration; 0 = lazy only |
| `SHC_MAX_KEY_BYTES` / `SHC_MAX_VALUE_BYTES` / `SHC_MAX_FRAME_BYTES` | 512 / 1 MiB / 4 MiB | Frame must exceed key + value + 1 KiB |
| `SHC_HEARTBEAT_INTERVAL` / `SHC_SUSPECT_AFTER` / `SHC_DEAD_AFTER` | 500ms / 1.5s / 4s | Must be strictly increasing |
| `SHC_REMOVE_DEAD_AFTER` | 30s | 0 disables automatic removal (then an operator must remove members) |
| `SHC_ANTI_ENTROPY_INTERVAL` | 30s | Periodic full repair pass; 0 = only on ring changes |
| `SHC_SYNC_RATE_KEYS` / `SHC_SYNC_BATCH` | 20000 / 256 | Migration throttle and batch size |
| `SHC_JOIN_TIMEOUT` | 30s | After this, a joining node goes active even if some sources never answered |
| `SHC_REQUEST_TIMEOUT` | 2s | Per replication or forward call |
| `SHC_REJECT_WHEN_MINORITY` | true | Refuse requests when seeing ≤ half the ring |
| `SHC_LEAVE_ON_SHUTDOWN` | false | SIGTERM performs a graceful leave (handoff) first |
| `SHC_ENABLE_FAULT_INJECTION` | false | Enables `/admin/faults`; never on an exposed deployment |
| `SHC_ENABLE_PPROF` | false | Enables `/debug/pprof/` |
| `SHC_LOG_LEVEL` / `SHC_LOG_FORMAT` | `info` / `json` | `debug` logs every request with its `rid` |

## Observability

**Logs** are structured (JSON by default) and carry `node`. Membership transitions,
removals, recovery and rebalance passes are logged at `info`/`warn`. Per-request logs
(`debug`) include `op`, `rid` (correlation ID, propagated across forward and replicate),
`result` and `dur_us`.

**Metrics.** No metric uses keys or request IDs as labels.

| Question | Metric |
|---|---|
| Is the node serving? | `shc_node_ready`, `shc_node_in_minority`, `shc_node_phase{phase}` |
| Request rate, errors, latency | `shc_requests_total{op,result}`, `shc_request_duration_seconds{op}` |
| Hit rate | `shc_store_hits_total / (hits + misses)` |
| Replication health / lag | `shc_replication_total{result}`, `shc_replication_inflight`, `shc_read_repairs_total` |
| Failure detection | `shc_peer_liveness{peer}` (0/1/2), `shc_heartbeats_total{result}`, `shc_membership_transitions_total{kind,to}` |
| Membership changes | `shc_ring_members`, `shc_ring_changes_total`, `shc_member_removals_total{trigger}` |
| Recovery / rebalancing | `shc_recoveries_total{result}`, `shc_rebalance_runs_total`, `shc_sync_keys_pushed_total`, `shc_sync_keys_received_total`, `shc_sync_errors_total`, `shc_rebalance_dropped_keys_total` |
| Memory | `shc_store_bytes`, `shc_store_max_bytes`, `shc_store_entries`, `shc_store_tombstones`, `shc_store_evictions_total`, `shc_store_expirations_total` |
| Balance | `shc_key_ownership_ratio{member}` |

Suggested alerts:

- `shc_node_ready == 0` for 1 minute.
- Any `shc_peer_liveness == 2`.
- `rate(shc_membership_transitions_total{to="dead"}[10m]) > 3` (flapping).
- `increase(shc_requests_total{result=~"quorum_failed|unavailable|minority"}[5m]) > 0`.
- `shc_store_bytes / shc_store_max_bytes > 0.9` together with a rising eviction rate.
- `increase(shc_sync_errors_total[10m]) > 0`.

## Runbooks

### A node stops responding

1. `curl nodeX:8000/healthz`. If it fails, the process or host is down: check the
   container/host and its logs.
2. From a healthy node, `curl node1:8000/cluster`, and look at that member's `liveness`,
   `last_seen_ago_ms` and `marked_dead`.
3. If it is `dead`, the cluster is already routing around it. Writes need W routable
   owners. With N=3 and one dead node, everything keeps working.
4. Restart it. It will show `phase=joining` and then `active`. Watch
   `/admin/status → recovery`.
5. If it will never return, remove it now instead of waiting 30 s:
   `curl -X POST node1:8000/admin/remove?id=nodeX`.

### Cache misses increase

1. Check `shc_store_evictions_total`. A rising rate means memory pressure: raise
   `SHC_MAX_MEMORY_BYTES` or add nodes.
2. Check `shc_store_expirations_total`. TTLs may be shorter than the access pattern needs.
3. Check `shc_member_removals_total` and `shc_recoveries_total`. A node that restarted
   without its peers holding copies, or removals after a multi-node loss, means data was
   genuinely lost.
4. Check `shc_node_ready` on every node: clients may be hitting a node that just
   restarted.

### Uneven key distribution

1. Look at `GET /cluster → ownership` or `shc_key_ownership_ratio`. With 128 vnodes, each
   node should own about 1/n ± a few %.
2. If ownership is even but load is not, the problem is hot keys, not placement. Find
   them in application metrics; this cache doesn't split hot keys.
3. Raise `SHC_VNODES` (on **every** node and client at once) for smoother ownership.

### Replication lag

1. `shc_replication_inflight` growing, or `shc_replication_total{result="error"}` rising,
   points to a slow or partitioned replica.
2. Inspect `shc_request_duration_seconds` on peers and `shc_peer_liveness`.
3. Lag is repaired automatically: anti-entropy runs every `SHC_ANTI_ENTROPY_INTERVAL`.
   Force a pass with `POST /admin/rebalance`.
4. Clients that need the newest value can use `?consistency=quorum`.

### Repeated node flapping

1. `shc_membership_transitions_total{kind="liveness",to="dead"}` keeps rising for one peer.
2. Typical causes: CPU starvation, long GC pauses (check container CPU limits), or a
   lossy network.
3. Each flap forces a resync of that node, which is safe but costly. Either fix the
   cause or raise `SHC_SUSPECT_AFTER`/`SHC_DEAD_AFTER`. If it persists, remove the node.

### Failed rebalancing

1. `/admin/status → rebalance.last_errors` and `shc_sync_errors_total` show the failing
   peer and the error.
2. Common causes: the peer is down (the pass retries next interval), or `too_large` (a
   peer has a smaller memory budget than the sender's entry).
3. Rebalancing is idempotent. After fixing the cause, run `POST /admin/rebalance` on each
   node.
4. A joining node stuck in `running` will go active after `SHC_JOIN_TIMEOUT` with
   `state: incomplete`.

### Memory exhaustion (OOM-killed container)

1. The store budget is approximate and excludes Go runtime overhead. Keep
   `SHC_MAX_MEMORY_BYTES` ≤ 50–60% of the container limit.
2. Very large values fragment the per-shard budget (total ÷ 16). Lower
   `SHC_MAX_VALUE_BYTES`.
3. An OOM-killed node restarts empty and resyncs from its peers. That's safe, but expect
   a burst of sync traffic.

### Network partition

1. Minority nodes report `/readyz` 503 with `in_minority: true` and return `minority` to
   clients. Majority nodes show the minority members as `dead`.
2. Do **not** remove minority members by hand during a short partition. The majority
   removes them after `SHC_REMOVE_DEAD_AFTER` if they stay unreachable.
3. After healing, minority nodes resync automatically: `/admin/status → recovery.reason`
   is `healed from minority partition` or `marked dead by …`.
4. Writes accepted on both sides (only possible within the detection window, or with
   minority refusal disabled) are reconciled by last-writer-wins. One of them is lost.

### Data loss after all replicas disappear

1. This is expected behaviour for an in-memory cache. Reads return a miss
   (`found=false`), not an error.
2. Confirm it from the logs: removals of every owner of the range (`removed member after
   prolonged failure`).
3. The application must repopulate from its source of truth (cache-aside). Raise
   `SHC_REPLICATION_FACTOR`, or spread nodes over more failure domains, to make
   simultaneous loss less likely.

## Day-2 procedures

- **Rolling restart.** Restart one node at a time and wait for `/readyz` (and `/cluster`
  showing it `routable`) before the next. Each restart costs one resync of that node's
  ranges.
- **Scale out.** Start a node with a new ID and the existing seeds. It joins, syncs,
  becomes active, and the others hand off and drop what they no longer own.
- **Scale in.** `POST /admin/leave` on the node to remove, or SIGTERM it with
  `SHC_LEAVE_ON_SHUTDOWN=true`.
- **Change N, W or vnodes.** These must agree across the cluster. Restarting with a
  different value creates a mixed configuration; there is no online reconfiguration in
  this version.
