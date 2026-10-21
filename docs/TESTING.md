# Testing

```bash
go test -race -count=1 ./...                         # everything below except e2e
SHC_E2E=1 go test -count=1 -v ./test/e2e/            # real processes
go test -run='^$' -fuzz=FuzzReadFrame -fuzztime=30s ./internal/protocol/
./scripts/demo.sh                                    # Docker Compose walkthrough
```

## Principles

- **Assert outcomes and invariants, not just API responses.** For example, check that the
  restarted node's own store holds the newest values, that a deleted key is absent on
  every node after anti-entropy, that the total copy count equals keys × RF, and that
  replicas converge to the same version.
- **Use real code paths.** In-process cluster tests run real nodes with their own TCP
  listeners, so forwarding, replication, gossip and sync all cross real sockets. Faults
  are injected at the transport layer (partitions, delays). Crashes are simulated by
  `Kill()`, which drops all state with no goodbye. The e2e test uses real `SIGKILL`.
- **No arbitrary sleeps.** Tests wait for conditions with `eventually(timeout, cond)`,
  using short protocol timeouts (heartbeat 40 ms, dead after 400 ms).
- **Run the race detector everywhere.** Every package runs under `-race` in CI. The cluster
  suite was also run 5× with `-race` and 10× without, with no failures, while developing.

## Inventory

### Unit tests

| Area | Tests |
|---|---|
| Store | set/get/delete; tombstones block stale resurrection; idempotent and out-of-order apply; lazy + sampled TTL; tombstone expiry; LRU and LFU victim order; memory bound and `ErrTooLarge`; compare-and-delete; concurrent access under `-race` with the sweeper running (both policies) |
| Version / HLC | ordering and tie-break; monotonic with frozen clock; `Observe`; wall clock jumping backwards |
| Ring | determinism across instances and input order; owners distinct and bounded (property-based, `testing/quick`); empty ring; **adding a 5th node moves 19.9% of keys vs 80.0% for modulo hashing**; removing a node moves only its keys and keeps survivor order; **virtual nodes cut key-count std-dev from 64% (1 vnode) to 9.7% (256 vnodes)**; ownership fractions sum to 1; hash spread |
| Membership | record precedence (generation, incarnation, terminal `left`); alive → suspect → dead; dead mark blocks routing until recovery; self marked dead / removed; rejoin; removal; stale gossip cannot resurrect a removed member |
| Protocol | round trip under one-byte reads; oversized, zero, truncated, wrong-version frames; malformed JSON; oversized write; fuzzing |
| Transport | 200 concurrent multiplexed calls; slow call times out without blocking others; dropped connection fails in-flight calls and reconnects; malformed payload keeps the connection, oversized frame closes it; fault injection (outbound block, inbound drop, delay) |
| Metrics, config | exposition format; env parsing and validation |
| Coordinator | `collectAcks` counts buffered acks (regression for the quorum race below) |

### In-process cluster and fault-injection tests (`internal/node`)

| Scenario | Test | Verifies |
|---|---|---|
| 3-node startup, routing | `TestThreeNodeRoutingAndReplication` | Any node answers any key; every node ends with every key (RF=3) |
| Replica placement | `TestReplicaPlacementMatchesRing` | With 5 nodes and RF=2, each key is stored on exactly its 2 ring owners |
| Validation | `TestValidationAndErrors` | Error codes for bad and oversized input, unknown op; a miss is not an error |
| Duplicate / out-of-order replication | `TestDuplicateAndOutOfOrderReplication` | Newest wins; HLC advances past observed versions |
| Read repair | `TestQuorumReadRepairsStaleReplica` | A replica that lost a key is repaired by a quorum read |
| Abrupt crash + empty restart | `TestNodeCrashFailoverAndRecovery` | Reads and writes continue; the restarted node holds the writes it missed |
| Partition, stale return, delete during partition | `TestPartitionedNodeRejectsRequestsAndResyncsOnHeal` | Minority refuses; majority writes; heal resyncs; no delete resurrection; majority not disrupted |
| Loss of all copies | `TestLossOfAllReplicasIsAMiss` | Explicit miss after both owners die; key writable again |
| Restore RF | `TestAutoRemovalRestoresReplicationFactor` | After removal, every key is on its new owners |
| Join under write load | `TestJoinRebalancesWithoutLosingConcurrentWrites` | No acknowledged write lost; copies exactly on owners after handoff |
| Graceful leave | `TestGracefulLeaveHandsOffData` | Every key survives the leaver's exit |
| Concurrent writes, same key | `TestConcurrentWritesToSameKeyConverge` | All replicas converge to one version and value |
| Slow replica | `TestSlowReplicaDoesNotBlockQuorumWrite` | The write finishes without the slow replica; anti-entropy repairs it |
| No quorum | `TestWriteFailsWithoutQuorum` | Refused before applying anything |
| TTL across replicas | `TestTTLIsAbsoluteAcrossReplicas` | Same absolute expiry everywhere; sync does not extend it; expires everywhere |
| Flapping | `TestFlappingNodeIsNotRoutedUntilResynced` | Never routed while dead-marked; transitions counted |
| Migration interruption | `TestJoinSurvivesSourceCrashMidSync` | A source dies mid-sync; the joiner still converges |
| Healthy cluster under load | `TestHealthyClusterNeverReportsQuorumFailure` | 24,000 concurrent writes, zero errors |
| Admin HTTP | `TestAdminHTTP` | Health, readiness, metrics, KV gateway, faults, status, leave |

### Process-level end-to-end (`test/e2e`)

`TestThreeProcessCrashAndRecovery` builds the binary and starts three OS processes. It
writes 50 keys, then `SIGKILL`s node-2 and checks that the `/cluster` API reports it
dead. It reads every key and writes half of them while node-2 is down. It then restarts
node-2, waits for `/readyz`, and checks that `/admin/status` shows a complete recovery
with all keys, and that quorum reads return the newest values. Finally it checks that
`SIGTERM` exits cleanly.

### Demo (`scripts/demo.sh`)

The demo was run against Docker Compose. Its output is in [DEMO.md](DEMO.md).

## Bugs found by testing

1. **Spurious `quorum_failed` on a healthy cluster** (~1 in 50k writes, found by the
   benchmark). The ack loop selected on a context that is cancelled as soon as the last
   replication call returns. Go's `select` then picked randomly between buffered acks
   and the closed `Done` channel. Fixed by waiting only on results (each is
   timeout-bounded). There is a deterministic test of the extracted function, plus a
   load test.
2. **The isolated node's dead marks forced healthy majority nodes to resync** (found by the
   partition test). Marks made while a node is in the minority are now never gossiped,
   and are cleared on heal.
3. **Concurrent map access in the recovery loop** (found by `-race`).
4. **First writes after cluster start failed with `unavailable`** (found by the benchmark
   prefill). Per-node readiness was green while peers still saw the node as joining for
   up to one heartbeat. Recovery now gossips the `active` phase immediately, and the
   scripts wait for cluster-wide convergence.

## Not tested (known gaps)

- Asymmetric partitions (A→B works, B→A doesn't).
- A join or removal *during* a partition; several simultaneous membership changes.
- Clock skew between nodes: all tests share one host clock. The HLC and LWW behaviour
  under skew is argued in [CONSISTENCY_MODEL](CONSISTENCY_MODEL.md) but not exercised.
- Real multi-host networks (packet loss, reordering, MTU issues); only loopback and the
  Docker bridge.
- Memory pressure across a full cluster (eviction is unit-tested per store, not
  cluster-wide).
- Long-running soak tests and clusters larger than 5 nodes.
- Linearizability checking with a history checker (e.g. Porcupine/Jepsen). The system
  does not claim linearizability, but such a checker would quantify stale reads.
