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
| Membership | removal votes need a fresh majority, the suspect cannot vote; record precedence (generation, incarnation, terminal `left`); alive → suspect → dead; dead mark blocks routing until recovery; self marked dead / removed; rejoin; removal; stale gossip cannot resurrect a removed member |
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
| Non-transitive partition | `TestNonTransitivePartitionDoesNotRemoveMembers` | Node-1 ↮ node-3 while both reach node-2: no removals, writes still succeed |
| Clock skew, sequential writes | `TestClockSkewDoesNotReorderSequentialWrites` | A coordinator 10 s fast writes v1, dies, and a correct-clock coordinator's v2 still wins everywhere (fails if HLC observation is disabled; verified) |
| Clock skew, TTL | `TestClockSkewShortensTTLOnFastReplica` | Pins the documented limitation: a replica 10 s ahead drops a 5 s TTL value as expired |
| Ack implies stored | `TestCoordinatorVersionExceedsLocalCopy` | A future version already in the store cannot make an acknowledged write disappear |
| Join during a partition | `TestJoinDuringPartition` | A node joins while another is isolated: the joiner goes active on the majority, the isolated node refuses requests, and after the heal no acknowledged write is lost, placement is exact and the healed node holds the newest values |
| Concurrent membership changes | `TestConcurrentMembershipChangesUnderLoad` | Two joins and one crash + removal at once under writes: no acknowledged write lost, exact placement |

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

5. **A broken link between two nodes caused mutual removal** (found by the non-transitive
   partition test). Node-1 and node-3 could not reach each other but both reached node-2.
   Each saw a majority, so each auto-removed the other, both rejoined, and the cycle
   repeated. Removal now needs corroboration: gossip carries each node's list of
   peers it sees as dead, and a member is removed only when a strict majority of the
   ring reports it dead.
6. **A write could be acknowledged and silently discarded** (found while verifying the
   clock-skew test). Quorum-read repair could store a peer's version locally without
   advancing the local clock. A later write coordinated by that node could then get a
   lower version, which the store rejected, while the client was told "ok". Now every
   path that receives a version advances the clock, and the coordinator advances past
   its local copy before issuing a version. A test injects a future version directly.
7. **A flawed test oracle** (found by the concurrent membership test). The join test
   treated a write that returned an error as "not applied". An error means *unknown
   outcome*. The tests now accept the last acknowledged value or any later
   unacknowledged one; any older value fails.

## Not tested (known gaps)

- True one-way packet loss at the network layer. Fault injection cuts links between
  node pairs, which covers non-transitive partitions (A↮C, both reach B). A one-way
  network fault breaks TCP in both directions, so it reduces to the same case.
- Real multi-host networks (packet loss, reordering, MTU issues); only loopback and the
  Docker bridge.
- Long-running soak tests and clusters larger than 6 nodes.
- Linearizability checking with a history checker (e.g. Porcupine/Jepsen). The system
  does not claim linearizability, but such a checker would quantify stale reads.
