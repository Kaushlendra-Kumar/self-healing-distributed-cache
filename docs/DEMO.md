# Demo transcript

Actual output of `./scripts/demo.sh` against the Docker Compose cluster (3 nodes, RF=3,
W=2, default timeouts), recorded on 2026-10-21 on a 4-vCPU Linux VM. HLC version numbers
and timestamps vary from run to run.

The script waits on real conditions (readiness, liveness, recovery count), not fixed sleeps.
Step 10 waits until node3 itself detects the minority: during the first ~4 s of a
partition, an isolated node can still answer `one` reads from its local copy (see
[CONSISTENCY_MODEL](CONSISTENCY_MODEL.md#staleness-windows-where-one-reads-can-be-stale)).

```text

== 1. Start three nodes
$ docker compose up -d
 Container self-healing-distributed-cache-node3-1 Creating 
 Container self-healing-distributed-cache-node2-1 Creating 
 Container self-healing-distributed-cache-node1-1 Creating 
 Container self-healing-distributed-cache-node3-1 Created 
 Container self-healing-distributed-cache-node2-1 Created 
 Container self-healing-distributed-cache-node1-1 Created 
 Container self-healing-distributed-cache-node3-1 Starting 
 Container self-healing-distributed-cache-node1-1 Starting 
 Container self-healing-distributed-cache-node2-1 Starting 
 Container self-healing-distributed-cache-node3-1 Started 
 Container self-healing-distributed-cache-node1-1 Started 
 Container self-healing-distributed-cache-node2-1 Started 

$ curl -s localhost:8001/cluster | pretty '{ring, replication_factor, write_quorum, members: [.members[] | {id, phase, liveness, routable}]}'
{
  "ring": [
    "node1",
    "node2",
    "node3"
  ],
  "replication_factor": 3,
  "write_quorum": 2,
  "members": [
    {
      "id": "node1",
      "phase": "active",
      "liveness": "alive",
      "routable": true
    },
    {
      "id": "node2",
      "phase": "active",
      "liveness": "alive",
      "routable": true
    },
    {
      "id": "node3",
      "phase": "active",
      "liveness": "alive",
      "routable": true
    }
  ]
}


== 2. Write a key through node1 and read it through node3
$ curl -s -X PUT localhost:8001/v1/kv/user:42 -d alice | pretty '{status, acks, version}'
{
  "status": "ok",
  "acks": 2,
  "version": {
    "t": 117478357727182848,
    "n": "node1"
  }
}

$ curl -s localhost:8003/v1/kv/user:42 | pretty
{
  "found": true,
  "key": "user:42",
  "ttl_ms": 0,
  "value": "alice",
  "version": {
    "t": 117478357727182848,
    "n": "node1"
  }
}


== 3. Verify replication: which nodes own the key, and each node's entry count
$ curl -s 'localhost:8001/debug/ring?key=user:42' | pretty
{
  "extras": null,
  "key": "user:42",
  "owners": [
    "node1",
    "node3",
    "node2"
  ],
  "routable": [
    "node1",
    "node3",
    "node2"
  ]
}

$ curl -s localhost:8001/admin/status | pretty '{phase, entries: .store.Entries}'
{
  "phase": "active",
  "entries": 1
}

$ curl -s localhost:8002/admin/status | pretty '{phase, entries: .store.Entries}'
{
  "phase": "active",
  "entries": 1
}

$ curl -s localhost:8003/admin/status | pretty '{phase, entries: .store.Entries}'
{
  "phase": "active",
  "entries": 1
}


== 4. Crash node2 with SIGKILL (no graceful leave)
$ docker compose kill -s SIGKILL node2
 Container self-healing-distributed-cache-node2-1 Killing 
 Container self-healing-distributed-cache-node2-1 Killed 


== 5. Observe failure detection (suspect after 1.5s, dead after 4s)
node1 sees node2 as: alive
node1 sees node2 as: alive
node1 sees node2 as: suspect
node1 sees node2 as: suspect
node1 sees node2 as: dead
$ curl -s localhost:8001/metrics | grep -E '^shc_peer_liveness|^shc_membership_transitions_total'
shc_membership_transitions_total{kind="liveness",to="dead"} 1
shc_membership_transitions_total{kind="liveness",to="suspect"} 1
shc_membership_transitions_total{kind="phase",to="active"} 2
shc_membership_transitions_total{kind="phase",to="joining"} 2
shc_peer_liveness{peer="node2"} 2
shc_peer_liveness{peer="node3"} 0


== 6. Read from the surviving replicas, and keep writing (W=2 of RF=3 still possible)
$ curl -s localhost:8001/v1/kv/user:42 | pretty '{value, version}'
{
  "value": "alice",
  "version": {
    "t": 117478357727182848,
    "n": "node1"
  }
}

$ curl -s -X PUT localhost:8003/v1/kv/user:42 -d alice-v2 | pretty '{status, acks}'
{
  "status": "ok",
  "acks": 2
}


== 7. Restart node2: a new generation with an empty memory
$ docker compose start node2
 Container self-healing-distributed-cache-node2-1 Starting 
 Container self-healing-distributed-cache-node2-1 Started 


== 8. Observe recovery: node2 synchronized before becoming ready
$ curl -s localhost:8002/admin/status | pretty '{phase, recovery, entries: .store.Entries}'
{
  "phase": "active",
  "recovery": {
    "state": "complete",
    "reason": "startup",
    "runs": 1,
    "started": "2026-10-21T10:11:24.618071181Z",
    "finished": "2026-10-21T10:11:25.62038693Z",
    "sources": [
      "node1",
      "node3"
    ],
    "keys_received": 2
  },
  "entries": 1
}


== 9. Verify consistency: a quorum read through node2 returns the write made while it was down
$ curl -s 'localhost:8002/v1/kv/user:42?consistency=quorum' | pretty '{value, version}'
{
  "value": "alice-v2",
  "version": {
    "t": 117478358015213568,
    "n": "node1"
  }
}


== 10. Partition node3 from the others with fault injection
$ curl -s -X POST localhost:8003/admin/faults -d '{"block":["*"]}' >/dev/null

node3 now knows it is isolated: it refuses requests instead of serving possibly stale data
$ curl -s localhost:8003/v1/kv/user:42 | pretty '{status, code, error}'
{
  "status": "error",
  "code": "minority",
  "error": "node sees 1 of 3 ring members; refusing requests in a minority partition"
}

$ curl -s -X PUT localhost:8001/v1/kv/user:42 -d alice-v3 | pretty '{status, acks}'
{
  "status": "ok",
  "acks": 2
}

healing the partition
$ curl -s localhost:8003/admin/status | pretty '{phase, recovery: {state: .recovery.state, reason: .recovery.reason, runs: .recovery.runs}}'
{
  "phase": "active",
  "recovery": {
    "state": "complete",
    "reason": "marked dead by node1",
    "runs": 2
  }
}

$ curl -s 'localhost:8003/v1/kv/user:42?consistency=quorum' | pretty '{value}'
{
  "value": "alice-v3"
}


== Done. Tear down with: docker compose down
```
