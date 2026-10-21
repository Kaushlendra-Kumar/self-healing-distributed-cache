#!/usr/bin/env bash
# End-to-end demonstration against the Docker Compose cluster:
# start 3 nodes, write, verify replication, crash a node, observe detection,
# read from survivors, rejoin, observe recovery, verify consistency, and
# finally simulate a network partition with fault injection.
#
#   ./scripts/demo.sh            # builds and starts the cluster
#   NO_BUILD=1 ./scripts/demo.sh # reuse an existing image
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
run() { printf '$ %s\n' "$*"; eval "$@"; echo; }
pretty() { if command -v jq >/dev/null; then jq "${1:-.}"; else cat; fi; }
wait_ready() {
  for _ in $(seq 1 60); do
    curl -fs "localhost:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "node on admin port $1 not ready" >&2; exit 1
}
wait_converged() { # every node sees every member as routable
  for _ in $(seq 1 60); do
    local ok=1
    for p in 8001 8002 8003; do
      curl -s "localhost:$p/cluster" | tr -d ' \n' | grep -q '"routable":false' && ok=0
    done
    [[ $ok == 1 ]] && return 0
    sleep 1
  done
  echo "cluster did not converge" >&2; exit 1
}
recovery_runs() { curl -s "localhost:$1/admin/status" | tr -d ' \n' | grep -o '"runs":[0-9]*' | head -1 | cut -d: -f2; }
liveness_of() { # admin_port member -> alive|suspect|dead
  curl -s "localhost:$1/cluster" | tr -d ' \n' | grep -o "\"id\":\"$2\"[^}]*\"liveness\":\"[a-z]*\"" | sed 's/.*"liveness":"\([a-z]*\)"/\1/'
}

bold "1. Start three nodes"
if [[ -z "${NO_BUILD:-}" ]]; then run docker compose up -d --build; else run docker compose up -d; fi
for p in 8001 8002 8003; do wait_ready $p; done
wait_converged
run "curl -s localhost:8001/cluster | pretty '{ring, replication_factor, write_quorum, members: [.members[] | {id, phase, liveness, routable}]}'"

bold "2. Write a key through node1 and read it through node3"
run "curl -s -X PUT localhost:8001/v1/kv/user:42 -d alice | pretty '{status, acks, version}'"
run "curl -s localhost:8003/v1/kv/user:42 | pretty"

bold "3. Verify replication: which nodes own the key, and each node's entry count"
run "curl -s 'localhost:8001/debug/ring?key=user:42' | pretty"
for p in 8001 8002 8003; do run "curl -s localhost:$p/admin/status | pretty '{phase, entries: .store.Entries}'"; done

bold "4. Crash node2 with SIGKILL (no graceful leave)"
run docker compose kill -s SIGKILL node2

bold "5. Observe failure detection (suspect after 1.5s, dead after 4s)"
for _ in $(seq 1 30); do
  l=$(liveness_of 8001 node2); echo "node1 sees node2 as: ${l:-?}"
  [[ "$l" == "dead" ]] && break; sleep 1
done
run "curl -s localhost:8001/metrics | grep -E '^shc_peer_liveness|^shc_membership_transitions_total'"

bold "6. Read from the surviving replicas, and keep writing (W=2 of RF=3 still possible)"
run "curl -s localhost:8001/v1/kv/user:42 | pretty '{value, version}'"
run "curl -s -X PUT localhost:8003/v1/kv/user:42 -d alice-v2 | pretty '{status, acks}'"

bold "7. Restart node2: a new generation with an empty memory"
run docker compose start node2
wait_ready 8002
wait_converged

bold "8. Observe recovery: node2 synchronized before becoming ready"
run "curl -s localhost:8002/admin/status | pretty '{phase, recovery, entries: .store.Entries}'"

bold "9. Verify consistency: a quorum read through node2 returns the write made while it was down"
run "curl -s 'localhost:8002/v1/kv/user:42?consistency=quorum' | pretty '{value, version}'"

bold "10. Partition node3 from the others with fault injection"
run "curl -s -X POST localhost:8003/admin/faults -d '{\"block\":[\"*\"]}' >/dev/null"
for p in 8001 8002; do curl -s -X POST localhost:$p/admin/faults -d '{"block":["node3"]}' >/dev/null; done
for _ in $(seq 1 30); do [[ "$(liveness_of 8001 node3)" == "dead" ]] && break; sleep 1; done
# node3 needs its own DeadAfter to conclude it is in the minority. Until then it
# may still answer R=1 reads from its local copy (see docs/CONSISTENCY_MODEL.md).
for _ in $(seq 1 30); do curl -fs localhost:8003/readyz >/dev/null 2>&1 || break; sleep 1; done
echo "node3 now knows it is isolated: it refuses requests instead of serving possibly stale data"
run "curl -s localhost:8003/v1/kv/user:42 | pretty '{status, code, error}'"
run "curl -s -X PUT localhost:8001/v1/kv/user:42 -d alice-v3 | pretty '{status, acks}'"
echo "healing the partition"
runs_before=$(recovery_runs 8003)
for p in 8001 8002 8003; do curl -s -X POST localhost:$p/admin/faults -d '{"reset":true}' >/dev/null; done
for _ in $(seq 1 30); do [[ "$(recovery_runs 8003)" -gt "$runs_before" ]] && break; sleep 1; done
wait_ready 8003
wait_converged
run "curl -s localhost:8003/admin/status | pretty '{phase, recovery: {state: .recovery.state, reason: .recovery.reason, runs: .recovery.runs}}'"
run "curl -s 'localhost:8003/v1/kv/user:42?consistency=quorum' | pretty '{value}'"

bold "Done. Tear down with: docker compose down"
