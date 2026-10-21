#!/usr/bin/env bash
# Measures, on a local 3-process cluster (RF=3, default timeouts):
#   1. failure detection time: SIGKILL -> peer marks the node dead
#   2. recovery time: restart -> node ready after synchronizing KEYS keys
#   3. join/rebalance time: a 4th node joins and becomes ready
set -euo pipefail
cd "$(dirname "$0")/.."
KEYS=${KEYS:-50000}
now_ms() { date +%s%3N; }
liveness() { curl -s "127.0.0.1:$1/cluster" | tr -d ' \n' | grep -o "\"id\":\"$2\"[^}]*\"liveness\":\"[a-z]*\"" | sed 's/.*"liveness":"\([a-z]*\)"/\1/'; }
field() { curl -s "127.0.0.1:$1/admin/status" | tr -d ' \n' | grep -o "\"$2\":[0-9]*" | head -1 | cut -d: -f2; }

go build -o bin/ ./cmd/...
trap './scripts/local-cluster.sh stop' EXIT
SHC_LOG_LEVEL=warn ./scripts/local-cluster.sh start 3 >/dev/null
echo "prefilling $KEYS keys (128 B values)..."
bin/cachebench -seeds 127.0.0.1:7001 -keys "$KEYS" -op get -d 1s -c 32 >/dev/null

t0=$(now_ms); ./scripts/local-cluster.sh kill 2
while [[ "$(liveness 8001 node2)" != "suspect" ]]; do sleep 0.02; done; t1=$(now_ms)
while [[ "$(liveness 8001 node2)" != "dead" ]]; do sleep 0.02; done; t2=$(now_ms)
echo "failure detection: suspect after $((t1 - t0)) ms, dead after $((t2 - t0)) ms (configured 1500 / 4000 ms)"

t0=$(now_ms); ./scripts/local-cluster.sh restart 2 >/dev/null; t1=$(now_ms)
echo "recovery: node2 restarted empty and was ready after $((t1 - t0)) ms, received $(field 8002 keys_received) entries, holds $(field 8002 Entries)"

echo 4 >.run/size
t0=$(now_ms); ./scripts/local-cluster.sh restart 4 >/dev/null; t1=$(now_ms)
echo "join: node4 ready after $((t1 - t0)) ms, received $(field 8004 keys_received) entries"
for i in 1 2 3 4; do
  for _ in $(seq 1 200); do [[ "$(curl -s 127.0.0.1:$((8000 + i))/admin/status | tr -d ' \n' | grep -o '"running":[a-z]*' | cut -d: -f2)" == "false" ]] && break; sleep 0.1; done
done
sleep 3
total=0; for i in 1 2 3 4; do e=$(field $((8000 + i)) Entries); total=$((total + e)); echo "node$i entries after rebalance: $e"; done
echo "total copies: $total (expected ~ $KEYS x 3 = $((KEYS * 3)))"
