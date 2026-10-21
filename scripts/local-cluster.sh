#!/usr/bin/env bash
# Run a cluster of cachenode processes on localhost without Docker.
#
#   ./scripts/local-cluster.sh start [N]   # nodes on 7001.. (data) and 8001.. (admin)
#   ./scripts/local-cluster.sh stop
#   ./scripts/local-cluster.sh kill <i>    # SIGKILL node i
#   ./scripts/local-cluster.sh restart <i>
#
# Extra SHC_* variables in the environment are passed through to every node.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${RUN_DIR:-.run}
mkdir -p "$RUN" bin
N_FILE="$RUN/size"

build() { go build -o bin/ ./cmd/...; }

start_node() {
  local i=$1 n=$2 seeds=""
  for j in $(seq 1 "$n"); do seeds+="127.0.0.1:$((7000 + j)),"; done
  SHC_NODE_ID="node$i" SHC_LISTEN_ADDR="127.0.0.1:$((7000 + i))" SHC_ADMIN_ADDR="127.0.0.1:$((8000 + i))" \
    SHC_SEEDS="${seeds%,}" SHC_LOG_FORMAT="${SHC_LOG_FORMAT:-text}" \
    nohup bin/cachenode >"$RUN/node$i.log" 2>&1 &
  echo $! >"$RUN/node$i.pid"
}

wait_ready() {
  for _ in $(seq 1 100); do
    curl -fs "127.0.0.1:$((8000 + $1))/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "node$1 not ready; see $RUN/node$1.log" >&2; return 1
}

case "${1:-}" in
  start)
    n=${2:-3}; echo "$n" >"$N_FILE"; build
    for i in $(seq 1 "$n"); do start_node "$i" "$n"; done
    for i in $(seq 1 "$n"); do wait_ready "$i"; done
    echo "started $n nodes: data 127.0.0.1:7001-$((7000 + n)), admin 127.0.0.1:8001-$((8000 + n)); logs in $RUN/"
    ;;
  stop)
    for f in "$RUN"/node*.pid; do [[ -f $f ]] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    ;;
  kill)
    kill -9 "$(cat "$RUN/node$2.pid")"; rm -f "$RUN/node$2.pid"
    ;;
  restart)
    start_node "$2" "$(cat "$N_FILE")"; wait_ready "$2"
    ;;
  *)
    echo "usage: $0 start [N] | stop | kill <i> | restart <i>" >&2; exit 2
    ;;
esac
