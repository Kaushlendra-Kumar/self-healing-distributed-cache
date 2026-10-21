#!/usr/bin/env bash
# Reproducible benchmark matrix against a local 3-process cluster.
# Results are appended as JSON lines to $OUT (default bench-results.jsonl).
#
#   DURATION=5s ./scripts/bench.sh
set -euo pipefail
cd "$(dirname "$0")/.."
DURATION=${DURATION:-5s}
OUT=${OUT:-bench-results.jsonl}
KEYS=${KEYS:-10000}
: >"$OUT"

bench() { # label, cachebench args...
  local label=$1; shift
  bin/cachebench -seeds 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -d "$DURATION" -keys "$KEYS" \
    -label "$label" -out "$OUT" "$@"
}

run_matrix() {
  local tag=$1
  for c in 1 8 32; do bench "$tag get c=$c" -op get -c "$c"; done
  for c in 1 8 32; do bench "$tag set c=$c" -op set -c "$c"; done
  bench "$tag delete c=8" -op delete -c 8
  bench "$tag mixed90/10 c=32" -op mixed -reads 0.9 -c 32
  bench "$tag set 1KiB c=16" -op set -c 16 -size 1024
  bench "$tag get 1KiB c=16" -op get -c 16 -size 1024
}

go build -o bin/ ./cmd/...
trap './scripts/local-cluster.sh stop' EXIT
echo "== RF=3, W=2 (default)"
SHC_LOG_LEVEL=warn ./scripts/local-cluster.sh start 3 >/dev/null
run_matrix "rf3"
./scripts/local-cluster.sh stop; sleep 1

echo "== RF=1, W=1 (no replication)"
SHC_REPLICATION_FACTOR=1 SHC_WRITE_QUORUM=1 SHC_LOG_LEVEL=warn ./scripts/local-cluster.sh start 3 >/dev/null
run_matrix "rf1"
echo "results written to $OUT"
