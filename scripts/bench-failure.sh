#!/usr/bin/env bash
# Two experiments on a local 3-process cluster:
#  A. Throughput and errors per second while a node is SIGKILLed and restarted
#     under a 90/10 read/write load.
#  B. Hit ratio when the key space is larger than cluster memory, for LRU vs LFU
#     and uniform vs zipf (skewed) access.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
trap './scripts/local-cluster.sh stop >/dev/null 2>&1 || true' EXIT
SEEDS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003

echo "== A. node failure under load (mixed 90/10, c=16, 10k keys; kill node2 at t=5s, restart at t=12s)"
SHC_LOG_LEVEL=warn ./scripts/local-cluster.sh start 3 >/dev/null
bin/cachebench -seeds "$SEEDS" -op mixed -reads 0.9 -c 16 -d 20s -keys 10000 -timeline -label "mixed under failure" >.run/failure.txt &
bench=$!
sleep 6   # ~1 s prefill + 5 s of measurement
./scripts/local-cluster.sh kill 2
sleep 7
./scripts/local-cluster.sh restart 2 >/dev/null &
wait $bench
cat .run/failure.txt
./scripts/local-cluster.sh stop >/dev/null; sleep 1

echo
echo "== B. hit ratio with 4 MiB per node (~16k entries/node, RF=3) and 100k keys, mixed 90/10, c=16"
for policy in lru lfu; do
  SHC_EVICTION=$policy SHC_MAX_MEMORY_BYTES=4194304 SHC_LOG_LEVEL=warn ./scripts/local-cluster.sh start 3 >/dev/null
  for dist in uniform zipf; do
    bin/cachebench -seeds "$SEEDS" -op mixed -reads 0.9 -c 16 -d 5s -keys 100000 -dist "$dist" -json |
      sed -E "s/.*\"hits\":([0-9]+),\"misses\":([0-9]+),\"ops_per_sec\":([0-9.]+).*/$policy $dist hits=\1 misses=\2 ops_per_sec=\3/" |
      awk '{split($3,h,"="); split($4,m,"="); printf "%-4s %-8s hit ratio=%.3f  %s\n", $1, $2, h[2]/(h[2]+m[2]), $5}'
  done
  curl -s 127.0.0.1:8001/metrics | grep -E '^shc_store_(evictions_total|bytes|entries) ' | tr '\n' ' '; echo
  ./scripts/local-cluster.sh stop >/dev/null; sleep 1
done
