#!/usr/bin/env bash
# Run one cache node per VM with Docker (host networking).
#
#   NODE_ID=node1 ADVERTISE=10.0.0.11 \
#   SEEDS=10.0.0.11:7000,10.0.0.12:7000,10.0.0.13:7000 \
#   IMAGE=ghcr.io/<you>/self-healing-cache:<tag> ./run-node.sh
#
# Keep ports 7000/8000 closed to the internet: there is no TLS or auth yet.
set -euo pipefail
: "${NODE_ID:?set NODE_ID}" "${ADVERTISE:?set ADVERTISE (this VM's private IP)}" "${SEEDS:?set SEEDS}"
IMAGE=${IMAGE:-self-healing-cache:local}
docker rm -f cache >/dev/null 2>&1 || true
docker run -d --name cache --restart unless-stopped --network host \
  --memory 512m \
  -e SHC_NODE_ID="$NODE_ID" \
  -e SHC_LISTEN_ADDR=:7000 -e SHC_ADMIN_ADDR=:8000 \
  -e SHC_ADVERTISE_ADDR="$ADVERTISE:7000" -e SHC_ADMIN_ADVERTISE_ADDR="$ADVERTISE:8000" \
  -e SHC_SEEDS="$SEEDS" \
  -e SHC_MAX_MEMORY_BYTES=268435456 \
  -e SHC_ENABLE_FAULT_INJECTION=false \
  "$IMAGE"
echo "started $NODE_ID; check: curl -s $ADVERTISE:8000/readyz"
