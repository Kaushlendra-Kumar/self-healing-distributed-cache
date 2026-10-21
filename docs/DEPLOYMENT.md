# Deployment guide

Nothing here has been provisioned. This guide describes how to run the cluster beyond
one machine, what has to change compared with Docker Compose, and which hosting options
fit a portfolio project at little or no cost.

## Local vs. real deployments

A Docker Compose network is **not** equivalent to a multi-host deployment:

| Aspect | Compose on one host | Real deployment |
|---|---|---|
| Failure domains | One host, kernel and disk: a host failure kills every node | Nodes on separate hosts / zones |
| Network | Bridge on one kernel; no loss, reordering or asymmetric partitions | Real latency, loss, MTU, firewalls, NAT |
| Clocks | One clock | Independent clocks; NTP is **required** (HLC versions and TTL expiry depend on wall time) |
| Identity / discovery | Compose DNS names | Stable private IPs or DNS names per node |
| Security | Isolated bridge | Must restrict ports and add TLS + authentication before any shared network |

## What changes for a real network

1. **Advertised addresses.** Set `SHC_ADVERTISE_ADDR=<private-ip-or-dns>:7000` and
   `SHC_ADMIN_ADVERTISE_ADDR`. Peers and clients dial exactly what is advertised.
2. **Seeds.** List two or three stable members in `SHC_SEEDS`. Any reachable seed is
   enough to join.
3. **Stable identity.** Keep `SHC_NODE_ID` stable per machine or slot. A restart with the
   same ID keeps its ring position, so only its own ranges resync. On Kubernetes, a
   StatefulSet with a headless Service gives `cache-0`, `cache-1`, … with stable DNS names.
4. **Clock sync.** Run chrony/NTP. Clock skew reorders concurrent writes and shifts TTL
   expiry (see [CONSISTENCY_MODEL](CONSISTENCY_MODEL.md#clock-skew-limitation)).
5. **Timeouts.** Across availability zones, add the inter-zone RTT (typically 1–2 ms) to
   expectations. The default 500 ms / 1.5 s / 4 s detection timings are generous; keep
   `SHC_REQUEST_TIMEOUT` well above p99 RTT.
6. **Failure domains.** Put each node in a different zone or fault domain, and use
   N ≤ number of domains. With 3 nodes in 3 zones and N=3, each key has a copy in every
   zone, so a whole zone can fail.

## Security

Before anything other than a private demo:

- Firewall or security groups: port 7000 open only to cluster nodes and application
  hosts; port 8000 only to monitoring and operators; nothing public.
- There is no TLS or authentication yet. Run inside a private VPC/VCN, a WireGuard or
  Tailscale mesh, or a service mesh that provides mTLS.
- Keep `SHC_ENABLE_FAULT_INJECTION=false` and `SHC_ENABLE_PPROF=false`.
- Secrets: none are needed today. When TLS or authentication is added, keep keys in the
  platform's secret manager (OCI Vault, GCP Secret Manager, AWS Secrets Manager, or
  Kubernetes Secrets), never in the image or `.env`.

## Resource sizing

- **Memory:** container limit ≈ `SHC_MAX_MEMORY_BYTES` / 0.55, because of Go runtime and
  GC headroom. Usable cache capacity ≈ (nodes × budget) / N.
- **CPU:** in the benchmarks, 4 vCPUs shared by 3 nodes plus the load generator handled
  about 50k mixed ops/s (see [BENCHMARKS](BENCHMARKS.md)). One vCPU per node is plenty
  for a demo.
- **Network:** a replicated write sends the value N−1 extra times. Rebalancing is capped
  by `SHC_SYNC_RATE_KEYS`.

## Logging and metrics

- Logs are JSON on stdout. Use the platform's collector (Docker logging driver, Cloud
  Logging, OCI Logging) or Grafana Alloy → Loki.
- Metrics: Prometheus scraping `:8000/metrics` on every node (see
  `deploy/prometheus.yml`), or Grafana Alloy remote-writing to Grafana Cloud's free tier.
- Alerts: see [OPERATIONS → Observability](OPERATIONS.md#observability).

## Deploy and roll back

1. Build and push an immutable image tag (e.g. `ghcr.io/<you>/self-healing-cache:<git-sha>`).
2. **Roll one node at a time.** Stop the node, start it on the new tag, and wait for
   `/readyz` 200 and for `/cluster` on a peer to show it `routable`. Then do the next.
   Each restart costs one resync of that node's ranges.
3. **Roll back** the same way with the previous tag. The wire protocol is versioned
   (frame version byte), so mixed versions during a roll are possible as long as the
   version byte is unchanged.

`deploy/vm/run-node.sh` is a minimal script for running one node per VM with Docker.

## Hosting options for a portfolio deployment

Free-tier terms change often. **Check each provider's current terms** before signing up.
The notes below reflect my understanding as of 2025–2026.

| Option | Cost | Fits this project? | Notes |
|---|---|---|---|
| **GitHub Codespaces** | Free monthly allowance for personal accounts | ✅ Best for a **demo** | Runs `docker compose up` and `scripts/demo.sh` in the browser; forwarded ports can be shared. One host, so not real failure domains |
| **Oracle Cloud Always Free** | Free (card verification required) | ✅ Best for a **free multi-host cluster** | Arm (Ampere A1) capacity can be split into several small VMs in one VCN, e.g. 3 × (1 OCPU, 6 GB). Place them in different fault domains. Arm capacity is sometimes unavailable in popular regions, and idle Always-Free VMs can be reclaimed |
| **Google Cloud** free trial credits (+ one always-free e2-micro) | Free while credits last | ✅ Best for **real zones** | 3 small VMs in 3 zones of one region demonstrates zone failure. Set a budget alert and delete the VMs afterwards |
| **AWS / Azure** new-account credits or free plans | Free while credits last | ✅ | Same pattern as GCP: 3 small instances across availability zones in one VPC |
| Fly.io | Small pay-as-you-go cost (no general free tier for new orgs) | ✅ technically | Private networking between machines supports raw TCP; multi-region is easy |
| Render, Railway, Koyeb free tiers | Free or trial | ❌ | Built for HTTP web services. Instances sleep or are limited to one, and private raw-TCP networking between instances is limited or paid |
| Play with Docker / Killercoda | Free, ephemeral (hours) | ⚠️ throwaway demos only | Nothing persists |
| GitHub Container Registry | Free for public images | ✅ image hosting | Push from CI |
| Grafana Cloud free tier | Free | ✅ metrics/logs | Limited series and retention |

**Recommendation:**

1. **Codespaces** for the portfolio demo: zero cost, reproducible, and recruiters can run
   it themselves.
2. **Oracle Cloud Always Free** for a real three-VM cluster with real `kill -9` and
   network-partition demos (`iptables` rules on a VM give genuine partitions).
3. **GCP trial credits** if you want to show zone-level failure domains, then tear the
   VMs down.

### Sketch: three VMs (Oracle Cloud or GCP)

1. Create a private network (VCN/VPC) with one subnet. The firewall allows TCP 7000 and
   8000 from the subnet CIDR only, plus SSH from your IP.
2. Create 3 VMs (Ubuntu, 1 vCPU, ≥ 1 GB RAM) in different fault domains or zones.
   Install Docker and enable chrony.
3. On each VM, run
   `NODE_ID=node1 ADVERTISE=10.0.0.11 SEEDS=10.0.0.11:7000,10.0.0.12:7000,10.0.0.13:7000 deploy/vm/run-node.sh`
   with that VM's values.
4. From your laptop, use an SSH tunnel: `ssh -L 8001:10.0.0.11:8000 vm1`, then
   `curl localhost:8001/cluster`.
5. Failure demos:
   - crash: `sudo docker kill -s KILL cache` on a VM;
   - partition: `sudo iptables -A INPUT -s 10.0.0.12 -j DROP` (and the reverse on the
     other VM); heal with `iptables -F`.
6. Cost control: on trial credits, set a budget alert and delete the VMs when done.

Kubernetes is **not** required for any of this. It adds scheduling, service discovery and
rolling updates, which are useful later. But the cache's own correctness (membership,
recovery, rebalancing) has to work without an orchestrator first, and a StatefulSet is
a straightforward follow-up.
