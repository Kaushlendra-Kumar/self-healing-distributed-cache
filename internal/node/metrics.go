package node

import (
	"sync/atomic"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/metrics"
)

// nodeMetrics holds every metric a node exports. Labels are limited to
// bounded sets: operation names, result codes, peer IDs and state names.
type nodeMetrics struct {
	reg *metrics.Registry

	requests         *metrics.CounterVec
	latency          *metrics.HistogramVec
	forwarded        *metrics.CounterVec
	replication      *metrics.CounterVec
	replPending      atomic.Int64
	readRepairs      *metrics.CounterVec
	heartbeats       *metrics.CounterVec
	transitions      *metrics.CounterVec
	removals         *metrics.CounterVec
	ringChanges      *metrics.CounterVec
	recoveries       *metrics.CounterVec
	rebalances       *metrics.CounterVec
	rebalanceDropped *metrics.CounterVec
	syncDigests      *metrics.CounterVec
	syncPushed       *metrics.CounterVec
	syncReceived     *metrics.CounterVec
	syncErrors       *metrics.CounterVec
}

func newNodeMetrics(n *Node) *nodeMetrics {
	r := metrics.NewRegistry()
	m := &nodeMetrics{
		reg:              r,
		requests:         r.NewCounterVec("shc_requests_total", "Client requests handled by this node, by operation and result.", "op", "result"),
		latency:          r.NewHistogramVec("shc_request_duration_seconds", "Client request latency at this node (including forwarding and replication).", metrics.DefaultLatencyBuckets, "op"),
		forwarded:        r.NewCounterVec("shc_forwarded_total", "Client requests forwarded to another owner.", "op"),
		replication:      r.NewCounterVec("shc_replication_total", "Replication messages sent by this node as coordinator, by result.", "result"),
		readRepairs:      r.NewCounterVec("shc_read_repairs_total", "Stale replicas repaired by quorum reads."),
		heartbeats:       r.NewCounterVec("shc_heartbeats_total", "Heartbeats sent to known peers, by result.", "result"),
		transitions:      r.NewCounterVec("shc_membership_transitions_total", "Observed membership transitions, by kind (liveness, phase) and new state.", "kind", "to"),
		removals:         r.NewCounterVec("shc_member_removals_total", "Members removed from the ring, by trigger.", "trigger"),
		ringChanges:      r.NewCounterVec("shc_ring_changes_total", "Ownership ring rebuilds."),
		recoveries:       r.NewCounterVec("shc_recoveries_total", "Join/recovery synchronizations, by outcome.", "result"),
		rebalances:       r.NewCounterVec("shc_rebalance_runs_total", "Rebalance / anti-entropy passes."),
		rebalanceDropped: r.NewCounterVec("shc_rebalance_dropped_keys_total", "Local copies dropped after handoff to new owners."),
		syncDigests:      r.NewCounterVec("shc_sync_digests_sent_total", "Key digests offered to peers during synchronization."),
		syncPushed:       r.NewCounterVec("shc_sync_keys_pushed_total", "Entries pushed to peers during synchronization."),
		syncReceived:     r.NewCounterVec("shc_sync_keys_received_total", "Entries received and applied from synchronization."),
		syncErrors:       r.NewCounterVec("shc_sync_errors_total", "Failed synchronization batches."),
	}
	r.NewGaugeFunc("shc_replication_inflight", "Replication messages currently awaiting acknowledgement (a lag indicator).", func() float64 {
		return float64(m.replPending.Load())
	})
	r.NewGaugeFunc("shc_store_entries", "Entries in the local store, including tombstones.", func() float64 { return float64(n.store.Stats().Entries) })
	r.NewGaugeFunc("shc_store_tombstones", "Delete markers in the local store.", func() float64 { return float64(n.store.Stats().Tombstones) })
	r.NewGaugeFunc("shc_store_bytes", "Approximate bytes used by the local store.", func() float64 { return float64(n.store.Stats().Bytes) })
	r.NewGaugeFunc("shc_store_max_bytes", "Configured memory budget.", func() float64 { return float64(n.cfg.MaxMemoryBytes) })
	r.NewCounterFunc("shc_store_hits_total", "Local reads that found a live value.", func() uint64 { return n.store.Stats().Hits })
	r.NewCounterFunc("shc_store_misses_total", "Local reads that found nothing, a tombstone or an expired value.", func() uint64 { return n.store.Stats().Misses })
	r.NewCounterFunc("shc_store_evictions_total", "Entries evicted under memory or entry pressure.", func() uint64 { return n.store.Stats().Evictions })
	r.NewCounterFunc("shc_store_expirations_total", "Entries removed because their TTL elapsed.", func() uint64 { return n.store.Stats().Expirations })
	r.NewGaugeFunc("shc_node_ready", "1 if this node is active and sees a majority of the ring.", func() float64 { return b2f(n.Ready()) })
	r.NewGaugeFunc("shc_node_in_minority", "1 if this node sees at most half of the ring.", func() float64 { return b2f(n.inMinority()) })
	r.NewGaugeFunc("shc_ring_members", "Members in the ownership ring.", func() float64 { return float64(n.ring.Load().Len()) })
	r.NewGaugeVecFunc("shc_peer_liveness", "Local failure-detector opinion of each peer: 0 alive, 1 suspect, 2 dead.", "peer", func() map[string]float64 {
		out := map[string]float64{}
		for _, p := range n.members.Peers() {
			out[p.ID] = float64(n.members.Liveness(p.ID))
		}
		return out
	})
	r.NewGaugeVecFunc("shc_key_ownership_ratio", "Fraction of the hash space for which each member is primary.", "member", func() map[string]float64 {
		return n.ring.Load().OwnershipFractions()
	})
	r.NewGaugeVecFunc("shc_node_phase", "1 for this node's current lifecycle phase.", "phase", func() map[string]float64 {
		cur := n.members.Self().Phase
		out := map[string]float64{}
		for _, p := range []membership.Phase{membership.PhaseJoining, membership.PhaseActive, membership.PhaseLeaving, membership.PhaseLeft} {
			out[string(p)] = b2f(p == cur)
		}
		return out
	})
	return m
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
