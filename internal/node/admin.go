package node

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
)

// adminHandler serves the HTTP operations surface:
//
//	GET  /healthz           liveness of the process
//	GET  /readyz            200 only when active and in the majority
//	GET  /metrics           Prometheus text format
//	GET  /cluster           membership, liveness, routability, ownership
//	GET  /debug/ring?key=k  owners of a key
//	GET  /admin/status      recovery and rebalance progress
//	POST /admin/rebalance   run an anti-entropy pass now
//	POST /admin/remove?id=  remove a member (e.g. a permanently failed node)
//	POST /admin/leave       hand off data, leave the ring and shut down
//	GET/POST /admin/faults  fault injection (only if SHC_ENABLE_FAULT_INJECTION)
//	GET  /debug/pprof/      Go profiler (only if SHC_ENABLE_PPROF)
//	GET/PUT/DELETE /v1/kv/{key}  convenience HTTP gateway to the cache
//	GET  /                  live dashboard (see dashboard.go)
func (n *Node) adminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": n.cfg.NodeID})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		self := n.members.Self()
		body := map[string]any{"node": n.cfg.NodeID, "phase": self.Phase, "in_minority": n.inMinority()}
		code := http.StatusOK
		if !n.Ready() {
			code = http.StatusServiceUnavailable
		}
		body["ready"] = code == http.StatusOK
		writeJSON(w, code, body)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		n.m.reg.WritePrometheus(w)
	})
	mux.HandleFunc("GET /cluster", func(w http.ResponseWriter, _ *http.Request) {
		up, total := n.members.ReachableRingCount()
		writeJSON(w, http.StatusOK, map[string]any{
			"self":               n.cfg.NodeID,
			"replication_factor": n.cfg.ReplicationFactor,
			"write_quorum":       n.cfg.WriteQuorumEffective(),
			"read_quorum":        n.cfg.ReadQuorum(),
			"vnodes":             n.cfg.VirtualNodes,
			"ring":               n.ring.Load().Nodes(),
			"reachable":          up,
			"ring_size":          total,
			"in_minority":        n.inMinority(),
			"members":            n.members.Status(),
			"ownership":          n.ring.Load().OwnershipFractions(),
		})
	})
	mux.HandleFunc("GET /debug/ring", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "key query parameter required"})
			return
		}
		p := n.place(key)
		writeJSON(w, http.StatusOK, map[string]any{"key": key, "owners": p.owners, "routable": p.routable, "extras": p.extras})
	})
	mux.HandleFunc("GET /admin/status", func(w http.ResponseWriter, _ *http.Request) {
		rec, rb := n.Status()
		writeJSON(w, http.StatusOK, map[string]any{"phase": n.members.Self().Phase, "recovery": rec, "rebalance": rb, "store": n.store.Stats()})
	})
	mux.HandleFunc("POST /admin/rebalance", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, n.RebalanceNow(r.Context(), "operator request"))
	})
	mux.HandleFunc("POST /admin/remove", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" || id == n.cfg.NodeID {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id must name another member (use /admin/leave for this node)"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"removed": n.RemoveMember(id)})
	})
	mux.HandleFunc("POST /admin/leave", func(w http.ResponseWriter, _ *http.Request) {
		if n.members.Self().Phase != membership.PhaseActive {
			writeJSON(w, http.StatusConflict, map[string]any{"error": "node is not active"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "leaving"})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.JoinTimeout)
			defer cancel()
			if err := n.Leave(ctx); err != nil {
				n.log.Warn("leave", "err", err)
			}
			n.shutdown()
		}()
	})
	mux.HandleFunc("/admin/faults", n.handleFaults)
	if n.cfg.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.HandleFunc("GET /v1/kv/{key}", n.httpGet)
	mux.HandleFunc("PUT /v1/kv/{key}", n.httpPut)
	mux.HandleFunc("DELETE /v1/kv/{key}", n.httpDelete)
	n.registerDashboard(mux)
	return mux
}

// faultRequest is the body accepted by POST /admin/faults.
type faultRequest struct {
	Block   []string         `json:"block"`
	Unblock []string         `json:"unblock"`
	DelayMs map[string]int64 `json:"delay_ms"`
	Reset   bool             `json:"reset"`
}

func (n *Node) handleFaults(w http.ResponseWriter, r *http.Request) {
	if n.faults == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "fault injection disabled (set SHC_ENABLE_FAULT_INJECTION=true)"})
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		var fr faultRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&fr); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if fr.Reset {
			n.faults.Reset()
		}
		n.faults.Block(fr.Block...)
		n.faults.Unblock(fr.Unblock...)
		for p, ms := range fr.DelayMs {
			n.faults.SetDelay(p, time.Duration(ms)*time.Millisecond)
		}
		n.log.Warn("fault injection updated", "faults", n.faults.Snapshot())
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, n.faults.Snapshot())
}

func httpStatus(resp *protocol.Message) int {
	if resp.Status == protocol.StatusOK {
		return http.StatusOK
	}
	switch resp.Code {
	case protocol.CodeBadRequest:
		return http.StatusBadRequest
	case protocol.CodeTooLarge:
		return http.StatusRequestEntityTooLarge
	case protocol.CodeNotOwner:
		return http.StatusMisdirectedRequest
	case protocol.CodeTimeout:
		return http.StatusGatewayTimeout
	case protocol.CodeInternal:
		return http.StatusInternalServerError
	}
	return http.StatusServiceUnavailable
}

func (n *Node) httpGet(w http.ResponseWriter, r *http.Request) {
	resp := n.Do(r.Context(), &protocol.Message{Op: protocol.OpGet, Key: r.PathValue("key"), Consistency: r.URL.Query().Get("consistency")})
	if resp.Status != protocol.StatusOK {
		writeJSON(w, httpStatus(resp), resp)
		return
	}
	if !resp.Found {
		writeJSON(w, http.StatusNotFound, map[string]any{"key": r.PathValue("key"), "found": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": r.PathValue("key"), "found": true, "value": string(resp.Value),
		"version": resp.Version, "ttl_ms": resp.TTLMs})
}

func (n *Node) httpPut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(n.cfg.MaxValueBytes)+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var ttl int64
	if s := r.URL.Query().Get("ttl"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			if ms, err2 := strconv.ParseInt(s, 10, 64); err2 == nil {
				d = time.Duration(ms) * time.Millisecond
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "ttl must be a duration like 30s"})
				return
			}
		}
		ttl = d.Milliseconds()
	}
	resp := n.Do(r.Context(), &protocol.Message{Op: protocol.OpSet, Key: r.PathValue("key"), Value: body, TTLMs: ttl})
	writeJSON(w, httpStatus(resp), resp)
}

func (n *Node) httpDelete(w http.ResponseWriter, r *http.Request) {
	resp := n.Do(r.Context(), &protocol.Message{Op: protocol.OpDelete, Key: r.PathValue("key")})
	writeJSON(w, httpStatus(resp), resp)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
