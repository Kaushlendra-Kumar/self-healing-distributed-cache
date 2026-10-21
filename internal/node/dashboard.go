package node

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// The dashboard is one static page served by every node. Member admin
// addresses are usually only reachable inside the cluster network (for
// example node2:8000 in Docker Compose), so the page never calls other nodes
// itself: the node that served it collects their views server side.
//
//	GET  /                 the dashboard page
//	GET  /ui/overview      every member's /cluster and /admin/status, side by side
//	POST /ui/faults?node=  forward a fault-injection request to that member

//go:embed web/dashboard.html
var dashboardHTML []byte

const dashboardFetchTimeout = time.Second

// nodeView is one member's own report, or the reason it could not be read.
type nodeView struct {
	ID      string          `json:"id"`
	Admin   string          `json:"admin_addr"`
	Cluster json.RawMessage `json:"cluster,omitempty"`
	Status  json.RawMessage `json:"status,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func (n *Node) registerDashboard(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	})
	mux.HandleFunc("GET /ui/overview", n.handleOverview)
	mux.HandleFunc("POST /ui/faults", n.handleProxyFaults)
}

func (n *Node) handleOverview(w http.ResponseWriter, r *http.Request) {
	members := n.members.Status()
	views := make([]nodeView, len(members))
	ctx, cancel := context.WithTimeout(r.Context(), dashboardFetchTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for i, m := range members {
		views[i] = nodeView{ID: m.ID, Admin: m.AdminAddr}
		if m.AdminAddr == "" {
			views[i].Error = "no admin address"
			continue
		}
		wg.Add(1)
		go func(v *nodeView) {
			defer wg.Done()
			var err error
			if v.Cluster, err = adminFetch(ctx, http.MethodGet, v.Admin, "/cluster", nil); err != nil {
				v.Error = err.Error()
				return
			}
			if v.Status, err = adminFetch(ctx, http.MethodGet, v.Admin, "/admin/status", nil); err != nil {
				v.Error = err.Error()
			}
		}(&views[i])
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{"served_by": n.cfg.NodeID, "nodes": views})
}

func (n *Node) handleProxyFaults(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	var admin string
	for _, m := range n.members.Status() {
		if m.ID == id {
			admin = m.AdminAddr
		}
	}
	if admin == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "node must name a member with an admin address"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), dashboardFetchTimeout)
	defer cancel()
	resp, err := adminFetch(ctx, http.MethodPost, admin, "/admin/faults", body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp)
}

// adminFetch calls another member's admin API and returns the body when the
// response is a 2xx, or an error carrying the body otherwise.
func adminFetch(ctx context.Context, method, addr, path string, body []byte) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			return nil, &adminError{e.Error}
		}
		return nil, &adminError{resp.Status}
	}
	return b, nil
}

type adminError struct{ msg string }

func (e *adminError) Error() string { return e.msg }
