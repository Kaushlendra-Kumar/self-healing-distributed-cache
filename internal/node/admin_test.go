package node

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
)

func TestAdminHTTP(t *testing.T) {
	c := newCluster(t, 1, func(cfg *config.Config) {
		cfg.ReplicationFactor = 1
		cfg.AdminAddr = "127.0.0.1:0"
	})
	base := "http://" + c.nodes["node-1"].AdminAddr()
	do := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := do("GET", "/healthz", ""); code != 200 {
		t.Fatal("healthz", code)
	}
	if code, body := do("GET", "/readyz", ""); code != 200 {
		t.Fatal("readyz", code, body)
	}
	if code, body := do("PUT", "/v1/kv/greeting?ttl=1m", "hello"); code != 200 {
		t.Fatal("put", code, body)
	}
	code, body := do("GET", "/v1/kv/greeting", "")
	var got map[string]any
	_ = json.Unmarshal([]byte(body), &got)
	if code != 200 || got["value"] != "hello" || got["ttl_ms"].(float64) <= 0 {
		t.Fatal("get", code, body)
	}
	if code, _ := do("DELETE", "/v1/kv/greeting", ""); code != 200 {
		t.Fatal("delete", code)
	}
	if code, _ := do("GET", "/v1/kv/greeting", ""); code != 404 {
		t.Fatal("expected 404 after delete", code)
	}
	if code, _ := do("PUT", "/v1/kv/x?ttl=bogus", "v"); code != 400 {
		t.Fatal("expected 400 for bad ttl", code)
	}
	_, metrics := do("GET", "/metrics", "")
	for _, want := range []string{`shc_requests_total{op="SET",result="ok"} 1`, "shc_store_entries", "shc_node_ready 1", "shc_request_duration_seconds_bucket"} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if code, body := do("GET", "/cluster", ""); code != 200 || !strings.Contains(body, `"replication_factor": 1`) {
		t.Fatal("cluster", code, body)
	}
	if code, body := do("GET", "/debug/ring?key=abc", ""); code != 200 || !strings.Contains(body, "node-1") {
		t.Fatal("ring", code, body)
	}
	if code, body := do("POST", "/admin/faults", `{"delay_ms":{"*":5}}`); code != 200 || !strings.Contains(body, `"*": 5`) {
		t.Fatal("faults", code, body)
	}
	if code, _ := do("POST", "/admin/rebalance", ""); code != 200 {
		t.Fatal("rebalance", code)
	}
	if code, _ := do("POST", "/admin/remove?id=node-1", ""); code != 400 {
		t.Fatal("removing self must be rejected", code)
	}
	if code, body := do("GET", "/admin/status", ""); code != 200 || !strings.Contains(body, `"state": "complete"`) {
		t.Fatal("status", code, body)
	}
	if code, _ := do("POST", "/admin/leave", ""); code != 202 {
		t.Fatal("leave", code)
	}
	select {
	case <-c.nodes["node-1"].Done():
	case <-time.After(5 * time.Second):
		t.Fatal("node did not stop after leave")
	}
}
