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

func TestDashboard(t *testing.T) {
	c := newCluster(t, 3, func(cfg *config.Config) { cfg.AdminAddr = "127.0.0.1:0" })
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
	type overview struct {
		ServedBy string `json:"served_by"`
		Nodes    []struct {
			ID      string `json:"id"`
			Error   string `json:"error"`
			Cluster struct {
				InMinority bool `json:"in_minority"`
			} `json:"cluster"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"nodes"`
	}
	getOverview := func() overview {
		t.Helper()
		code, body := do("GET", "/ui/overview", "")
		var o overview
		if code != 200 || json.Unmarshal([]byte(body), &o) != nil {
			t.Fatal("overview", code, body)
		}
		return o
	}

	if code, body := do("GET", "/", ""); code != 200 || !strings.Contains(body, "<title>Cache Cluster Dashboard</title>") {
		t.Fatal("dashboard page", code)
	}
	if code, _ := do("GET", "/nope", ""); code != 404 {
		t.Fatal("unknown paths must not serve the dashboard", code)
	}

	o := getOverview()
	if o.ServedBy != "node-1" || len(o.Nodes) != 3 {
		t.Fatalf("overview: %+v", o)
	}
	for _, n := range o.Nodes {
		if n.Error != "" || n.Status.Phase != "active" || n.Cluster.InMinority {
			t.Fatalf("node %s not healthy in overview: %+v", n.ID, n)
		}
	}

	// Partitioning node-3 through node-1's proxy must reach node-3 itself.
	if code, body := do("POST", "/ui/faults?node=node-3", `{"block":["*"]}`); code != 200 || !strings.Contains(body, `"*"`) {
		t.Fatal("proxy faults", code, body)
	}
	eventually(t, 10*time.Second, "node-3 reports minority", func() bool {
		for _, n := range getOverview().Nodes {
			if n.ID == "node-3" {
				return n.Cluster.InMinority
			}
		}
		return false
	})
	if code, _ := do("POST", "/ui/faults?node=node-3", `{"reset":true}`); code != 200 {
		t.Fatal("heal", code)
	}
	c.waitConverged()

	if code, _ := do("POST", "/ui/faults?node=ghost", `{}`); code != 400 {
		t.Fatal("unknown node must be rejected", code)
	}
}
