// Package e2e runs the real cachenode binary as separate OS processes and
// exercises the demonstration scenario: start three nodes, write, kill one
// with SIGKILL, keep serving, restart it and verify it resynchronized.
//
// It is skipped unless SHC_E2E=1 because it builds a binary and binds ports.
//
//	SHC_E2E=1 go test ./test/e2e/ -v
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/pkg/client"
)

type proc struct {
	id, data, admin string
	cmd             *exec.Cmd
	log             *os.File
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (p *proc) start(t *testing.T, bin string, seeds []string) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"SHC_NODE_ID="+p.id,
		"SHC_LISTEN_ADDR="+p.data,
		"SHC_ADMIN_ADDR="+p.admin,
		"SHC_SEEDS="+strings.Join(seeds, ","),
		"SHC_HEARTBEAT_INTERVAL=100ms",
		"SHC_SUSPECT_AFTER=300ms",
		"SHC_DEAD_AFTER=1s",
		"SHC_REMOVE_DEAD_AFTER=0",
		"SHC_LOG_FORMAT=text",
	)
	cmd.Stdout, cmd.Stderr = p.log, p.log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.cmd = cmd
}

func getJSON(url string, v any) (int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(v)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestThreeProcessCrashAndRecovery(t *testing.T) {
	if os.Getenv("SHC_E2E") == "" {
		t.Skip("set SHC_E2E=1 to run process-level tests")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "cachenode")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/cachenode")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	procs := make([]*proc, 3)
	var seeds []string
	for i := range procs {
		f, _ := os.Create(filepath.Join(dir, fmt.Sprintf("node-%d.log", i+1)))
		procs[i] = &proc{id: fmt.Sprintf("node-%d", i+1), data: freePort(t), admin: freePort(t), log: f}
		seeds = append(seeds, procs[i].data)
	}
	t.Cleanup(func() {
		for _, p := range procs {
			if p.cmd != nil && p.cmd.ProcessState == nil {
				_ = p.cmd.Process.Kill()
				_ = p.cmd.Wait()
			}
		}
		if t.Failed() {
			for _, p := range procs {
				b, _ := os.ReadFile(p.log.Name())
				t.Logf("---- %s log ----\n%s", p.id, b)
			}
		}
	})
	for _, p := range procs {
		p.start(t, bin, seeds)
	}
	ready := func(p *proc) bool {
		var body map[string]any
		code, err := getJSON("http://"+p.admin+"/readyz", &body)
		return err == nil && code == 200
	}
	eventually(t, 20*time.Second, "all nodes ready", func() bool {
		return ready(procs[0]) && ready(procs[1]) && ready(procs[2])
	})

	ctx := context.Background()
	cl, err := client.New(ctx, client.Config{Seeds: seeds, RefreshInterval: 200 * time.Millisecond, Attempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < 50; i++ {
		if _, err := cl.Set(ctx, fmt.Sprint("demo:", i), []byte("v1"), 0); err != nil {
			t.Fatal(err)
		}
	}

	// Crash node-2 abruptly.
	victim := procs[1]
	if err := victim.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = victim.cmd.Wait()
	eventually(t, 10*time.Second, "node-1 detects node-2 dead", func() bool {
		var cluster struct {
			Members []struct {
				ID       string `json:"id"`
				Liveness string `json:"liveness"`
			} `json:"members"`
		}
		if _, err := getJSON("http://"+procs[0].admin+"/cluster", &cluster); err != nil {
			return false
		}
		for _, m := range cluster.Members {
			if m.ID == "node-2" {
				return m.Liveness == "dead"
			}
		}
		return false
	})
	// Surviving replicas keep serving reads and accept writes.
	for i := 0; i < 50; i++ {
		r, err := cl.Get(ctx, fmt.Sprint("demo:", i))
		if err != nil || !r.Found || string(r.Value) != "v1" {
			t.Fatalf("read during failure: %+v %v", r, err)
		}
		if i%2 == 0 {
			if _, err := cl.Set(ctx, fmt.Sprint("demo:", i), []byte("v2"), 0); err != nil {
				t.Fatalf("write during failure: %v", err)
			}
		}
	}

	// Restart node-2 with an empty memory; it must resynchronize.
	victim.start(t, bin, seeds)
	eventually(t, 20*time.Second, "node-2 ready again", func() bool { return ready(victim) })
	var status struct {
		Recovery struct {
			State        string `json:"state"`
			KeysReceived int    `json:"keys_received"`
		} `json:"recovery"`
		Store struct {
			Entries int `json:"Entries"`
		} `json:"store"`
	}
	if _, err := getJSON("http://"+victim.admin+"/admin/status", &status); err != nil {
		t.Fatal(err)
	}
	if status.Recovery.State != "complete" || status.Store.Entries < 50 {
		t.Fatalf("node-2 did not resynchronize: %+v", status)
	}
	for i := 0; i < 50; i++ {
		want := "v1"
		if i%2 == 0 {
			want = "v2"
		}
		r, err := cl.GetQuorum(ctx, fmt.Sprint("demo:", i))
		if err != nil || string(r.Value) != want {
			t.Fatalf("demo:%d = %q (%v), want %q", i, r.Value, err, want)
		}
	}

	// Graceful shutdown on SIGTERM exits cleanly.
	for _, p := range procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range procs {
		if err := p.cmd.Wait(); err != nil {
			t.Errorf("%s exited with %v", p.id, err)
		}
	}
}
