// Command cachenode runs one node of the self-healing distributed cache.
// All configuration comes from SHC_* environment variables; see
// docs/OPERATIONS.md or .env.example.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/config"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/node"
)

func main() {
	printCfg := flag.Bool("print-config", false, "print the effective configuration and exit")
	probe := flag.String("probe", "", "check a local admin endpoint (e.g. /readyz) and exit 0 if it returns 200; for container health checks")
	flag.Parse()
	if *probe != "" {
		os.Exit(runProbe(*probe))
	}

	cfg, err := config.FromEnv(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:\n"+err.Error())
		os.Exit(2)
	}
	if *printCfg {
		fmt.Printf("%+v\n", cfg)
		return
	}
	log := newLogger(cfg)
	n, err := node.New(cfg, node.Options{Logger: log})
	if err != nil {
		log.Error("create node", "err", err)
		os.Exit(1)
	}
	if err := n.Start(); err != nil {
		log.Error("start node", "err", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("shutdown signal received", "signal", s.String(), "leave", cfg.LeaveOnShutdown)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		n.Stop(ctx)
		cancel()
	case <-n.Done():
		// Stopped from the admin API (POST /admin/leave).
	}
}

// runProbe lets minimal container images (no curl or wget) health-check the
// node through its own binary.
func runProbe(path string) int {
	addr := os.Getenv("SHC_ADMIN_ADDR")
	if addr == "" {
		addr = config.Default().AdminAddr
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + addr + path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "status", resp.StatusCode)
		return 1
	}
	return 0
}

func newLogger(cfg config.Config) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
