// Package config loads node configuration from environment variables
// (prefixed SHC_) with validated defaults. Every option is documented in
// docs/OPERATIONS.md and .env.example.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Read consistency levels.
const (
	ReadOne    = "one"    // first routable owner (the primary when healthy)
	ReadQuorum = "quorum" // R = floor(RF/2)+1 owners, newest version wins, read repair
)

// Config holds every tunable of a node.
type Config struct {
	NodeID         string
	ListenAddr     string // data-plane TCP listener
	AdvertiseAddr  string // address peers and clients use to reach ListenAddr
	AdminAddr      string // HTTP admin/metrics listener ("" disables)
	AdminAdvertise string
	Seeds          []string // data-plane addresses of existing members

	ReplicationFactor int
	WriteQuorum       int    // acks required before a write succeeds (0 = majority of RF)
	ReadConsistency   string // default for requests that do not specify one
	VirtualNodes      int

	MaxMemoryBytes int64
	MaxEntries     int
	Eviction       string
	TombstoneTTL   time.Duration
	SweepInterval  time.Duration
	MaxKeyBytes    int
	MaxValueBytes  int
	MaxFrameBytes  int

	HeartbeatInterval   time.Duration
	SuspectAfter        time.Duration
	DeadAfter           time.Duration
	RemoveDeadAfter     time.Duration // 0 disables automatic removal
	AntiEntropyInterval time.Duration
	SyncRateKeys        int // keys/second budget for migration and repair
	SyncBatch           int
	JoinTimeout         time.Duration

	RequestTimeout     time.Duration
	RejectWhenMinority bool
	EnableFaults       bool
	EnablePprof        bool
	LeaveOnShutdown    bool
	LogLevel           string
	LogFormat          string
}

// Default returns the defaults used when no environment variable is set.
func Default() Config {
	return Config{
		ListenAddr:          ":7000",
		AdminAddr:           ":8000",
		ReplicationFactor:   3,
		ReadConsistency:     ReadOne,
		VirtualNodes:        128,
		MaxMemoryBytes:      64 << 20,
		Eviction:            "lru",
		TombstoneTTL:        60 * time.Second,
		SweepInterval:       time.Second,
		MaxKeyBytes:         512,
		MaxValueBytes:       1 << 20,
		MaxFrameBytes:       4 << 20,
		HeartbeatInterval:   500 * time.Millisecond,
		SuspectAfter:        1500 * time.Millisecond,
		DeadAfter:           4 * time.Second,
		RemoveDeadAfter:     30 * time.Second,
		AntiEntropyInterval: 30 * time.Second,
		SyncRateKeys:        20000,
		SyncBatch:           256,
		JoinTimeout:         30 * time.Second,
		RequestTimeout:      2 * time.Second,
		RejectWhenMinority:  true,
		LogLevel:            "info",
		LogFormat:           "json",
	}
}

// WriteQuorumEffective resolves the configured write quorum.
func (c *Config) WriteQuorumEffective() int {
	if c.WriteQuorum > 0 {
		return c.WriteQuorum
	}
	return c.ReplicationFactor/2 + 1
}

// ReadQuorum returns R for quorum reads.
func (c *Config) ReadQuorum() int { return c.ReplicationFactor/2 + 1 }

// Validate checks invariants.
func (c *Config) Validate() error {
	var errs []error
	if c.NodeID == "" {
		errs = append(errs, errors.New("SHC_NODE_ID is required"))
	}
	if strings.ContainsAny(c.NodeID, " #,") {
		errs = append(errs, errors.New("SHC_NODE_ID must not contain spaces, '#' or ','"))
	}
	if c.ReplicationFactor < 1 {
		errs = append(errs, errors.New("SHC_REPLICATION_FACTOR must be >= 1"))
	}
	if w := c.WriteQuorumEffective(); w < 1 || w > c.ReplicationFactor {
		errs = append(errs, fmt.Errorf("SHC_WRITE_QUORUM must be between 1 and the replication factor (%d)", c.ReplicationFactor))
	}
	if c.ReadConsistency != ReadOne && c.ReadConsistency != ReadQuorum {
		errs = append(errs, fmt.Errorf("SHC_READ_CONSISTENCY must be %q or %q", ReadOne, ReadQuorum))
	}
	if c.VirtualNodes < 1 {
		errs = append(errs, errors.New("SHC_VNODES must be >= 1"))
	}
	if c.HeartbeatInterval <= 0 || c.SuspectAfter <= c.HeartbeatInterval || c.DeadAfter <= c.SuspectAfter {
		errs = append(errs, errors.New("require 0 < SHC_HEARTBEAT_INTERVAL < SHC_SUSPECT_AFTER < SHC_DEAD_AFTER"))
	}
	if c.MaxValueBytes+c.MaxKeyBytes+1024 > c.MaxFrameBytes {
		errs = append(errs, errors.New("SHC_MAX_FRAME_BYTES must exceed SHC_MAX_VALUE_BYTES + SHC_MAX_KEY_BYTES + 1KiB"))
	}
	if c.SyncBatch < 1 || c.SyncRateKeys < 1 {
		errs = append(errs, errors.New("SHC_SYNC_BATCH and SHC_SYNC_RATE_KEYS must be >= 1"))
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("SHC_REQUEST_TIMEOUT must be > 0"))
	}
	return errors.Join(errs...)
}

// FromEnv overlays environment variables on Default().
func FromEnv(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	c := Default()
	var errs []error
	str := func(key string, dst *string) {
		if v := getenv(key); v != "" {
			*dst = v
		}
	}
	num := func(key string, dst *int) {
		if v := getenv(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	num64 := func(key string, dst *int64) {
		if v := getenv(key); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	dur := func(key string, dst *time.Duration) {
		if v := getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = d
		}
	}
	boolean := func(key string, dst *bool) {
		if v := getenv(key); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = b
		}
	}
	str("SHC_NODE_ID", &c.NodeID)
	str("SHC_LISTEN_ADDR", &c.ListenAddr)
	str("SHC_ADVERTISE_ADDR", &c.AdvertiseAddr)
	str("SHC_ADMIN_ADDR", &c.AdminAddr)
	str("SHC_ADMIN_ADVERTISE_ADDR", &c.AdminAdvertise)
	if v := getenv("SHC_SEEDS"); v != "" {
		for _, s := range strings.Split(v, ",") {
			if s = strings.TrimSpace(s); s != "" {
				c.Seeds = append(c.Seeds, s)
			}
		}
	}
	num("SHC_REPLICATION_FACTOR", &c.ReplicationFactor)
	num("SHC_WRITE_QUORUM", &c.WriteQuorum)
	str("SHC_READ_CONSISTENCY", &c.ReadConsistency)
	num("SHC_VNODES", &c.VirtualNodes)
	num64("SHC_MAX_MEMORY_BYTES", &c.MaxMemoryBytes)
	num("SHC_MAX_ENTRIES", &c.MaxEntries)
	str("SHC_EVICTION", &c.Eviction)
	dur("SHC_TOMBSTONE_TTL", &c.TombstoneTTL)
	dur("SHC_SWEEP_INTERVAL", &c.SweepInterval)
	num("SHC_MAX_KEY_BYTES", &c.MaxKeyBytes)
	num("SHC_MAX_VALUE_BYTES", &c.MaxValueBytes)
	num("SHC_MAX_FRAME_BYTES", &c.MaxFrameBytes)
	dur("SHC_HEARTBEAT_INTERVAL", &c.HeartbeatInterval)
	dur("SHC_SUSPECT_AFTER", &c.SuspectAfter)
	dur("SHC_DEAD_AFTER", &c.DeadAfter)
	dur("SHC_REMOVE_DEAD_AFTER", &c.RemoveDeadAfter)
	dur("SHC_ANTI_ENTROPY_INTERVAL", &c.AntiEntropyInterval)
	num("SHC_SYNC_RATE_KEYS", &c.SyncRateKeys)
	num("SHC_SYNC_BATCH", &c.SyncBatch)
	dur("SHC_JOIN_TIMEOUT", &c.JoinTimeout)
	dur("SHC_REQUEST_TIMEOUT", &c.RequestTimeout)
	boolean("SHC_REJECT_WHEN_MINORITY", &c.RejectWhenMinority)
	boolean("SHC_ENABLE_FAULT_INJECTION", &c.EnableFaults)
	boolean("SHC_ENABLE_PPROF", &c.EnablePprof)
	boolean("SHC_LEAVE_ON_SHUTDOWN", &c.LeaveOnShutdown)
	str("SHC_LOG_LEVEL", &c.LogLevel)
	str("SHC_LOG_FORMAT", &c.LogFormat)
	if err := errors.Join(errs...); err != nil {
		return c, err
	}
	return c, c.Validate()
}
