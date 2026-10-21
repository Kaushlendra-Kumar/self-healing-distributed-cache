package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestFromEnv(t *testing.T) {
	c, err := FromEnv(env(map[string]string{
		"SHC_NODE_ID":                "node-1",
		"SHC_SEEDS":                  "a:7000, b:7000,,",
		"SHC_REPLICATION_FACTOR":     "3",
		"SHC_HEARTBEAT_INTERVAL":     "100ms",
		"SHC_SUSPECT_AFTER":          "300ms",
		"SHC_DEAD_AFTER":             "1s",
		"SHC_EVICTION":               "lfu",
		"SHC_ENABLE_FAULT_INJECTION": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Seeds) != 2 || c.Seeds[1] != "b:7000" || c.HeartbeatInterval != 100*time.Millisecond || !c.EnableFaults || c.Eviction != "lfu" {
		t.Fatalf("%+v", c)
	}
	if c.WriteQuorumEffective() != 2 || c.ReadQuorum() != 2 {
		t.Fatal("quorum defaults")
	}
}

func TestValidation(t *testing.T) {
	_, err := FromEnv(env(map[string]string{
		"SHC_REPLICATION_FACTOR": "2",
		"SHC_WRITE_QUORUM":       "3",
		"SHC_READ_CONSISTENCY":   "all",
		"SHC_DEAD_AFTER":         "1ms",
		"SHC_VNODES":             "x",
	}))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"SHC_VNODES"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
	c := Default()
	c.ReplicationFactor = 2
	c.WriteQuorum = 3
	c.ReadConsistency = "all"
	err = c.Validate()
	for _, want := range []string{"SHC_NODE_ID", "SHC_WRITE_QUORUM", "SHC_READ_CONSISTENCY"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
}
