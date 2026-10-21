// Package client is a cluster-aware Go client for the cache.
//
// The client fetches the membership view from any node, builds the same
// consistent-hash ring the servers use and sends each request directly to the
// first routable owner of the key, saving the forwarding hop. Its view may be
// stale; that is safe because any node accepts any request and forwards it to
// the owner from its own, fresher view. On connection failures or routing
// errors the client refreshes its view and retries another node.
//
// Retries make writes at-least-once: if a SET times out after the server
// applied it, the retry applies the same value again with a newer version.
// For a cache this is harmless, but it means a timed-out write must be
// treated as "possibly applied".
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/ring"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/transport"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

// Error codes returned in *Error.Code.
const (
	CodeBadRequest   = protocol.CodeBadRequest
	CodeTooLarge     = protocol.CodeTooLarge
	CodeUnavailable  = protocol.CodeUnavailable
	CodeQuorumFailed = protocol.CodeQuorumFailed
	CodeMinority     = protocol.CodeMinority
)

// Error is a structured server error.
type Error = protocol.RemoteError

// ErrNoNodes is returned when no node in the view or seed list answers.
var ErrNoNodes = errors.New("client: no cache node reachable")

// Config configures a Client.
type Config struct {
	Seeds           []string      // data-plane addresses (host:port) of any nodes
	Timeout         time.Duration // per attempt (default 3s)
	RefreshInterval time.Duration // maximum age of the membership view (default 2s)
	Attempts        int           // attempts per operation (default 3)
}

// Client is safe for concurrent use.
type Client struct {
	cfg  Config
	pool *transport.Pool

	mu        sync.RWMutex
	ring      *ring.Ring
	addrs     map[string]string
	routable  map[string]bool
	rf        int
	refreshed time.Time
}

// New connects to the cluster and loads the membership view.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Seeds) == 0 {
		return nil, errors.New("client: at least one seed address is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 2 * time.Second
	}
	if cfg.Attempts <= 0 {
		cfg.Attempts = 3
	}
	c := &Client{cfg: cfg, pool: transport.NewPool(transport.ClientConfig{DefaultTimeout: cfg.Timeout}, nil), ring: &ring.Ring{}}
	if err := c.Refresh(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Close releases connections.
func (c *Client) Close() { c.pool.Close() }

// Refresh reloads the membership view from the first node that answers.
func (c *Client) Refresh(ctx context.Context) error {
	c.mu.RLock()
	var addrs []string
	for id, a := range c.addrs {
		if c.routable[id] {
			addrs = append(addrs, a)
		}
	}
	c.mu.RUnlock()
	addrs = append(addrs, c.cfg.Seeds...)
	var lastErr error = ErrNoNodes
	for _, a := range addrs {
		actx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		resp, err := c.pool.Call(actx, "", a, &protocol.Message{Op: protocol.OpMembers})
		cancel()
		if err == nil {
			err = resp.AsError()
		}
		if err != nil {
			lastErr = err
			continue
		}
		ids := make([]string, 0, len(resp.Members))
		m := make(map[string]string, len(resp.Members))
		for _, mem := range resp.Members {
			ids = append(ids, mem.ID)
			m[mem.ID] = mem.Addr
		}
		rt := make(map[string]bool, len(resp.Routable))
		for _, id := range resp.Routable {
			rt[id] = true
		}
		c.mu.Lock()
		c.ring = ring.New(resp.VNodes, ids)
		c.addrs, c.routable, c.rf, c.refreshed = m, rt, resp.RF, time.Now()
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("refresh membership: %w", lastErr)
}

type target struct{ id, addr string }

// candidates lists routable owners first (the server would pick the same
// coordinator), then every other routable member (which will forward).
func (c *Client) candidates(key string) []target {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []target
	seen := map[string]bool{}
	for _, o := range c.ring.Owners(key, c.rf) {
		if c.routable[o] {
			out = append(out, target{o, c.addrs[o]})
			seen[o] = true
		}
	}
	for id, a := range c.addrs {
		if c.routable[id] && !seen[id] {
			out = append(out, target{id, a})
		}
	}
	return out
}

func retryable(code string) bool {
	return code == protocol.CodeNotOwner || code == protocol.CodeMinority || code == protocol.CodeUnavailable
}

func (c *Client) do(ctx context.Context, req *protocol.Message) (*protocol.Message, error) {
	c.mu.RLock()
	stale := time.Since(c.refreshed) > c.cfg.RefreshInterval
	c.mu.RUnlock()
	if stale {
		_ = c.Refresh(ctx) // best effort; an old view still works through forwarding
	}
	var lastErr error = ErrNoNodes
	for attempt := 0; attempt < c.cfg.Attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
			_ = c.Refresh(ctx)
		}
		for _, t := range c.candidates(req.Key) {
			actx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
			resp, err := c.pool.Call(actx, t.id, t.addr, req)
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if rerr := resp.AsError(); rerr != nil {
				lastErr = rerr
				if retryable(resp.Code) {
					continue
				}
				return resp, rerr
			}
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// GetResult is the outcome of a read.
type GetResult struct {
	Value   []byte
	Found   bool
	Version version.Version
	TTL     time.Duration // remaining lifetime; 0 = no expiry
}

// Get reads key from its primary (consistency "one").
func (c *Client) Get(ctx context.Context, key string) (GetResult, error) {
	return c.get(ctx, key, "one")
}

// GetQuorum reads key from a majority of its replicas and returns the newest
// version, repairing stale replicas.
func (c *Client) GetQuorum(ctx context.Context, key string) (GetResult, error) {
	return c.get(ctx, key, "quorum")
}

func (c *Client) get(ctx context.Context, key, cl string) (GetResult, error) {
	resp, err := c.do(ctx, &protocol.Message{Op: protocol.OpGet, Key: key, Consistency: cl})
	if err != nil {
		return GetResult{}, err
	}
	r := GetResult{Value: resp.Value, Found: resp.Found, TTL: time.Duration(resp.TTLMs) * time.Millisecond}
	if resp.Version != nil {
		r.Version = *resp.Version
	}
	return r, nil
}

// Set writes key. ttl <= 0 means no expiry. It returns the version assigned by
// the coordinator once the write quorum has acknowledged it.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (version.Version, error) {
	resp, err := c.do(ctx, &protocol.Message{Op: protocol.OpSet, Key: key, Value: value, TTLMs: ttl.Milliseconds()})
	if err != nil {
		return version.Version{}, err
	}
	return *resp.Version, nil
}

// Delete removes key (by writing a versioned tombstone).
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, &protocol.Message{Op: protocol.OpDelete, Key: key})
	return err
}

// Owners returns the owners of key in the client's current view.
func (c *Client) Owners(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Owners(key, c.rf)
}
