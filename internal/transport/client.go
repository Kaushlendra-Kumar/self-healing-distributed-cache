package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
)

// ErrClosed is returned after Client.Close.
var ErrClosed = errors.New("transport: client closed")

// ClientConfig tunes a Client.
type ClientConfig struct {
	MaxFrame       int
	DialTimeout    time.Duration // default 1s
	DefaultTimeout time.Duration // applied when ctx has no deadline (default 5s)
}

// Client multiplexes concurrent requests over a single TCP connection to one
// address. If the connection fails, every in-flight call fails with the
// connection error (the caller cannot know whether the remote side applied
// the request) and the next call dials a fresh connection.
type Client struct {
	addr   string
	cfg    ClientConfig
	nextID atomic.Uint64

	mu     sync.Mutex
	cur    *clientConn
	closed bool
}

type clientConn struct {
	c    net.Conn
	cfg  ClientConfig
	wmu  sync.Mutex
	mu   sync.Mutex
	pend map[uint64]chan *protocol.Message
	err  error
	done chan struct{}
}

// NewClient creates a client; it dials lazily.
func NewClient(addr string, cfg ClientConfig) *Client {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = time.Second
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 5 * time.Second
	}
	return &Client{addr: addr, cfg: cfg}
}

// Addr returns the remote address.
func (c *Client) Addr() string { return c.addr }

// Close closes the connection and fails in-flight calls.
func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	cur := c.cur
	c.cur = nil
	c.mu.Unlock()
	if cur != nil {
		cur.fail(ErrClosed)
	}
}

func (c *Client) conn(ctx context.Context) (*clientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.cur != nil {
		select {
		case <-c.cur.done:
			c.cur = nil
		default:
			return c.cur, nil
		}
	}
	d := net.Dialer{Timeout: c.cfg.DialTimeout}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
	}
	cc := &clientConn{c: nc, cfg: c.cfg, pend: map[uint64]chan *protocol.Message{}, done: make(chan struct{})}
	go cc.readLoop()
	c.cur = cc
	return cc, nil
}

// Call sends req and waits for the correlated response or ctx expiry.
// A nil error means a response arrived; it may still be an error response
// (check resp.AsError()). The request is copied, so callers may send the same
// message to several peers concurrently.
func (c *Client) Call(ctx context.Context, req *protocol.Message) (*protocol.Message, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.DefaultTimeout)
		defer cancel()
	}
	cc, err := c.conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.addr, err)
	}
	m := *req
	m.ID = c.nextID.Add(1)
	ch := make(chan *protocol.Message, 1)
	if err := cc.register(m.ID, ch); err != nil {
		return nil, err
	}
	defer cc.unregister(m.ID)

	cc.wmu.Lock()
	dl, _ := ctx.Deadline()
	_ = cc.c.SetWriteDeadline(dl)
	err = protocol.WriteFrame(cc.c, &m, c.cfg.MaxFrame)
	cc.wmu.Unlock()
	if err != nil {
		if !errors.Is(err, protocol.ErrFrameTooLarge) {
			cc.fail(err)
		}
		return nil, fmt.Errorf("send to %s: %w", c.addr, err)
	}
	// A successful write only means the bytes reached the local kernel; the
	// outcome is unknown until the response arrives.
	select {
	case resp := <-ch:
		return resp, nil
	case <-cc.done:
		return nil, fmt.Errorf("connection to %s lost: %w", c.addr, cc.failure())
	case <-ctx.Done():
		return nil, fmt.Errorf("call %s to %s: %w", req.Op, c.addr, ctx.Err())
	}
}

func (cc *clientConn) register(id uint64, ch chan *protocol.Message) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.err != nil {
		return cc.err
	}
	cc.pend[id] = ch
	return nil
}

func (cc *clientConn) unregister(id uint64) {
	cc.mu.Lock()
	delete(cc.pend, id)
	cc.mu.Unlock()
}

func (cc *clientConn) failure() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.err
}

func (cc *clientConn) fail(err error) {
	cc.mu.Lock()
	if cc.err != nil {
		cc.mu.Unlock()
		return
	}
	cc.err = err
	cc.pend = map[uint64]chan *protocol.Message{}
	close(cc.done)
	cc.mu.Unlock()
	cc.c.Close()
}

func (cc *clientConn) readLoop() {
	for {
		m, err := protocol.ReadFrame(cc.c, cc.cfg.MaxFrame)
		if err != nil {
			var de *protocol.DecodeError
			if errors.As(err, &de) {
				continue // cannot correlate; the caller will time out
			}
			cc.fail(err)
			return
		}
		cc.mu.Lock()
		ch := cc.pend[m.ID]
		delete(cc.pend, m.ID)
		cc.mu.Unlock()
		if ch != nil {
			ch <- m // buffered; never blocks
		}
	}
}

// Pool keeps one Client per peer address and applies outbound fault
// injection keyed by peer ID.
type Pool struct {
	cfg    ClientConfig
	faults *Faults

	mu      sync.Mutex
	clients map[string]*Client
}

// NewPool creates a pool. faults may be nil.
func NewPool(cfg ClientConfig, faults *Faults) *Pool {
	return &Pool{cfg: cfg, faults: faults, clients: map[string]*Client{}}
}

// Call sends req to the peer with the given ID at addr.
func (p *Pool) Call(ctx context.Context, peerID, addr string, req *protocol.Message) (*protocol.Message, error) {
	if err := p.faults.beforeSend(ctx, peerID); err != nil {
		return nil, err
	}
	p.mu.Lock()
	c, ok := p.clients[addr]
	if !ok {
		c = NewClient(addr, p.cfg)
		p.clients[addr] = c
	}
	p.mu.Unlock()
	return c.Call(ctx, req)
}

// Close closes every client.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for a, c := range p.clients {
		c.Close()
		delete(p.clients, a)
	}
}
