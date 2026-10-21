// Package transport moves protocol messages over TCP.
//
// The server reads frames on one goroutine per connection and handles each
// request on its own goroutine (bounded per connection), so a slow request
// does not block others multiplexed on the same connection. Responses are
// written under a per-connection mutex and correlated by message ID.
package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
)

// Handler processes one request and returns its response. It must honour ctx.
type Handler func(ctx context.Context, req *protocol.Message) *protocol.Message

// ServerConfig tunes a Server.
type ServerConfig struct {
	MaxFrame        int
	MaxInflightConn int           // concurrent requests per connection (default 256)
	WriteTimeout    time.Duration // per response write (default 5s)
	Faults          *Faults       // optional inbound fault injection
	Logger          *slog.Logger
}

// Server accepts connections and dispatches frames to a Handler.
type Server struct {
	ln     net.Listener
	h      Handler
	cfg    ServerConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewServer wraps a listener. Call Serve to start accepting.
func NewServer(ln net.Listener, h Handler, cfg ServerConfig) *Server {
	if cfg.MaxInflightConn <= 0 {
		cfg.MaxInflightConn = 256
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{ln: ln, h: h, cfg: cfg, ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}}
}

// Addr returns the listening address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Close is called.
func (s *Server) Serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(c)
	}
}

// Close stops accepting, closes every connection and waits for in-flight
// handlers to return.
func (s *Server) Close() {
	s.mu.Lock()
	s.cancel()
	s.ln.Close()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) serveConn(c net.Conn) {
	var handlers sync.WaitGroup
	defer func() {
		c.Close()
		handlers.Wait()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	var wmu sync.Mutex
	write := func(m *protocol.Message) {
		wmu.Lock()
		defer wmu.Unlock()
		_ = c.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		if err := protocol.WriteFrame(c, m, s.cfg.MaxFrame); err != nil {
			if errors.Is(err, protocol.ErrFrameTooLarge) {
				_ = protocol.WriteFrame(c, &protocol.Message{ID: m.ID, RID: m.RID, Status: protocol.StatusError,
					Code: protocol.CodeTooLarge, Error: "response exceeds maximum frame size"}, s.cfg.MaxFrame)
				return
			}
			c.Close() // a failed write leaves the stream in an unknown state
		}
	}
	sem := make(chan struct{}, s.cfg.MaxInflightConn)
	for {
		req, err := protocol.ReadFrame(c, s.cfg.MaxFrame)
		if err != nil {
			var de *protocol.DecodeError
			if errors.As(err, &de) {
				// Framing is intact; answer and keep the connection.
				write(&protocol.Message{Status: protocol.StatusError, Code: protocol.CodeBadRequest, Error: de.Error()})
				continue
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && s.ctx.Err() == nil {
				s.cfg.Logger.Debug("closing connection", "remote", c.RemoteAddr().String(), "err", err)
				if errors.Is(err, protocol.ErrFrameTooLarge) || errors.Is(err, protocol.ErrUnsupportedVersion) {
					write(&protocol.Message{Status: protocol.StatusError, Code: protocol.CodeBadRequest, Error: err.Error()})
				}
			}
			return
		}
		if s.cfg.Faults.Blocked(req.From) {
			continue // simulate a partition: the request is silently lost
		}
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			return
		}
		handlers.Add(1)
		go func() {
			defer func() { <-sem; handlers.Done() }()
			resp := s.h(s.ctx, req)
			if resp == nil {
				resp = protocol.OK(req)
			}
			resp.ID = req.ID
			if s.cfg.Faults.Blocked(req.From) {
				return
			}
			write(resp)
		}()
	}
}
