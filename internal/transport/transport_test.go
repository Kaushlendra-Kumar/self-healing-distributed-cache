package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/protocol"
)

func startServer(t *testing.T, h Handler, cfg ServerConfig) *Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ln, h, cfg)
	go s.Serve()
	t.Cleanup(s.Close)
	return s
}

func echo(ctx context.Context, m *protocol.Message) *protocol.Message {
	switch m.Op {
	case "SLOW":
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
	r := protocol.OK(m)
	r.Key = m.Key
	return r
}

func TestConcurrentMultiplexedCalls(t *testing.T) {
	s := startServer(t, echo, ServerConfig{})
	c := NewClient(s.Addr().String(), ClientConfig{})
	defer c.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmt.Sprint(i)
			resp, err := c.Call(context.Background(), &protocol.Message{Op: "ECHO", Key: k})
			if err != nil {
				errs <- err
				return
			}
			if resp.Key != k {
				errs <- fmt.Errorf("response mismatched: %s != %s", resp.Key, k)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestSlowResponseTimesOutWithoutBlockingOthers(t *testing.T) {
	s := startServer(t, echo, ServerConfig{})
	c := NewClient(s.Addr().String(), ClientConfig{})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, &protocol.Message{Op: "SLOW"})
		done <- err
	}()
	// A fast call on the same connection is not stuck behind the slow one.
	if _, err := c.Call(context.Background(), &protocol.Message{Op: "ECHO"}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestDroppedConnectionFailsInflightAndReconnects(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	c := NewClient(addr, ClientConfig{})
	defer c.Close()
	errc := make(chan error, 1)
	go func() {
		_, err := c.Call(context.Background(), &protocol.Message{Op: "ECHO"})
		errc <- err
	}()
	raw := <-accepted
	raw.Close() // peer drops the connection with the request in flight
	if err := <-errc; err == nil {
		t.Fatal("expected error for in-flight call on dropped connection")
	}
	ln.Close()

	// Bring a real server up on the same address; the client redials.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skip("port reuse unavailable:", err)
	}
	s := NewServer(ln2, echo, ServerConfig{})
	go s.Serve()
	defer s.Close()
	if _, err := c.Call(context.Background(), &protocol.Message{Op: "ECHO"}); err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
}

func TestMalformedPayloadKeepsConnection(t *testing.T) {
	s := startServer(t, echo, ServerConfig{MaxFrame: 1024})
	nc, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	body := []byte{protocol.Version, '{', 'x'}
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(body)))
	nc.Write(append(hdr, body...))
	resp, err := protocol.ReadFrame(nc, 0)
	if err != nil || resp.Code != protocol.CodeBadRequest {
		t.Fatalf("expected bad_request, got %+v %v", resp, err)
	}
	// Same connection still works.
	protocol.WriteFrame(nc, &protocol.Message{ID: 9, Op: "ECHO", Key: "ok"}, 0)
	resp, err = protocol.ReadFrame(nc, 0)
	if err != nil || resp.ID != 9 || resp.Key != "ok" {
		t.Fatalf("connection unusable after malformed message: %+v %v", resp, err)
	}
	// An oversized frame closes the connection after an error response.
	binary.BigEndian.PutUint32(hdr, 1<<20)
	nc.Write(hdr)
	resp, err = protocol.ReadFrame(nc, 0)
	if err != nil || resp.Code != protocol.CodeBadRequest {
		t.Fatalf("expected error response for oversized frame, got %+v %v", resp, err)
	}
	nc.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := protocol.ReadFrame(nc, 0); err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestFaultInjection(t *testing.T) {
	f := NewFaults()
	s := startServer(t, echo, ServerConfig{Faults: f})
	p := NewPool(ClientConfig{}, f)
	defer p.Close()
	ctx := context.Background()
	if _, err := p.Call(ctx, "peer", s.Addr().String(), &protocol.Message{Op: "ECHO"}); err != nil {
		t.Fatal(err)
	}
	f.Block("peer")
	if _, err := p.Call(ctx, "peer", s.Addr().String(), &protocol.Message{Op: "ECHO"}); !errors.Is(err, ErrInjected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	// Inbound blocking: requests from "peer" are dropped by the server.
	p2 := NewPool(ClientConfig{}, nil)
	defer p2.Close()
	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := p2.Call(tctx, "x", s.Addr().String(), &protocol.Message{Op: "ECHO", From: "peer"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected inbound drop to time out, got %v", err)
	}
	f.Reset()
	f.SetDelay(AllPeers, 30*time.Millisecond)
	start := time.Now()
	if _, err := p.Call(ctx, "peer", s.Addr().String(), &protocol.Message{Op: "ECHO"}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("delay not applied")
	}
	if st := f.Snapshot(); st.DelayMs[AllPeers] != 30 {
		t.Fatalf("%+v", st)
	}
}

func TestClosedClient(t *testing.T) {
	c := NewClient("127.0.0.1:1", ClientConfig{DialTimeout: 50 * time.Millisecond})
	c.Close()
	if _, err := c.Call(context.Background(), &protocol.Message{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}
