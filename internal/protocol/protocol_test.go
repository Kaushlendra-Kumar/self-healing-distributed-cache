package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

func TestRoundTripWithPartialReads(t *testing.T) {
	var buf bytes.Buffer
	in := []*Message{
		{ID: 1, Op: OpSet, Key: "k", Value: []byte{0, 1, 2, 255}, TTLMs: 500},
		{ID: 2, Op: OpReplicate, Entry: &store.Entry{Key: "k", Version: version.Version{T: 7, Node: "a"}}},
		{ID: 3, Status: StatusError, Code: CodeTimeout, Error: "slow"},
	}
	for _, m := range in {
		if err := WriteFrame(&buf, m, 0); err != nil {
			t.Fatal(err)
		}
	}
	r := iotest.OneByteReader(&buf) // worst-case fragmentation
	for _, want := range in {
		got, err := ReadFrame(r, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want.ID || got.Op != want.Op || !bytes.Equal(got.Value, want.Value) || got.Code != want.Code {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
	if _, err := ReadFrame(r, 0); err != io.EOF {
		t.Fatalf("expected clean EOF, got %v", err)
	}
}

func frame(n uint32, body []byte) []byte {
	b := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(b, n)
	return append(b, body...)
}

func TestMalformedFrames(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"oversized length rejected before allocation", frame(1<<31, nil), ErrFrameTooLarge},
		{"zero length", frame(0, nil), ErrEmptyFrame},
		{"truncated body", frame(10, []byte{Version, '{'}), io.ErrUnexpectedEOF},
		{"truncated header", []byte{0, 0}, io.ErrUnexpectedEOF},
		{"wrong version", frame(3, []byte{9, '{', '}'}), ErrUnsupportedVersion},
	}
	for _, c := range cases {
		_, err := ReadFrame(bytes.NewReader(c.data), 1024)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
	_, err := ReadFrame(bytes.NewReader(frame(4, []byte{Version, 'n', 'o', '!'})), 1024)
	var de *DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("expected DecodeError, got %v", err)
	}
}

func TestWriteRejectsOversized(t *testing.T) {
	err := WriteFrame(io.Discard, &Message{Value: make([]byte, 2048)}, 1024)
	if err != ErrFrameTooLarge {
		t.Fatal(err)
	}
}

func TestErrorHelpers(t *testing.T) {
	req := &Message{ID: 5, RID: "r"}
	e := Err(req, CodeBadRequest, "bad %s", "key")
	var re *RemoteError
	if !errors.As(e.AsError(), &re) || re.Code != CodeBadRequest || e.ID != 5 || e.RID != "r" {
		t.Fatalf("%+v", e)
	}
	if OK(req).AsError() != nil {
		t.Fatal("ok must not be an error")
	}
}

func FuzzReadFrame(f *testing.F) {
	var buf bytes.Buffer
	_ = WriteFrame(&buf, &Message{ID: 1, Op: OpGet, Key: "x"}, 0)
	f.Add(buf.Bytes())
	f.Add([]byte{0, 0, 0, 1, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadFrame(bytes.NewReader(data), 4096) // must never panic
	})
}
