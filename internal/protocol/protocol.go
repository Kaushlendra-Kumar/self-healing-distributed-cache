// Package protocol defines the versioned, length-prefixed wire format used
// between clients and nodes and between nodes.
//
// Frame layout (all integers big-endian):
//
//	+----------------+---------+----------------------------+
//	| length uint32  | ver u8  | JSON-encoded Message       |
//	+----------------+---------+----------------------------+
//	  length = 1 + len(JSON)
//
// Length-prefixing solves TCP's lack of message boundaries: a reader knows
// exactly how many bytes belong to the next message, even when the kernel
// delivers them in several partial reads. The length is checked against a
// configured maximum before any allocation so a malformed or hostile peer
// cannot make the receiver allocate gigabytes.
//
// Requests and responses are correlated by Message.ID, which lets many
// requests share (multiplex) one connection with out-of-order responses.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/membership"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/store"
	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/internal/version"
)

// Version is the current wire protocol version.
const Version byte = 1

// DefaultMaxFrame bounds a single frame (4 MiB).
const DefaultMaxFrame = 4 << 20

// Frame errors.
var (
	ErrFrameTooLarge      = errors.New("protocol: frame exceeds maximum size")
	ErrEmptyFrame         = errors.New("protocol: empty frame")
	ErrUnsupportedVersion = errors.New("protocol: unsupported version")
)

// Operation names.
const (
	// Client-facing operations; any node accepts them and forwards as needed.
	OpGet     = "GET"
	OpSet     = "SET"
	OpDelete  = "DELETE"
	OpMembers = "MEMBERS"
	OpPing    = "PING"

	// Internal operations between nodes.
	OpReplicate   = "REPLICATE"    // apply one versioned entry
	OpReadLocal   = "READ_LOCAL"   // return the local entry (for quorum reads)
	OpGossip      = "GOSSIP"       // heartbeat + membership exchange
	OpSyncDigest  = "SYNC_DIGEST"  // "I hold these keys at these versions"; reply lists keys needed
	OpSyncPush    = "SYNC_PUSH"    // full entries for keys the peer needs
	OpSyncRequest = "SYNC_REQUEST" // joining node asks a peer to push everything it should own
)

// Status values.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Error codes. Clients should branch on Code, not on the Error text.
const (
	CodeBadRequest     = "bad_request"
	CodeTooLarge       = "too_large"
	CodeUnavailable    = "unavailable"   // no routable owner for the key
	CodeQuorumFailed   = "quorum_failed" // write may be partially applied
	CodeMinority       = "minority"      // node cannot see a majority of the ring
	CodeNotOwner       = "not_owner"     // forwarded request reached a non-owner
	CodeTimeout        = "timeout"
	CodeInternal       = "internal"
	CodeUnknownOp      = "unknown_op"
	CodeShuttingDown   = "shutting_down"
	CodeNotReady       = "not_ready"
	CodeFaultInjection = "fault_injected"
)

// Digest is a key and the version the sender holds.
type Digest struct {
	Key     string          `json:"k"`
	Version version.Version `json:"v"`
}

// Message is the single envelope used for every request and response. Only
// the fields relevant to an operation are set; the rest are omitted from the
// encoding.
type Message struct {
	ID  uint64 `json:"id"`
	Op  string `json:"op,omitempty"`
	RID string `json:"rid,omitempty"` // correlation ID propagated across hops for logging
	// From is the sending node's ID ("" for external clients).
	From string `json:"from,omitempty"`

	Key         string `json:"key,omitempty"`
	Value       []byte `json:"value,omitempty"`
	TTLMs       int64  `json:"ttl_ms,omitempty"`
	Consistency string `json:"cl,omitempty"`
	Forwarded   bool   `json:"fwd,omitempty"`

	Entry     *store.Entry          `json:"entry,omitempty"`
	Entries   []store.Entry         `json:"entries,omitempty"`
	Digests   []Digest              `json:"digests,omitempty"`
	Keys      []string              `json:"keys,omitempty"`
	Members   []membership.Member   `json:"members,omitempty"`
	DeadMarks []membership.DeadMark `json:"dead,omitempty"`
	Down      []string              `json:"down,omitempty"` // peers the sender currently sees as dead
	Target    *membership.Member    `json:"target,omitempty"`

	// Response fields.
	Status   string           `json:"status,omitempty"`
	Code     string           `json:"code,omitempty"`
	Error    string           `json:"error,omitempty"`
	Found    bool             `json:"found,omitempty"`
	Version  *version.Version `json:"version,omitempty"`
	Count    int              `json:"count,omitempty"`
	Acks     int              `json:"acks,omitempty"`
	RF       int              `json:"rf,omitempty"`
	VNodes   int              `json:"vnodes,omitempty"`
	Routable []string         `json:"routable,omitempty"`
}

// Err builds an error response for req.
func Err(req *Message, code, format string, args ...any) *Message {
	return &Message{ID: req.ID, RID: req.RID, Status: StatusError, Code: code, Error: fmt.Sprintf(format, args...)}
}

// OK builds an empty success response for req.
func OK(req *Message) *Message {
	return &Message{ID: req.ID, RID: req.RID, Status: StatusOK}
}

// RemoteError is a structured error returned by a peer.
type RemoteError struct {
	Code string
	Msg  string
}

func (e *RemoteError) Error() string { return e.Code + ": " + e.Msg }

// AsError converts an error response into a *RemoteError (nil for success).
func (m *Message) AsError() error {
	if m.Status == StatusError {
		return &RemoteError{Code: m.Code, Msg: m.Error}
	}
	return nil
}

// WriteFrame encodes msg and writes a single frame. Callers must serialize
// concurrent writers on the same connection.
func WriteFrame(w io.Writer, msg *Message, maxFrame int) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrame
	}
	if len(body)+1 > maxFrame {
		return ErrFrameTooLarge
	}
	buf := make([]byte, 5+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body)+1))
	buf[4] = Version
	copy(buf[5:], body)
	_, err = w.Write(buf)
	return err
}

// ReadFrame reads exactly one frame. It tolerates arbitrary partial reads
// (io.ReadFull) and rejects oversized frames before allocating their body.
// A JSON decoding error is returned as *DecodeError so a server can answer
// it and keep the connection open: framing is still intact.
func ReadFrame(r io.Reader, maxFrame int) (*Message, error) {
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrame
	}
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, ErrEmptyFrame
	}
	if uint64(n) > uint64(maxFrame) {
		return nil, ErrFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if body[0] != Version {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, body[0])
	}
	var m Message
	if err := json.Unmarshal(body[1:], &m); err != nil {
		return nil, &DecodeError{Err: err}
	}
	return &m, nil
}

// DecodeError reports a well-framed message whose payload could not be parsed.
type DecodeError struct{ Err error }

func (e *DecodeError) Error() string { return "protocol: malformed message: " + e.Err.Error() }
func (e *DecodeError) Unwrap() error { return e.Err }
