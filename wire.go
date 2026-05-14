package podshare

import "time"

// ProtocolVersion identifies the wire format. Bump when a breaking
// change is made. Receivers reject messages whose V is higher than this
// constant; lower versions are accepted with default values for any
// fields the sender didn't populate.
const ProtocolVersion uint8 = 1

// wireMessage is the framing for a single Set or Delete event broadcast
// across pods. The envelope itself is JSON; only Value is codec-encoded
// so swapping codecs does not break protocol compatibility.
type wireMessage struct {
	V         uint8     `json:"v,omitempty"`
	Kind      string    `json:"kind"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	Origin    string    `json:"origin"`
	Timestamp time.Time `json:"timestamp"`
	// Seq is a per-origin monotonic counter. It is the third LWW
	// tiebreaker after Timestamp and Origin, ensuring two same-instant
	// writes from the same node are ordered deterministically.
	Seq uint64 `json:"seq,omitempty"`
}

const (
	kindSet    = "set"
	kindDelete = "delete"
)

// wireSnapshot is the full-state catch-up payload sent to new nodes.
type wireSnapshot struct {
	V       uint8                        `json:"v,omitempty"`
	Entries map[string]wireSnapshotEntry `json:"entries"`
}

type wireSnapshotEntry struct {
	Value     []byte    `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Origin    string    `json:"origin"`
	Tombstone bool      `json:"tombstone,omitempty"`
	Seq       uint64    `json:"seq,omitempty"`
}
