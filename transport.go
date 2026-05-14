package podshare

import "context"

// Transport carries wire messages between pods that share a topic.
//
// Implementations are responsible for fan-out: every Subscribe channel
// associated with topic must receive every message that any pod publishes
// to topic. Self-delivery is allowed — Store filters its own messages by
// node ID — so implementations need not suppress echoes.
//
// Snapshot persists the most recent full state for topic; FetchSnapshot
// returns it (or nil, nil if no snapshot exists). Snapshots let late-
// joining nodes catch up without replaying the entire event stream.
type Transport interface {
	Publish(ctx context.Context, topic string, msg []byte) error

	// Subscribe returns a receive-only channel of inbound messages for
	// topic. The channel is closed when the transport is closed. Calling
	// Subscribe more than once for the same topic is implementation-
	// defined; built-in transports return an error.
	Subscribe(ctx context.Context, topic string) (<-chan []byte, error)

	Snapshot(ctx context.Context, topic string, data []byte) error

	// FetchSnapshot returns the latest persisted snapshot for topic, or
	// (nil, nil) if no snapshot has been written yet.
	FetchSnapshot(ctx context.Context, topic string) ([]byte, error)

	Close() error
}
