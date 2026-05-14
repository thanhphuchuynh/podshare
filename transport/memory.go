// Package transport contains pluggable Transport implementations for
// podshare: Memory (testing), Redis (pub/sub), and P2P (TCP mesh).
package transport

import (
	"context"
	"errors"
	"sync"
)

// MemoryTransport is an in-process broker. Stores constructed against the
// same MemoryTransport instance share state — useful for testing and for
// single-process simulations of multi-pod behavior.
//
// MemoryTransport is safe for concurrent use.
type MemoryTransport struct {
	// mu is RW: Publish takes RLock so many publishers run in parallel,
	// while Close (which closes subscriber channels) takes the write
	// lock and therefore waits for all in-flight Publishes to finish.
	// Without this, Close races Publish and we send on a closed channel.
	mu        sync.RWMutex
	subs      map[string][]chan []byte
	snapshots map[string][]byte
	onError   func(error)
	closed    bool
}

// MemoryOption configures a MemoryTransport at construction.
type MemoryOption func(*MemoryTransport)

// WithMemoryOnError installs a callback that fires when Publish drops a
// message because a subscriber's buffer is full. Without this, drops
// are silent — which masks real bugs in tests. Wire it up.
func WithMemoryOnError(fn func(error)) MemoryOption {
	return func(m *MemoryTransport) { m.onError = fn }
}

// NewMemoryTransport returns an empty in-memory transport.
func NewMemoryTransport(opts ...MemoryOption) *MemoryTransport {
	m := &MemoryTransport{
		subs:      make(map[string][]chan []byte),
		snapshots: make(map[string][]byte),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *MemoryTransport) Publish(_ context.Context, topic string, msg []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return errors.New("transport: closed")
	}
	for _, ch := range m.subs[topic] {
		// Defensive copy: callers may reuse msg.
		buf := append([]byte(nil), msg...)
		select {
		case ch <- buf:
		default:
			if m.onError != nil {
				m.onError(errors.New("transport: memory subscriber buffer full; dropped message"))
			}
		}
	}
	return nil
}

func (m *MemoryTransport) Subscribe(_ context.Context, topic string) (<-chan []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("transport: closed")
	}
	ch := make(chan []byte, 256)
	m.subs[topic] = append(m.subs[topic], ch)
	return ch, nil
}

func (m *MemoryTransport) Snapshot(_ context.Context, topic string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("transport: closed")
	}
	m.snapshots[topic] = append([]byte(nil), data...)
	return nil
}

func (m *MemoryTransport) FetchSnapshot(_ context.Context, topic string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("transport: closed")
	}
	raw, ok := m.snapshots[topic]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), raw...), nil
}

func (m *MemoryTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for _, chs := range m.subs {
		for _, ch := range chs {
			close(ch)
		}
	}
	m.subs = nil
	return nil
}
