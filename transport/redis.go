package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RedisOptions configures a RedisTransport. Pass an existing Client for
// custom dial options, TLS, sentinel, or cluster setups; otherwise the
// transport constructs a default Client from Addr/Password/DB.
type RedisOptions struct {
	Addr     string
	Password string
	DB       int

	// Prefix is prepended to channel and snapshot keys ("podshare" by default).
	Prefix string

	// Client, when non-nil, is used directly; Addr/Password/DB/TLSConfig
	// are ignored. The transport will not close a Client it did not own.
	Client *redis.Client

	// TLSConfig, when non-nil, wraps the Redis connection with TLS.
	// Ignored if Client is provided (configure TLS on that Client
	// directly).
	TLSConfig *tls.Config
}

// RedisTransport publishes events on a Redis pub/sub channel and stores
// snapshots in a sibling key. Redis is the simplest path when an instance
// is already part of your stack.
//
// Semantics: Redis Pub/Sub is at-most-once. Messages published while a
// subscriber is disconnected (network blip, server failover) are lost
// with no replay. Replicas will diverge until the next snapshot or the
// next write to each affected key. For at-least-once semantics, build a
// transport on top of Redis Streams + consumer groups instead.
//
// Security: connections use whatever Client you supply. Plaintext by
// default — pass a Client built with TLS options for transport-level
// encryption. Anyone with credentials can write any key on the channel.
type RedisTransport struct {
	client    *redis.Client
	prefix    string
	ownClient bool

	mu     sync.Mutex
	subs   map[string]*redisSub
	closed bool
}

type redisSub struct {
	pubsub *redis.PubSub
	out    chan []byte
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRedisTransport constructs a transport. It pings the server eagerly
// so configuration errors surface at construction time.
func NewRedisTransport(opts RedisOptions) (*RedisTransport, error) {
	if opts.Prefix == "" {
		opts.Prefix = "podshare"
	}

	var (
		client    *redis.Client
		ownClient bool
	)
	switch {
	case opts.Client != nil:
		client = opts.Client
	case opts.Addr != "":
		client = redis.NewClient(&redis.Options{
			Addr:      opts.Addr,
			Password:  opts.Password,
			DB:        opts.DB,
			TLSConfig: opts.TLSConfig,
		})
		ownClient = true
	default:
		return nil, errors.New("transport: redis requires Addr or Client")
	}

	pingCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		if ownClient {
			_ = client.Close()
		}
		return nil, fmt.Errorf("transport: redis ping: %w", err)
	}

	return &RedisTransport{
		client:    client,
		prefix:    opts.Prefix,
		ownClient: ownClient,
		subs:      make(map[string]*redisSub),
	}, nil
}

func (r *RedisTransport) channelKey(topic string) string {
	return fmt.Sprintf("%s:%s:events", r.prefix, topic)
}

func (r *RedisTransport) snapshotKey(topic string) string {
	return fmt.Sprintf("%s:%s:snapshot", r.prefix, topic)
}

func (r *RedisTransport) Publish(ctx context.Context, topic string, msg []byte) error {
	return r.client.Publish(ctx, r.channelKey(topic), msg).Err()
}

func (r *RedisTransport) Subscribe(ctx context.Context, topic string) (<-chan []byte, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("transport: closed")
	}
	if _, exists := r.subs[topic]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("transport: already subscribed to %q", topic)
	}

	pubsub := r.client.Subscribe(ctx, r.channelKey(topic))
	if _, err := pubsub.Receive(ctx); err != nil {
		r.mu.Unlock()
		_ = pubsub.Close()
		return nil, fmt.Errorf("transport: subscribe %q: %w", topic, err)
	}

	out := make(chan []byte, 256)
	subCtx, subCancel := context.WithCancel(context.Background())
	sub := &redisSub{
		pubsub: pubsub,
		out:    out,
		cancel: subCancel,
		done:   make(chan struct{}),
	}
	r.subs[topic] = sub
	r.mu.Unlock()

	go func() {
		defer close(sub.done)
		defer close(out)
		ch := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- []byte(m.Payload):
				case <-subCtx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

func (r *RedisTransport) Snapshot(ctx context.Context, topic string, data []byte) error {
	return r.client.Set(ctx, r.snapshotKey(topic), data, 0).Err()
}

func (r *RedisTransport) FetchSnapshot(ctx context.Context, topic string) ([]byte, error) {
	raw, err := r.client.Get(ctx, r.snapshotKey(topic)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (r *RedisTransport) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	subs := r.subs
	r.subs = nil
	r.mu.Unlock()

	for _, sub := range subs {
		sub.cancel()
		_ = sub.pubsub.Close()
		<-sub.done
	}

	if r.ownClient {
		return r.client.Close()
	}
	return nil
}
