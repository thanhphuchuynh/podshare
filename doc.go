// Package podshare is a generic, type-safe cross-pod data store.
//
// A Store[T] holds a strongly-typed map[string]T that is replicated
// across every pod sharing the same topic. Reads are served from a
// local in-memory cache (zero network hop). Writes are applied
// locally first, then broadcast over a pluggable Transport so peers
// converge on the same state.
//
// Conflict resolution is last-write-wins ordered by (Timestamp,
// Origin, Seq). For richer semantics (CRDT, vector clocks) layer
// merge logic on top of the Watch channel.
//
// Two transports ship with the module: a Redis pub/sub transport in
// transport/redis.go for shops that already run Redis, and a P2P TCP
// transport in transport/p2p.go for dependency-free meshes. A Memory
// transport in transport/memory.go is provided for tests.
//
// # When to use podshare
//
// Good fits — small mutable state that every pod needs the same
// view of:
//
//   - feature flags and config
//   - rate-limit counters and quotas
//   - chat-history hot cache
//   - presence / online-user registries
//   - fleet-wide circuit breakers and kill switches
//   - small routing tables ("user X lives on pod-3")
//
// Bad fits — reach for something else:
//
//   - source of truth for unloseable data (no durability guarantee)
//   - large datasets (every pod holds the full map)
//   - high write churn on the same key (LWW drops concurrent updates)
//   - strong consistency, locks, or leader election (use etcd)
//   - cross-region replication (designed for in-cluster fan-out)
//
// # Trade-offs vs alternatives
//
// olric — embedded distributed KV with sharding and replicas. Pick
// when the dataset is too large to replicate on every pod and you
// want it embedded with no extra service.
//
// NATS JetStream KV — keyed bucket with watch, history, and
// durability. Runs against a NATS cluster (not embedded). The
// "boring production" answer when state must survive pod restarts.
//
// etcd v3 — linearizable KV with watch. Overkill for shared cache;
// the right pick for locks, leader election, and config that must
// not split-brain.
//
// hashicorp/memberlist — gossip primitive. Use as a building block
// if you need gossip semantics (anti-entropy, failure detection)
// and you're willing to write the typed map and LWW logic yourself.
//
// groupcache — peer cache with consistent hashing. Different model:
// each key lives on one peer, not full replication. Cannot
// substitute for podshare's "every pod has the full view."
//
// # Operational notes
//
// LWW orders by wall-clock time. Run NTP or PTP — sustained skew
// between pods will reorder writes.
//
// Snapshot persistence is asynchronous and debounced (default
// 200ms, see WithSnapshotInterval). Bursts coalesce into a single
// snapshot; Close flushes any pending state.
//
// Tombstones are kept for WithTombstoneTTL (default 24h) so
// concurrent late writes from peers cannot resurrect deleted keys.
// Expired tombstones are GCed at snapshot time, so an idle topic
// will not compact until the next write.
//
// Watch uses non-blocking dispatch. A consumer whose buffer fills
// is dropped (channel closed) and reported via WithErrorHandler;
// treat a closed Watch channel as "fell behind — re-subscribe and
// re-snapshot via Get."
//
// Each Transport documents its own semantics. RedisTransport is
// at-most-once (Redis Pub/Sub does not replay missed messages);
// P2PTransport is plaintext unless you supply a TLSConfig with
// mTLS for authenticated meshes.
package podshare
