# podshare

Generic, type-safe cross-pod data sharing for Go. A `Store[T]` is a
`map[string]T` that is replicated across every pod sharing a topic.
Reads are served locally (zero network hop); writes broadcast over a
pluggable transport.

```go
type Config struct {
    RateLimit    int             `json:"rate_limit"`
    FeatureFlags map[string]bool `json:"feature_flags"`
}

tr, _ := transport.NewRedisTransport(transport.RedisOptions{Addr: "localhost:6379"})
defer tr.Close()

store, _ := podshare.New[Config](ctx, "config", tr)
defer store.Close()

store.Set(ctx, "global", Config{RateLimit: 1000})
cfg, _ := store.Get("global")           // local, ~16ns

for ev := range store.Watch(ctx) {
    fmt.Println(ev.Kind, ev.Key, ev.Origin)
}
```

## Status

Pre-1.0. The wire format carries a version byte (`ProtocolVersion`)
and breaking changes will bump it. The Go API may still change.

## Transports

| Transport | When to use | Semantics |
|---|---|---|
| `transport.NewRedisTransport` | Redis is already in your stack | At-most-once Pub/Sub; snapshot in a sibling key |
| `transport.NewP2PTransport` | No external deps; in-cluster mesh | TCP, optional mTLS via `TLSConfig` |
| `transport.NewMemoryTransport` | Tests, single-process simulation | In-process broker |

## API at a glance

```go
// Reads — all O(local map lookup)
store.Get(key)
store.Keys()
store.Snapshot()
store.Version(key)              // for SetIf
store.Stats()                   // counters: writes, reads, watchers, snapshots

// Writes — apply locally, broadcast, debounce snapshot
store.Set(ctx, key, value)
store.Delete(ctx, key)
store.SetMany(ctx, kvs)         // atomic on local view
store.DeleteMany(ctx, keys)
store.SetIf(ctx, key, value, prev)  // best-effort CAS (see godoc)
store.Seed(initial)             // hydrate at startup without broadcasting

// Watch — non-blocking dispatch, slow watchers are dropped
store.Watch(ctx)                // all events
store.WatchKey(ctx, key)        // one key
store.WatchPrefix(ctx, prefix)  // "ws:", "feature:checkout:" …

// Recovery
store.Refresh(ctx)              // re-fetch snapshot, merge under LWW
store.Close()
```

## Options

- `WithCodec` / `WithNodeID` / `WithWatchBuffer` — basics
- `WithSnapshotInterval` / `WithGCInterval` / `WithTombstoneTTL` — snapshot + compaction tuning
- `WithMerger` — CRDT-style per-value merge (replaces LWW outcome for `Set`)
- `WithErrorHandler` — surface decode/snapshot/dropped-watcher errors
- `WithLogger` — `*slog.Logger` for operational events

## When to reach for podshare vs. something else

**Good fits**: feature flags, rate-limit counters, chat-history hot
cache, presence registries, fleet-wide circuit breakers, small routing
tables ("user X lives on pod-3").

**Bad fits**: durable source of truth, large datasets (every pod
holds the full map), strong consistency (use etcd), cross-region
replication.

For high write churn on the same key, LWW drops concurrent writes by
design. `WithMerger` is safe **only** when your merge function is a
true CRDT join (commutative, associative, idempotent) — peers each
run the merger against their own local state. For ordered chat-message
logs, model as a G-Set of `{ID, Timestamp, Content}` and sort on read;
a list-append merger will diverge across pods.

Comparison with **olric**, **NATS JetStream KV**, **etcd v3**,
**memberlist**, **groupcache** — see `go doc github.com/thanhphuchuynh/podshare`.

## Subpackages

- [`transport`](./transport) — Memory, Redis, and P2P transports
- [`callreply`](./callreply) — RPC layer for "call this method on whichever
  pod owns target X" (e.g., forwarding a push to the pod holding a
  user's WebSocket)
- [`prom`](./prom) — Prometheus collector wrapping `Stats()`. Optional;
  only imports `client_golang` when you import this subpackage.
- [`examples/basic`](./examples/basic), [`redis`](./examples/redis),
  [`p2p`](./examples/p2p), [`chat-cache`](./examples/chat-cache),
  [`feature-flags`](./examples/feature-flags),
  [`ws-routing`](./examples/ws-routing)

## Operational gotchas

- **Clock skew**: LWW orders by wall-clock time. Run NTP/PTP.
- **Redis Pub/Sub is at-most-once**: messages between drop and reconnect
  are lost. Call `store.Refresh(ctx)` after detecting reconnection.
- **P2P is plaintext by default**: pass a `TLSConfig` with mTLS for
  authenticated meshes.
- **Watchers must drain**: a full watcher channel gets dropped (channel
  closed) and reported via `WithErrorHandler`. Treat that as "fell
  behind — re-subscribe and re-snapshot."
- **Tombstones**: keys remain as tombstones for `WithTombstoneTTL` (24h
  default) and are GC'd on the snapshot tick. Idle topics still
  compact via `WithGCInterval` (5m default).

## Run tests / benchmarks / fuzz

```sh
go test -race ./...
go test -bench=. -benchtime=1s ./...
go test -fuzz=FuzzReadFrame -fuzztime=30s -run=^$ ./transport
```
