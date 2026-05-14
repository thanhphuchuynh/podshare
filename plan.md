# podshare — plan

Status: pre-1.0. Two review/fix passes done. P0/P1 bugs from both reviews
closed. Go 1.26. Module path `github.com/thanhphuchuynh/podshare`.

The work below is grouped by **stop-and-validate** (do this first) and
**keep-building** (the deferred queue, in priority order).

---

## Recommended path: stop and validate

Two iterations of "find bugs by reading code" have hit diminishing
returns. Real signal comes from putting podshare under a workload that
wasn't designed by its author. The chat-history hot cache is the
obvious validator.

### Step 0 — pre-publish housekeeping (1 afternoon)

- [x] Replace `github.com/yourorg/podshare` → `github.com/thanhphuchuynh/podshare`.
- [ ] `git init`; commit current tree as `v0.1.0`. CHANGELOG and API
  stability claims need a history to anchor to.
- [ ] Add `LICENSE`. Apache-2.0 or MIT; either is fine for a Go library.
- [ ] Add minimal CI:
  - `go test -race ./...`
  - `go vet ./...`
  - `go test -fuzz=FuzzReadFrame -fuzztime=30s -run=^$ ./transport`
  - `go build ./examples/...` (catches the broken-example regression)
- [ ] Push to GitHub (`thanhphuchuynh/podshare`), enable Actions.

### Step 1 — integration test against the real use case (1–2 days)

Build a small chat-server toy that *uses* podshare end-to-end:

- 2–3 pods deployed (local docker-compose is fine; minikube is better).
- Redis transport for the routing/hot-cache.
- `callreply` for cross-pod message forwarding (the WS-routing pattern).
- 100–200 concurrent fake sessions exchanging messages, with some pods
  killed mid-session and restarted.

Things this will reveal that the unit tests won't:

| Concern | What to watch |
|---|---|
| Snapshot bandwidth | Network bytes/sec at realistic key counts; 200ms default may be too aggressive |
| Redis reconnect | What happens when the cluster fails over mid-session |
| GC pressure | Allocations/sec under sustained JSON envelope load |
| Clock skew | Whether `WithMaxClockSkew` actually fires in a real cluster (NTP usually keeps it <100ms) |
| Memory growth | Heap profile after 1h of sustained load |
| Watcher backlog | Whether the default `WatchBuffer=64` is large enough for real burst rates |

Come back with the 1–3 deferred items that actually got in your way.
The rest of the backlog below stops being speculative.

### Step 2 — chaos tests (half a day)

- Kill a pod mid-write. Does the survivor's state converge after the
  killed pod is replaced via snapshot fetch?
- Drop 10% of Redis Pub/Sub messages via toxiproxy. Does `Refresh`
  recover state?
- Partition the P2P mesh and heal. Does `OnConnect` fire correctly with
  expected counts?

---

## Keep-building backlog (priority order)

Each item below was deferred in the CHANGELOG with a stated reason.
Promote based on what the integration test reveals.

### Tier 1 — operability (do as soon as you have a real user)

- **Prometheus collector wrapping `Stats()`** (~30 lines). Without
  this, "we have metrics" is theoretical.
- **OpenTelemetry tracing hooks** — spans on `Set`/`Get`/`Watch`/`Call`.
  Required for prod debugging once two services depend on each other.
- **Structured slog usage on the write hot path** — currently logs
  only on snapshot/GC. Lifecycle is covered; write-flow isn't.

### Tier 2 — durability / second transport

- **Second transport: NATS JetStream KV.** Proves the Transport
  interface generalizes. Gives users an at-least-once option, which
  Redis Pub/Sub can't be. Critical for any "must not lose writes" case.
- **Subscription cancel function from `Transport.Subscribe`** — closes
  the leaked-subscription gap when Stores are created/destroyed at
  runtime. Needs an interface change.

### Tier 3 — wire / performance (only if bench warrants)

- **`json.RawMessage` value path when codec is JSONCodec** — saves the
  base64 round-trip. Conditional on codec identity. Skip otherwise.
- **Sharded data map** — only if a real workload pushes >50k writes/sec
  on a single store and the bench points to the mutex.
- **Per-peer P2P send queue tuning** — current default 1024 is a guess.

### Tier 4 — design refactors (only if a 2nd consumer needs them)

- **`callreply.RouteTable` interface** so callers can supply etcd / a
  Redis hash / their own router instead of an internal podshare Store.
- **Per-peer epoch / stable peer identity** in P2P so `OnConnect` /
  `OnDisconnect` events are deduplicatable across flaps.

---

## Anti-recommendations (things NOT to do next)

- **Another round of code review.** We've hit the floor; the remaining
  finds are speculative ("might break if X under Y"). Running it
  reveals more than reading it.
- **Sharded maps before a workload demands them.** The bench currently
  shows JSON marshal as the bottleneck, not the mutex.
- **Building a "smart" snapshot strategy** (adaptive intervals,
  partial snapshots). Wait until the simple default breaks.
- **More options.** The surface is already wide. Hide complexity, don't
  add knobs.

---

## Open questions to resolve before v1.0

1. Should `Set` returning `ErrBroadcast` be a hard error or a warning?
   Current code returns it as an error; some callers will want to
   ignore it.
2. Should `WithMerger` be renamed to something that screams "CRDT
   required"? `WithCRDTJoin`? The current name still misleads.
3. Should `Transport` have an explicit `Unsubscribe`, or should
   subscription lifetime tie to ctx? Pick one before adopters write
   plugins.
4. What's the API stability promise pre-1.0 vs. post-1.0? Document.

---

## Done so far (for context)

- Core `Store[T]` with LWW + Seq tiebreaker, debounced snapshots,
  tombstone TTL + periodic GC, error handler + slog hooks.
- Transports: Memory (RWMutex-correct), Redis (Pub/Sub + TLSConfig),
  P2P (TCP mesh + mTLS + per-peer queues + keepalive +
  OnConnect/OnDisconnect).
- `callreply` RPC subpackage with self-call short-circuit,
  `MaxConcurrentHandlers` backpressure, and wire-deadline propagation.
- API: `Set/Get/Delete/Watch{,Filter,Key,Prefix}/SetMany/DeleteMany/Seed/TrySet/Version/Refresh/Stats/Close{,WithContext}`.
- Coverage: race regression for dispatch, MemoryTransport close race,
  Refresh, snapshot-persist failure path, slow watcher drop, Seed
  guard, P2P OnConnect, fuzz on the frame parser.
- Examples: basic, redis, p2p, chat-cache, feature-flags, ws-routing.
- Docs: README, CHANGELOG with explicit deferral rationale, godoc on
  every public symbol.
- 5-run `-race` clean. ~880ns/Set, ~17ns/Get, ~3.7µs cross-pod, ~8.5µs
  RPC round-trip on Apple M1 Pro.
