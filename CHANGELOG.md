# Changelog

## Unreleased — observability + schema-drift detection

### Added

- **`StrictJSONCodec`** in the main package. Uses `json.DisallowUnknownFields`
  on decode — schema drift during a rolling deploy surfaces as `OnError`
  calls instead of silent field-drop. Wire it with
  `podshare.WithCodec[T](podshare.StrictJSONCodec{})` and pair with
  `WithErrorHandler` for observability.
- **`podshare/prom`** subpackage. Wraps `Store.Stats()` as a
  `prometheus.Collector` with 12 metrics (writes, reads, watchers,
  snapshots, keys, tombstones, GC). Constructor takes a `func() Stats`
  so it can also wrap custom Stats sources. Pulls in
  `github.com/prometheus/client_golang` only when imported.

### Tests

- `TestStrictJSONCodec_*` — accepts matching schema and missing fields,
  rejects unknown fields, surfaces drift via `OnError` end-to-end.
- `TestCollectorRegistersAndScrapes` — lint passes, 12 metrics emitted,
  spot-check expected values for writes/reads/keys/tombstones, label
  propagation verified.

## Unreleased — second-review fixes + Go 1.26

### Toolchain

- `go 1.26` in `go.mod`. Range-over-int loops modernized.

### Fixed (correctness)

- **`dispatch` race against close.** Watcher cleanup could `close(w.ch)` between dispatch's snapshot of the watchers map and its send, causing a send-on-closed-channel panic. `dispatch` now holds `watchersMu` through the send loop and inlines the slow-watcher drop. Regression test `TestDispatchRaceVsClose` races writes against close 25× to confirm.
- **`MemoryTransport` race against close.** Surfaced by the regression test above — `Close()` could close subscriber channels while a concurrent `Publish` was still mid-send. Switched to `sync.RWMutex`: `Publish` holds the read lock through its send loop, `Close` takes the write lock and therefore waits.
- **`SetMany`/`DeleteMany` dispatch outside the data lock.** Same fix already applied to `Refresh`: events are collected under `s.mu`, the lock is released, then events fan out. A 10k-key batch no longer wedges readers for the full duration.
- **`Seed` updates `WritesLocal` and `WritesApplied`** so `Stats` invariants hold after seeding.
- **`CloseWithContext` cleanup safety.** The post-timeout loop now calls `removeWatcher` (membership-guarded) instead of raw `close`, so concurrent per-Watch cleanup goroutines can't trigger a double-close panic.

### Fixed (semantics + docs)

- **`WithMerger` godoc rewritten** to require a true CRDT join (commutative, associative, idempotent). Previously the README and option doc implied list-append works — it does not (peers diverge). G-Set + sort-on-read recipe documented.
- README updated to match: removed the "use `WithMerger` for chat append" advice.

### Added

- **`WithMaxClockSkew(d)`** option (default 1 min). Incoming peer messages whose timestamp drifts more than `d` from local clock are reported via `OnError`. Replication still applies (LWW continues), but operators get an observable signal for misconfigured NTP/PTP.
- **`*slog.Logger` wired through lifecycle** — store start, refresh start/complete, close — not just GC events. The option is no longer cosmetic.
- **`callreply.Options.MaxConcurrentHandlers`** (default 1024). Inbound calls past the cap reply with `"endpoint busy"` instead of spawning unbounded handler goroutines.
- **`callreply.Call` self-call short-circuit.** When `route.PodID == SelfID`, the handler runs in-process without a transport round-trip. Same semaphore bound and same handler context as remote.
- **Caller deadline carried on the wire.** `callreply.envelope.DeadlineNanos` is set from `ctx.Deadline()` on the caller and used to scope the handler's context on the receiver. Handlers no longer get the receiver's local timeout regardless of caller intent.

### Coverage

- `TestDispatchRaceVsClose` — the regression that revealed both the dispatch race and the MemoryTransport race.
- `TestCallSelfShortCircuits` — confirms the in-process path works without routing-table propagation.
- `TestMaxConcurrentHandlersReturnsBusy` — verifies open backpressure.
- `TestCallerDeadlinePropagatesToHandler` — handler observes the caller's deadline within sub-ms skew.

### Not done (still intentional)

- **Transport.Unsubscribe** — same reasons as last round; needs design.
- **callreply RouteTable interface** — same.
- **`json.RawMessage` for `wireMessage.Value`** — only helps when the codec produces valid JSON; complicates the non-JSON codec case. Skipped pending a clearer cost/benefit signal.
- **Per-pod identity / epoch for `P2POptions.OnConnect`** — would change the wire.



All notable changes to this package. Format is loosely Keep a Changelog;
versions correspond to `ProtocolVersion` for wire-breaking changes and a
project SemVer once tagged.

## Unreleased — post-review fixes

### Changed (breaking-ish; pre-1.0)

- `SetIf` renamed to `TrySet`. The "If" naming overstated the
  guarantees; godoc now leads with "this is NOT a distributed CAS."
- `Store.WatchKey(ctx, key)` and `Store.WatchPrefix(ctx, prefix)` methods
  removed. Use `Watch(ctx, podshare.WatchKey("k"))` /
  `Watch(ctx, podshare.WatchPrefix("prefix"))` instead. The
  `WatchOption` pattern composes — add new filters without growing the
  method surface.

### Added

- `ErrBroadcast` — wraps publish failures so callers can detect
  "local applied, broadcast failed" via `errors.Is(err, ErrBroadcast)`.
- `ErrSeedNonEmpty` — `Seed` now refuses to overwrite an already-
  populated store.
- `CloseWithContext(ctx)` — bounded shutdown wait for hung transports.
- `WithMemoryOnError` on `transport.MemoryTransport` — silent drops were
  masking flaky-test causes.

### Fixed

- `Refresh` now collects events under the lock and dispatches after
  release. A 10k-key refresh no longer wedges every reader for ~7ms.
- `Stats()` is now O(1) — keysLive / tombstones counters are
  maintained incrementally instead of iterating the map under RLock.
- `callreply.Endpoint` now stores the inbox prefix as a separate field
  instead of slicing the inbox string at call time.
- `WithMerger` godoc now explicitly warns against re-entrancy (calling
  back into the Store from inside the merger deadlocks).
- Removed dead `marshalWireMessage` wrapper.
- Removed `License: fill in` placeholder from README.

### Coverage

- `TestErrBroadcastWrapsPublishFailure` — failing transport, verify
  local apply + `errors.Is(err, ErrBroadcast)`.
- `TestSnapshotFailureGoesToErrorHandler` — async snapshot failure
  surfaces through `WithErrorHandler`.
- `TestRefreshAdoptsNewSnapshot` — explicit Refresh test (previously
  missing entirely).
- `TestSeedRejectsNonEmpty`.
- `TestStatsTracksKeysIncrementally`.
- `TestCloseWithContextRespectsTimeout` — stuck-transport shutdown.
- `TestP2POnConnectFires` — connect/disconnect callbacks.

### Not done (intentional, with reason)

- **Transport.Unsubscribe.** Touches every transport implementation
  plus callreply; needs design (cancel func vs new interface method
  vs ctx-driven). Deferred to a follow-up.
- **callreply decoupling from podshare.** Routing-table interface
  would let callreply ride on etcd/Redis hash without a Store. Worth
  doing but a real refactor, not a fix.
- **P2P OnConnect epoch / peer identity stability.** Flapping peers
  fire repeated connect/disconnect; users would have to dedupe. Adding
  a stable peer ID + epoch is a wire-format change.

## Unreleased

### Added — correctness

- `ProtocolVersion = 1`. `wireMessage` and `wireSnapshot` carry a `V`
  field; receivers reject messages with higher major versions and
  accept lower versions with default values for missing fields.
- Watch event ordering: `dispatch` now runs inside the data mutex, so
  watchers observe events in the same order writes were applied.
  Non-blocking sends still drop slow consumers, so the lock window is
  bounded.
- Per-node monotonic `Seq` counter (already present) is now the third
  LWW tiebreaker — same-instant same-origin writes are ordered
  deterministically.

### Added — API surface

- `Store.SetMany`, `Store.DeleteMany` — batch writes that are atomic on
  the local view (other readers see either pre-batch or post-batch).
- `Store.Seed(map[string]T)` — load initial state without broadcasting.
- `Store.SetIf(ctx, key, value, prev)` + `Store.Version(key)` —
  versioned best-effort CAS (documented non-atomic across pods).
- `Store.WatchKey(ctx, key)`, `Store.WatchPrefix(ctx, prefix)` —
  server-side-filtered watches that don't consume buffer for misses.
- `Store.Refresh(ctx)` — re-fetch and LWW-merge the transport snapshot,
  intended for use after reconnects.
- `Store.Stats() Stats` — counters: local writes, applied/rejected,
  reads, dispatched events, active/dropped watchers, snapshot count
  and size, live keys, tombstones, GC count.

### Added — options

- `WithLogger(*slog.Logger)` — operational events.
- `WithMerger(func(existing, incoming T) T)` — CRDT-style per-value
  merge replacing LWW for the Set path. Runs under the data mutex.
- `WithGCInterval(time.Duration)` — periodic tombstone compaction tick
  for idle topics (default 5m).

### Added — transports

- `transport.RedisOptions.TLSConfig` — symmetric with the P2P transport.
- `transport.P2POptions.OnConnect` / `OnDisconnect` callbacks for each
  peer connection lifecycle event. Use them to trigger `Store.Refresh`
  after a reconnect.

### Added — quality

- `FuzzReadFrame` for the P2P wire parser (`go test -fuzz=FuzzReadFrame
  -run=^$ ./transport`).
- `Store.Stats` test coverage and a slow-watcher-drop regression test.

### Changed

- `Subscribe` channel lifetimes still tied to transport close. Per-Store
  unsubscribe (without closing the Transport) is intentionally not
  added in this round — see *Deferred* below.

### Deferred (intentional)

- **Sharded data map.** Bench showed `Set` is dominated by JSON
  marshal (~800ns), not the single RWMutex. Sharding adds complexity
  without practical wins at the workload sizes podshare targets. If
  you're hitting 100k+ writes/sec on one store, open an issue.
- **Codec applied to the envelope.** Custom codecs still only affect
  the inner `Value`. Making the envelope swappable adds wire-format
  versioning headaches for marginal payoff.
- **JSON marshal buffer pool.** Benchmarked; lost to the standard
  `json.Marshal` at typical message sizes (encoder + result copy
  overhead exceeded savings). Kept `marshalWireMessage` as a wrapper
  so a future codec swap lands in one place.
- **Automatic re-snapshot on P2P reconnect.** Exposed `Store.Refresh`
  and P2P `OnConnect/OnDisconnect` instead; callers wire them as they
  prefer.

## v0 (initial)

- `Store[T]`, `Event[T]`, LWW with timestamp tiebreaking by origin.
- Transports: Memory, Redis (Pub/Sub + `SET` snapshots), P2P (TCP mesh).
- Async/debounced snapshot persistence; tombstone TTL with GC at
  snapshot time.
- `callreply` subpackage for cross-pod RPC over any transport.
- Examples: basic, redis, p2p, chat-cache, feature-flags, ws-routing.
