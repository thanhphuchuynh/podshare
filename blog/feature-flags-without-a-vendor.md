---
title: "Feature flag rollouts in Go without a vendor"
published: false
description: A 50-line pattern using sub-millisecond pub/sub and lock-free reads. Sometimes the right answer is the small one.
tags: go, featureflags, distributed, opensource
canonical_url: https://thanhphuchuynh.github.io/podshare/
series: podshare
---

## The problem

Product wants to ship a new checkout flow. Engineering wants a way to roll it out gradually — 1% canary, 10%, 50%, 100% — with an instant rollback if anything breaks.

So you reach for the obvious thing: LaunchDarkly. Or Statsig. Or ConfigCat. Or Unleash if you're feeling self-hosty. Annual contract, SDK in every service, outbound calls to fetch flag state, maybe a websocket for real-time updates.

That's a lot of moving parts for "we want a tiny boolean to flip across four pods at the same time."

For a class of feature-flagging problems — the simple class, which is most of them — you can do this in fifty lines of Go.

## What "rolling out a feature" actually needs

Strip the marketing copy and the core flag system is three things:

1. **A small piece of mutable state** shared across every pod. (`{enabled: true, rollout: 50}`.)
2. **Live updates without restarts.** When you change the rollout from 10 to 50, every pod sees it within a second.
3. **Instant rollback.** Set rollout to 0, or delete the flag entirely.

That's it. Targeting rules per segment, A/B analytics, audit logs — those are real features, but they're separable. You can buy them when you need them. You don't need them on day one.

## The pattern: replicated map + atomic pointer

- One **operator pod** publishes the flag state (could be triggered by an admin endpoint, a CLI, or a git commit).
- Every **worker pod** subscribes via `Watch` and caches the latest state in a process-local `atomic.Pointer[Flag]`.
- Application code reads the pointer with one atomic load — no locks, no network calls, no SDK initialization.

Reads are lock-free and sub-microsecond. Writes broadcast across the cluster in a single Redis pub/sub round trip.

## Building it

I'll use [`podshare`](https://github.com/thanhphuchuynh/podshare) — a small library that gives you "shared map across pods" over any transport (Redis, P2P TCP, in-memory). Bring your own if you have one.

```go
package flags

import (
    "context"
    "hash/fnv"
    "log/slog"
    "sync"
    "sync/atomic"

    "github.com/thanhphuchuynh/podshare"
    "github.com/thanhphuchuynh/podshare/transport"
)

type Flag struct {
    Enabled bool   `json:"enabled"`
    Rollout int    `json:"rollout"` // 0..100
    Note    string `json:"note,omitempty"`
}

type Registry struct {
    store *podshare.Store[Flag]
    // One atomic.Pointer per flag name. Lock-free reads.
    cache sync.Map // map[string]*atomic.Pointer[Flag]
}

func New(ctx context.Context, podID string) (*Registry, error) {
    tr, err := transport.NewRedisTransport(transport.RedisOptions{
        Addr: "redis:6379",
    })
    if err != nil {
        return nil, err
    }

    store, err := podshare.New[Flag](ctx, "feature-flags", tr,
        podshare.WithNodeID[Flag](podID),
        podshare.WithErrorHandler[Flag](func(e error) {
            slog.Error("flags", "err", e)
        }),
    )
    if err != nil {
        return nil, err
    }

    r := &Registry{store: store}

    // Order matters: start Watch BEFORE reading the snapshot, so events
    // arriving in the small window between Snapshot() and the watcher
    // start aren't lost. Both paths converge to the right state.
    go r.watch(ctx)
    for k, v := range store.Snapshot() {
        r.set(k, v)
    }
    return r, nil
}

func (r *Registry) set(name string, f Flag) {
    p, _ := r.cache.LoadOrStore(name, &atomic.Pointer[Flag]{})
    p.(*atomic.Pointer[Flag]).Store(&f)
}

func (r *Registry) clear(name string) {
    if p, ok := r.cache.Load(name); ok {
        p.(*atomic.Pointer[Flag]).Store(nil)
    }
}

func (r *Registry) watch(ctx context.Context) {
    for ev := range r.store.Watch(ctx) {
        switch ev.Kind {
        case podshare.EventSet:
            r.set(ev.Key, ev.Value)
        case podshare.EventDelete:
            r.clear(ev.Key)
        }
    }
}

// IsEnabled — the hot path. One atomic load, zero allocations.
func (r *Registry) IsEnabled(name string) bool {
    p, ok := r.cache.Load(name)
    if !ok {
        return false
    }
    f := p.(*atomic.Pointer[Flag]).Load()
    return f != nil && f.Enabled
}

// RolloutFor — does this user fall under the current rollout percentage?
// FNV-hash the (name, userID) pair, mod 100, compare against the rollout.
func (r *Registry) RolloutFor(name, userID string) bool {
    p, ok := r.cache.Load(name)
    if !ok {
        return false
    }
    f := p.(*atomic.Pointer[Flag]).Load()
    if f == nil || !f.Enabled {
        return false
    }
    if f.Rollout >= 100 {
        return true
    }
    if f.Rollout <= 0 {
        return false
    }
    h := fnv.New32a()
    h.Write([]byte(name + ":" + userID))
    return int(h.Sum32()%100) < f.Rollout
}
```

The operator side is even smaller:

```go
func RollOut(ctx context.Context, store *podshare.Store[Flag], name string, percent int) error {
    return store.Set(ctx, name, Flag{Enabled: true, Rollout: percent})
}

func RollBack(ctx context.Context, store *podshare.Store[Flag], name string) error {
    return store.Delete(ctx, name)
}
```

That's the whole system. Sub-millisecond rollouts, no SDK, no vendor.

## The patterns that matter

Four specific things in this code are worth pointing out.

### 1. Atomic pointer for lock-free reads

`IsEnabled` runs once per request — possibly thousands of times per second per pod. With a typical SDK that's a map lookup behind a `RWMutex`, which is fine, but it's a synchronization point. With `atomic.Pointer[Flag]` and a `sync.Map` keyed by flag name, it's a single atomic load. Roughly 5 nanoseconds, no allocations.

That matters if you're checking flags inside hot loops (request handlers, packet processors).

### 2. Delete sets the pointer to nil, not zero-Flag

This is the subtle one. When `Delete` propagates, the `Watch` handler receives an event where `ev.Value` is the **zero value** of `Flag` — `{Enabled: false, Rollout: 0, Note: ""}`.

If you naively store `&ev.Value`, you'd end up with a non-nil pointer to a "feature is disabled" state. That's the wrong shape: subsequent code can't distinguish "flag never existed" from "flag was rolled back to zero." Always explicitly `Store(nil)` on `EventDelete`.

```go
case podshare.EventDelete:
    r.clear(ev.Key) // atomic.Pointer[Flag].Store(nil)
```

### 3. Watch starts before Snapshot is read

The order is `go r.watch(ctx)` first, then `store.Snapshot()`. If you flip them, there's a race window where an event arrives between the snapshot read and the watcher's first iteration — and that event silently disappears.

Both orderings converge to a consistent state because podshare dispatches watch events under the data lock — but only one of the two orderings handles the race correctly. Watch first.

### 4. `WatchPrefix` for related flags

If you want to subscribe only to a category — say, every flag under `feature.checkout.*`:

```go
events := r.store.Watch(ctx, podshare.WatchPrefix("feature.checkout."))
```

Server-side filtering. Events that don't match never consume the watcher's buffer. Useful if your codebase has hundreds of flags and a given service only cares about a dozen.

## Honest comparison to vendors

| Thing | LaunchDarkly / Statsig | This pattern |
|---|---|---|
| Hot-path read | ~50ns (SDK cache) | ~5ns (atomic.Pointer) |
| Rollout propagation | 1–10s typical | <100ms (Redis pub/sub) |
| User-segment targeting | Built in | DIY hash function |
| A/B analytics | Built in | DIY (your warehouse + event bus) |
| Audit log | Built in | DIY (log every Set/Delete) |
| Web UI | Built in | DIY (your admin tool) |
| Vendor pricing | $$$ at scale | $0 marginal |
| External dependency | Outbound API | Just Redis (which you have) |
| Failure mode | Vendor outage → static defaults | Redis outage → static defaults |

**Vendors charge for the UI, the targeting engine, and the analytics.** If you don't need those, you're paying for nothing.

When you *do* need rich targeting (cohort overrides, gradual rollouts by geography, multi-armed experiments with statistical significance), the build-vs-buy math flips. Until then, fifty lines of Go.

## What this doesn't do (and when to upgrade)

- **No targeting beyond percentage.** If you need "ship to NA but not EU until GDPR sign-off," you write the targeting logic yourself.
- **No experimentation.** A/B testing with statistical significance is a real product, not a config flag. Pay for it when you actually need it.
- **No audit log.** Every `Set`/`Delete` is just a pub/sub message. Log them yourself for compliance trail.
- **No mobile SDKs.** This is server-side. If your iOS app needs flag state, it's a separate API call.

The point isn't that vendors are bad. The point is that "I need to roll out a feature" doesn't always mean "I need a Configuration-as-a-Service contract."

## Try it

The library is at [github.com/thanhphuchuynh/podshare](https://github.com/thanhphuchuynh/podshare). The runnable example for this pattern is at [`examples/feature-flags/`](https://github.com/thanhphuchuynh/podshare/tree/main/examples/feature-flags) — `go run` it and watch live rollouts in your terminal.

There's an animated demo on [the landing page](https://thanhphuchuynh.github.io/podshare/) showing how writes propagate across pods in real time. Same primitives.

---

**TL;DR**: feature flags are a small piece of state shared across pods. If that's all you need, build it. Save the vendor budget for the problems that actually need a vendor.
