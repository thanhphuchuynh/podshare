---
title: "How do you push to a WebSocket on a pod you don't own?"
published: false
description: A small Go library and a pattern for cross-pod WebSocket fan-out — without trying to ship file descriptors across machines.
tags: go, distributed, websocket, opensource
canonical_url: https://thanhphuchuynh.github.io/podshare/
series: podshare
---

## The problem

Your chat server runs on four pods behind a load balancer.

Alice opens a WebSocket. The load balancer happens to route her to **pod-1**, so pod-1 now holds a `*websocket.Conn` for her in process memory.

Bob, on another machine, sends Alice a message. His HTTP request hits the load balancer and lands on **pod-3**.

Pod-3 looks up Alice's connection in its local map. **Empty.** Pod-3 doesn't have Alice's WebSocket. Pod-1 does.

How does pod-3 deliver the message?

---

The naive answer is "use a shared cache." Stuff every pod's connections into Redis, look up Alice's connection from anywhere, write to it.

You can't. A `*websocket.Conn` is a Go struct wrapping a TCP file descriptor plus a bunch of mutable state in pod-1's address space. None of that survives serialization. None of it would mean anything on pod-3 even if you could ship it across — the FD is bound to pod-1's kernel, not yours.

You can't replicate the connection. You can only replicate **the knowledge of which pod has it.**

## The pattern: routing table, not connection store

The split looks like this:

| Layer | Where it lives | Replicated? |
|---|---|---|
| `*websocket.Conn` (the actual handle) | only on the owning pod, in process memory | **no — impossible** |
| `userID → podID` (the routing table) | every pod | **yes** |
| Cross-pod message forwarding | per-pod inbox channel | unicast |

When pod-3 wants to push a message to Alice, it doesn't look for her connection. It looks at the routing table, sees `alice → pod-1`, and forwards a small "please send this to Alice" message to pod-1's inbox. Pod-1 picks up the message and writes to its local `alice.Conn`.

What you're sharing across pods is **two bytes of intent**, not the connection itself.

Diagrammatically:

```
Pod-3                       Redis pub/sub                Pod-1
  │                              │                          │
  │   call{id, "ws:alice",       │                          │
  │    "Send", "hello, alice"}  ─┼─►                        │
  │                              │   handlers["ws:alice"]   │
  │                              │     ["Send"](args)       │
  │                              │            ↓             │
  │                              │      conn.WriteMessage() │
  │                              │                          │
  │◄── reply{id, nil/err} ──────┼──                         │
```

## Building it in Go

I built [`podshare`](https://github.com/thanhphuchuynh/podshare) for the routing table half and `podshare/callreply` for the unicast forwarder half. Both ride on a pluggable transport — Redis pub/sub, a TCP mesh, or an in-memory broker for tests.

Here's the WebSocket case end-to-end, in roughly thirty lines:

```go
import (
    "github.com/thanhphuchuynh/podshare/callreply"
    "github.com/thanhphuchuynh/podshare/transport"
)

// One transport, one Endpoint per pod.
tr, _ := transport.NewRedisTransport(transport.RedisOptions{Addr: "redis:6379"})
defer tr.Close()

ep, _ := callreply.New(tr, callreply.Options{SelfID: podName()})
defer ep.Close()

// When a user connects on THIS pod, register a handler for them.
func userConnected(userID string, conn *websocket.Conn) {
    target := "ws:" + userID
    ep.Register(target, "Send", func(ctx context.Context, args []byte) ([]byte, error) {
        return nil, conn.WriteMessage(websocket.TextMessage, args)
    })
}

// When a user disconnects on THIS pod, release the route.
func userDisconnected(userID string) {
    ep.Unregister("ws:" + userID)
}

// Send a message to a user from ANY pod.
func sendToUser(ctx context.Context, userID string, msg []byte) error {
    _, err := ep.Call(ctx, "ws:"+userID, "Send", msg)
    return err
}
```

That's the whole thing.

- `ep.Register` publishes a route claim: "`ws:alice` lives on pod-1." Every other pod in the cluster sees the claim through the underlying replicated map.
- `ep.Call("ws:alice", "Send", msg)` resolves the route, publishes a call envelope to pod-1's inbox, and awaits the reply on its own inbox.
- On pod-1, the registered handler runs, writes to the local `*websocket.Conn`, returns "ok."

When Alice's connection drops, `userDisconnected` removes the route. Subsequent `Call`s return `no route for "ws:alice"` — that's how peers know she's offline.

If Alice reconnects to **pod-3** (sticky session changed, deploy rolled, whatever), pod-3 calls `Register` and the route flips automatically. Any in-flight `Call` from another pod that targets her now lands on pod-3 — no manual migration step.

## The bit that gets confusing: who's actually replicating what?

The routing table is a `map[string]Route` where `Route = {PodID, UpdatedAt}`. Every pod has a complete local copy. When pod-1 writes `alice → pod-1`, the write broadcasts via the transport (Redis pub/sub in our example), and every other pod applies it to its local map.

That's the small thing being replicated. Reads are local — looking up "who owns alice?" is a single map lookup with an `RLock`. The benchmark on my M1 is ~17 nanoseconds.

The connection itself never moves. The call envelope (`{id, target, method, args, reply}`) flies to the owning pod's inbox and the reply flies back. Per-message overhead is one round trip through your existing message bus.

## Sanity-check by killing things

The interesting test of any distributed pattern is: what happens when a pod dies?

- **Pod-1 crashes hard while Alice is connected.** The route `alice → pod-1` is still in the routing table. Subsequent `Call`s land on pod-1's inbox, which now has no subscriber, so the calls time out. Pod-1 restarts but doesn't `Register` Alice (she's not connected to it), and the stale route eventually times out via the routing table's tombstone TTL.

  In practice you want to clean this up faster than 24 hours. The fix is a heartbeat: re-`Register` every N seconds and treat any route whose `UpdatedAt` is older than 2N as dead. Five lines of application code.

- **Network partitions briefly.** Routes set during the partition propagate when the network heals. The library has a `store.Refresh(ctx)` for catch-up after reconnection — call it from your transport's reconnect hook.

- **Alice migrates between pods.** Whichever pod runs `Register` most recently wins by last-write-wins on the routing table. No coordination needed.

## Why this pattern is more useful than it sounds

Once you have a small, eventually-consistent, per-pod replicated map, a whole class of "I just need everyone to know about this" problems collapses into one solution:

- **Feature flags.** Operator pod sets `new_checkout → 50%`; every worker sees it via `Watch` within milliseconds.
- **Rate-limit policies.** All pods agree on the current limit for this user.
- **Presence.** Same shape as the WebSocket routing table — just per-user instead of per-target.
- **Fleet-wide circuit breakers.** One pod trips a flag; the rest back off.
- **Chat-history hot cache.** Replicate the last N messages of active conversations so any pod can answer a follow-up turn without hitting the database.

None of these need a Raft cluster. They need a small, fast, eventually-consistent map that every pod can read locally and any pod can write to.

## What this isn't

Honesty pass:

- **Not durable.** Pub/sub is at-most-once. If you need writes to survive a Redis failover, layer on Redis Streams or NATS JetStream KV.
- **Not strongly consistent.** Two pods writing the same key concurrently get LWW with a timestamp + node-ID + sequence tiebreaker — deterministic but not linearizable. If you need locks or leader election, use etcd.
- **Not for huge datasets.** Every pod holds the full map. Fine for routing tables (thousands of entries), wrong for terabytes (use sharded storage).
- **Order-sensitive merges need care.** If you're tempted to use the merger hook for "append to a list" — don't. Peers will diverge. Model the value as a set of `{ID, Timestamp, Content}` and sort on read.

## Try it

Repo: [github.com/thanhphuchuynh/podshare](https://github.com/thanhphuchuynh/podshare).

There's an [animated demo on the landing page](https://thanhphuchuynh.github.io/podshare/) that shows the routing/replication in real time — click any pod to write a key, watch it propagate; toggle a pod offline and back, watch it snapshot-hydrate from peers when it returns.

A runnable WebSocket-routing example lives at `examples/ws-routing/` and builds on the snippet above (with a fake connection so you can `go run` it without a browser):

```sh
go run github.com/thanhphuchuynh/podshare/examples/ws-routing@latest
```

If the pattern is useful — or if I missed an obvious gotcha — I'd love to hear about it.

---

**TL;DR**: don't share the WebSocket. Share the route. The WebSocket lives where it lives; everyone else asks the pod that owns it to write the message. One small map gets replicated; one small RPC carries the call.
