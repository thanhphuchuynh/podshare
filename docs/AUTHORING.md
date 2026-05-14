# Authoring guides and tutorials for podshare

This is a workbook for *when* and *how* to write narrative docs beyond
godoc. Not a fixed schedule — a checklist to pull from when a real
need shows up.

## The four kinds of doc (Diataxis)

Every doc serves exactly one purpose. Mixing them confuses readers.

| Type | Purpose | Audience moment | podshare status |
|---|---|---|---|
| **Tutorial** | learning by doing | "I'm new — show me" | missing |
| **How-to guide** | task-oriented recipe | "I have a job to do" | partial (examples/ are code-only) |
| **Reference** | comprehensive API truth | "I'm coding — what's the signature" | done (godoc) |
| **Explanation** | concepts, design trade-offs | "I want to understand" | partial (doc.go has alternatives) |

## Recommendation: don't write these speculatively

Library docs ahead of users are guesswork and rot fast. Two rules:

1. **Wait for the second question.** If two different people stumble on
   the same thing, write a how-to. One person — answer directly.
2. **Tutorials only when there's a clear "from zero to value in 20
   minutes" path.** Halfway tutorials are worse than none.

So: **right now**, this directory should stay roughly empty. Add files
when real users surface real questions.

## Sketch of what eventually lives here

When you do write each, here's a starter outline.

### Tutorial template

Single learning arc; the reader follows along start-to-finish.

```markdown
# Tutorial: Build a multi-pod feature flag service in 20 minutes

## What you'll build
[screenshot of the working thing]

## Prerequisites
- Go 1.26+
- Redis running on localhost:6379 (or use the Memory transport — noted inline)

## Step 1: scaffold the project
...

## Step 2: define the Flag type
...

## Step 3: spin up the store
...

## Step 4: watch from another pod
...

## Step 5: roll back via Delete
...

## What you learned
- Set/Watch lifecycle
- How LWW orders concurrent rollouts
- When to use WithErrorHandler

## Next steps
- See [callreply tutorial] for cross-pod RPC
- See [examples/feature-flags](../examples/feature-flags) for the
  complete runnable version
```

### How-to template

Task-oriented. Reader has a goal and wants the shortest path.

```markdown
# How to: recover state after a Redis Pub/Sub disconnect

## When to use this
- Your transport is RedisTransport
- You've detected a connection loss (Sentinel failover, network blip)
- You need missed events caught up

## Steps

1. Detect the reconnect signal in your application code.
2. Call `store.Refresh(ctx)`. It re-fetches the snapshot and
   LWW-merges; adopted entries fire Watch events.
3. Continue normal operation.

## Gotchas
- `Refresh` events arrive in batch; consumers building derived state
  should treat the batch atomically.
- If the Redis snapshot is also stale (e.g., cluster went down and came
  back empty), `Refresh` returns nil — recovery requires application-
  level re-seed.

## Related
- godoc: [Store.Refresh]
- Example: [examples/redis](../examples/redis)
```

### How-to ideas (prioritized when users actually ask)

1. **How to deploy podshare on Kubernetes (P2P + headless Service).**
   The non-trivial setup. Worth writing the moment one user deploys it.
2. **How to write a custom Codec** (msgpack, protobuf).
3. **How to use podshare with WithMerger as a true CRDT** — G-Set of
   message IDs, sort on read. Worth doing because users will reach for
   `WithMerger` thinking it solves list-append (it doesn't).
4. **How to wire podshare into Prometheus** using `Stats()`.
5. **How to detect and recover from a Redis cluster failover.**
6. **How to build a chat-history hot cache pattern end-to-end.**
   `examples/chat-cache` is the code; this would be the narrative.

### Explanation template

Conceptual, no step-by-step. Helps readers form a mental model.

```markdown
# Why podshare uses LWW (and not Raft, CRDTs, or Last-Modified)

## The problem
[set the stage in 1-2 paragraphs]

## What we picked
LWW ordered by (Timestamp, Origin, Seq).

## Why not Raft
[honest trade-off]

## Why not full CRDTs
[honest trade-off — WithMerger is a CRDT seam, not a CRDT impl]

## Consequences for users
- Clock skew matters. Run NTP.
- Concurrent writes are eventually-consistent, never atomic.
- ...
```

## Workflow: writing a doc

1. **Open it as a draft PR before writing.** Title is enough to invite
   feedback. Half of doc authoring is figuring out what *not* to say.
2. **Borrow from the runnable example.** If `examples/foo` exists, the
   how-to should reference it; if not, write the example first.
3. **One example per doc.** Multiple parallel examples make tutorials
   confusing.
4. **Link to godoc.** Don't restate signatures — link to them with
   `[pkg.Symbol]` syntax.
5. **Test the doc.** If it has commands, copy them into a fresh shell
   and run them. Most stale docs are stale because nobody ran them
   recently.

## Publishing narrative docs

Markdown under `docs/` renders directly on GitHub. That's enough for
pre-1.0. If you outgrow it:

| Tool | When to pick it |
|---|---|
| **mkdocs-material** | Minimal config, single-version site, GH Pages publish, ~10 min setup. Default choice for libraries. |
| **Docusaurus** | You have ≥2 supported major versions and need versioned docs. Heavier (React build), longer setup. |
| **GitHub Wiki** | Don't. It's not versioned with the code, drifts immediately. |

Both real options publish via GitHub Pages off a `gh-pages` branch or
the `/docs` folder on `main`. The setup is generic enough that any
mkdocs-material tutorial covers it.

## Quality bar

Before merging a guide, ask:

- Does it follow exactly **one** of the four Diataxis kinds, or is it
  mixed?
- Does each command actually work, copied verbatim?
- Are the links live (godoc symbols exist, example dirs exist)?
- Would you, six months from now, still recognize what this doc is for?

If yes to all four: ship. If not: revise or drop.
