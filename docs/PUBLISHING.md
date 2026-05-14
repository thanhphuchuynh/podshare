# Publishing podshare docs

For a Go library, **pkg.go.dev is the canonical reference**. You already
have everything it needs: godoc comments on every exported symbol, the
README, runnable examples. No site generator. Just push and tag.

## One-time setup

```sh
# 1. Push to GitHub (if not already)
git push -u origin main

# 2. Tag the release
git tag v0.1.0
git push origin v0.1.0

# 3. Force pkg.go.dev to fetch and index (instead of waiting for its next scan)
GOPROXY=https://proxy.golang.org \
  go list -m github.com/thanhphuchuynh/podshare@v0.1.0

# Or just visit the URL in a browser — first hit triggers indexing:
#   https://pkg.go.dev/github.com/thanhphuchuynh/podshare
```

After a minute or two:

- Main package — <https://pkg.go.dev/github.com/thanhphuchuynh/podshare>
- `callreply` — <https://pkg.go.dev/github.com/thanhphuchuynh/podshare/callreply>
- `transport` — <https://pkg.go.dev/github.com/thanhphuchuynh/podshare/transport>

## Local preview before publishing

`pkgsite` is the modern viewer (replaces the deprecated `godoc` server).
Renders identically to pkg.go.dev — catches missing docstrings, broken
examples, and formatting issues before they're public.

```sh
go install golang.org/x/pkgsite/cmd/pkgsite@latest
pkgsite -http=:8080 .
# open http://localhost:8080/github.com/thanhphuchuynh/podshare
```

## What pkg.go.dev renders

| Source | Rendered as |
|---|---|
| `README.md` | Package overview at the top of the page |
| `// Doc` comments on exported symbols | Symbol reference |
| `func ExampleFoo()` in `*_test.go` | Inline example under `Foo`'s docs, with copy + run buttons |
| Sub-packages (`callreply/`, `transport/`) | Linked from the main page |

## What pkg.go.dev does NOT render

- `examples/*` directories — these are runnable programs (`go run
  ./examples/basic`), NOT Go-style examples. Useful for users, invisible
  to pkg.go.dev. Keep them; just don't expect them to surface there.
- Narrative docs (`docs/`, wiki) — for those, GitHub renders the
  markdown directly when users browse the repo.

## Improving what renders (best ROI in order)

1. **`Example*` functions** in `*_test.go`. These appear inline on
   pkg.go.dev with run/test buttons; `go test` verifies the `// Output:`
   line still matches. Highest impact for time spent. Example:

   ```go
   func ExampleStore_Set() {
       tr := transport.NewMemoryTransport()
       defer tr.Close()
       s, _ := podshare.New[int](context.Background(), "demo", tr)
       defer s.Close()
       s.Set(context.Background(), "answer", 42)
       v, _ := s.Get("answer")
       fmt.Println(v)
       // Output: 42
   }
   ```

2. **Sub-package `doc.go` files.** Top-level `doc.go` exists; consider
   adding one for `callreply/` and `transport/` so each section has its
   own overview.

3. **Cross-links in godoc** with `[Name]` / `[pkg.Name]` syntax —
   Go 1.19+. Renders as clickable links on pkg.go.dev.

## Verify in CI

Add to the existing test workflow:

```yaml
- name: Verify documentation
  run: |
    go vet ./...
    go test ./...   # Example* functions check their Output: lines
```

Stale godoc compiles; stale `Example*` output fails the test. CI catches
the second.

## When to look beyond pkg.go.dev

For a library, pkg.go.dev + README + examples is the standard. Reach
for narrative docs (`docs/`, GitHub Pages) only when you start
answering the same user question twice. See `docs/AUTHORING.md` for
guidance on when and how.
