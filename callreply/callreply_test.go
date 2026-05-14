package callreply_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare/callreply"
	"github.com/thanhphuchuynh/podshare/transport"
)

func waitFor(t *testing.T, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if want() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func TestCallReachesOwner(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, err := callreply.New(tr, callreply.Options{SelfID: "pod-A"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := callreply.New(tr, callreply.Options{SelfID: "pod-B"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// pod-A registers a handler that echoes.
	if err := a.Register("ws:alice", "Send", func(_ context.Context, args []byte) ([]byte, error) {
		var s string
		if err := json.Unmarshal(args, &s); err != nil {
			return nil, err
		}
		out, _ := json.Marshal("got:" + s)
		return out, nil
	}); err != nil {
		t.Fatal(err)
	}

	// pod-B sees the route eventually.
	waitFor(t, func() bool {
		_, ok := b.Owner("ws:alice")
		return ok
	})

	// pod-B calls the handler on pod-A.
	args, _ := json.Marshal("hello")
	res, err := b.Call(context.Background(), "ws:alice", "Send", args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	var got string
	if err := json.Unmarshal(res, &got); err != nil {
		t.Fatal(err)
	}
	if got != "got:hello" {
		t.Fatalf("got %q, want %q", got, "got:hello")
	}
}

func TestNoRouteFails(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, err := callreply.New(tr, callreply.Options{SelfID: "pod-A"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	_, err = a.Call(context.Background(), "ws:nobody", "Send", nil)
	if err == nil {
		t.Fatal("expected error for missing route")
	}
}

func TestHandlerErrorPropagates(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A"})
	defer a.Close()
	b, _ := callreply.New(tr, callreply.Options{SelfID: "B"})
	defer b.Close()

	if err := a.Register("svc", "fail", func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		_, ok := b.Owner("svc")
		return ok
	})

	_, err := b.Call(context.Background(), "svc", "fail", nil)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v, want error 'boom'", err)
	}
}

func TestRouteMigratesOnReregister(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A"})
	defer a.Close()
	b, _ := callreply.New(tr, callreply.Options{SelfID: "B"})
	defer b.Close()
	c, _ := callreply.New(tr, callreply.Options{SelfID: "C"})
	defer c.Close()

	hits := make(chan string, 4)
	regHandler := func(label string) callreply.Handler {
		return func(_ context.Context, _ []byte) ([]byte, error) {
			hits <- label
			return []byte(`"ok"`), nil
		}
	}

	if err := a.Register("svc", "ping", regHandler("A")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := c.Owner("svc"); return ok })

	if _, err := c.Call(context.Background(), "svc", "ping", nil); err != nil {
		t.Fatal(err)
	}
	if got := <-hits; got != "A" {
		t.Fatalf("first call hit %s, want A", got)
	}

	// Migrate ownership: A unregisters, B registers.
	_ = a.Unregister("svc")
	if err := b.Register("svc", "ping", regHandler("B")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		owner, ok := c.Owner("svc")
		return ok && owner == "B"
	})

	if _, err := c.Call(context.Background(), "svc", "ping", nil); err != nil {
		t.Fatal(err)
	}
	if got := <-hits; got != "B" {
		t.Fatalf("after migration call hit %s, want B", got)
	}
}

func TestCallSelfShortCircuits(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A"})
	defer a.Close()

	calls := 0
	if err := a.Register("local-svc", "ping", func(_ context.Context, args []byte) ([]byte, error) {
		calls++
		return args, nil
	}); err != nil {
		t.Fatal(err)
	}

	// Self call: no need to wait for routing to converge, the local
	// handler already exists.
	res, err := a.Call(context.Background(), "local-svc", "ping", []byte(`"hi"`))
	if err != nil {
		t.Fatalf("self-call: %v", err)
	}
	if string(res) != `"hi"` {
		t.Fatalf("got %q want %q", res, `"hi"`)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
}

func TestMaxConcurrentHandlersReturnsBusy(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	// Cap of 1 + a blocker handler => second call gets "endpoint busy".
	a, _ := callreply.New(tr, callreply.Options{SelfID: "A", MaxConcurrentHandlers: 1})
	defer a.Close()
	b, _ := callreply.New(tr, callreply.Options{SelfID: "B"})
	defer b.Close()

	release := make(chan struct{})
	defer close(release)
	if err := a.Register("svc", "block", func(_ context.Context, _ []byte) ([]byte, error) {
		<-release
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := b.Owner("svc"); return ok })

	// Fire the blocker. Don't wait for it to return.
	firstDone := make(chan error, 1)
	go func() {
		_, err := b.Call(context.Background(), "svc", "block", nil)
		firstDone <- err
	}()

	// Give the blocker a moment to acquire the semaphore slot.
	time.Sleep(50 * time.Millisecond)

	// Second call should immediately see "endpoint busy".
	_, err := b.Call(context.Background(), "svc", "block", nil)
	if err == nil || err.Error() != "endpoint busy" {
		t.Fatalf("got err=%v, want 'endpoint busy'", err)
	}
}

func TestCallerDeadlinePropagatesToHandler(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A", CallTimeout: 5 * time.Second})
	defer a.Close()
	b, _ := callreply.New(tr, callreply.Options{SelfID: "B", CallTimeout: 5 * time.Second})
	defer b.Close()

	if err := a.Register("svc", "echodl", func(ctx context.Context, _ []byte) ([]byte, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("no deadline")
		}
		return []byte(dl.Format(time.RFC3339Nano)), nil
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := b.Owner("svc"); return ok })

	deadline := time.Now().Add(200 * time.Millisecond).UTC().Truncate(time.Microsecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	res, err := b.Call(ctx, "svc", "echodl", nil)
	if err != nil {
		t.Fatal(err)
	}
	gotDeadline, err := time.Parse(time.RFC3339Nano, string(res))
	if err != nil {
		t.Fatalf("parse handler deadline: %v", err)
	}
	skew := gotDeadline.Sub(deadline).Abs()
	if skew > 5*time.Millisecond {
		t.Fatalf("handler saw deadline %v, want close to %v (skew %v)", gotDeadline, deadline, skew)
	}
}

func TestCallTimeoutOnUnresponsiveHandler(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{
		SelfID:      "A",
		CallTimeout: 100 * time.Millisecond,
	})
	defer a.Close()
	b, _ := callreply.New(tr, callreply.Options{
		SelfID:      "B",
		CallTimeout: 100 * time.Millisecond,
	})
	defer b.Close()

	block := make(chan struct{})
	defer close(block)

	if err := a.Register("svc", "wait", func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-block:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := b.Owner("svc"); return ok })

	start := time.Now()
	_, err := b.Call(context.Background(), "svc", "wait", nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("timeout took too long: %v", time.Since(start))
	}
}
