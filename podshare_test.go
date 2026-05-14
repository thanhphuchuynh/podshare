package podshare_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type cfg struct {
	Limit int    `json:"limit"`
	Tag   string `json:"tag"`
}

func newPair(t *testing.T) (*podshare.Store[cfg], *podshare.Store[cfg], func()) {
	t.Helper()
	tr := transport.NewMemoryTransport()
	ctx := context.Background()
	a, err := podshare.New[cfg](ctx, "topic", tr, podshare.WithNodeID[cfg]("a"))
	if err != nil {
		t.Fatalf("new a: %v", err)
	}
	b, err := podshare.New[cfg](ctx, "topic", tr, podshare.WithNodeID[cfg]("b"))
	if err != nil {
		_ = a.Close()
		t.Fatalf("new b: %v", err)
	}
	return a, b, func() {
		_ = a.Close()
		_ = b.Close()
		_ = tr.Close()
	}
}

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

func TestSetReplicates(t *testing.T) {
	a, b, cleanup := newPair(t)
	defer cleanup()

	if err := a.Set(context.Background(), "k", cfg{Limit: 100, Tag: "x"}); err != nil {
		t.Fatalf("set: %v", err)
	}

	waitFor(t, func() bool {
		v, ok := b.Get("k")
		return ok && v.Limit == 100 && v.Tag == "x"
	})
}

func TestDeleteReplicates(t *testing.T) {
	a, b, cleanup := newPair(t)
	defer cleanup()
	ctx := context.Background()

	if err := a.Set(ctx, "k", cfg{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, ok := b.Get("k")
		return ok
	})

	if err := a.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, ok := b.Get("k")
		return !ok
	})
}

func TestWatchEmitsEvents(t *testing.T) {
	a, b, cleanup := newPair(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := b.Watch(ctx)

	var got []podshare.Event[cfg]
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range events {
			mu.Lock()
			got = append(got, ev)
			mu.Unlock()
			if len(got) == 2 {
				return
			}
		}
	}()

	if err := a.Set(ctx, "k1", cfg{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := a.Set(ctx, "k2", cfg{Limit: 2}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not see two events")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	for _, ev := range got {
		if ev.Origin != "a" {
			t.Errorf("origin = %q, want a", ev.Origin)
		}
		if ev.Kind != podshare.EventSet {
			t.Errorf("kind = %v, want set", ev.Kind)
		}
	}
}

func TestLastWriteWinsByTimestamp(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	a, err := podshare.New[cfg](ctx, "lww", tr, podshare.WithNodeID[cfg]("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b, err := podshare.New[cfg](ctx, "lww", tr, podshare.WithNodeID[cfg]("b"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Set(ctx, "k", cfg{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	// Ensure b sees it before we overwrite, so timestamps are properly ordered.
	waitFor(t, func() bool {
		_, ok := b.Get("k")
		return ok
	})

	time.Sleep(2 * time.Millisecond)
	if err := b.Set(ctx, "k", cfg{Limit: 2}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		v, ok := a.Get("k")
		return ok && v.Limit == 2
	})
}

func TestLateJoinerHydratesFromSnapshot(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	a, err := podshare.New[cfg](ctx, "snap", tr, podshare.WithNodeID[cfg]("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Set(ctx, "warm", cfg{Limit: 42, Tag: "alpha"}); err != nil {
		t.Fatal(err)
	}
	_ = a.Close()

	c, err := podshare.New[cfg](ctx, "snap", tr, podshare.WithNodeID[cfg]("c"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	v, ok := c.Get("warm")
	if !ok {
		t.Fatal("late joiner missing key from snapshot")
	}
	if v.Limit != 42 || v.Tag != "alpha" {
		t.Fatalf("unexpected value: %+v", v)
	}
}

func TestCloseClosesWatchers(t *testing.T) {
	tr := transport.NewMemoryTransport()
	ctx := context.Background()
	s, err := podshare.New[cfg](ctx, "topic", tr)
	if err != nil {
		t.Fatal(err)
	}

	w := s.Watch(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range w {
		}
	}()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = tr.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher channel did not close after Close")
	}
}

func TestRapidSelfWritesDoNotLose(t *testing.T) {
	// Regression: two same-instant writes from the same node previously
	// lost the second one because the LWW tiebreaker dropped equal-origin
	// equal-timestamp incomings. The Seq counter must order them.
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	s, err := podshare.New[cfg](ctx, "rapid", tr, podshare.WithNodeID[cfg]("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 200
	for i := 0; i < n; i++ {
		if err := s.Set(ctx, "k", cfg{Limit: i}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	v, ok := s.Get("k")
	if !ok {
		t.Fatal("key missing after rapid writes")
	}
	if v.Limit != n-1 {
		t.Fatalf("last write lost: got Limit=%d, want %d", v.Limit, n-1)
	}
}

func TestSlowWatcherIsDropped(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dropErr error
	var dropMu sync.Mutex
	dropped := make(chan struct{}, 1)

	s, err := podshare.New[cfg](ctx, "slow", tr,
		podshare.WithNodeID[cfg]("a"),
		podshare.WithWatchBuffer[cfg](2),
		podshare.WithErrorHandler[cfg](func(e error) {
			dropMu.Lock()
			dropErr = e
			dropMu.Unlock()
			select {
			case dropped <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	slow := s.Watch(ctx)
	fast := s.Watch(ctx)

	// Drain fast in a goroutine so it doesn't get dropped.
	fastCount := 0
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for range fast {
			fastCount++
		}
	}()

	// Don't read slow — buffer (2) fills, then dispatch drops it.
	for i := 0; i < 50; i++ {
		if err := s.Set(ctx, "k", cfg{Limit: i}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-dropped:
	case <-time.After(2 * time.Second):
		t.Fatal("slow watcher was not dropped")
	}

	dropMu.Lock()
	if dropErr == nil {
		t.Fatal("error handler not called")
	}
	dropMu.Unlock()

	// Slow channel must be closed (drain returns nothing more).
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := <-slow; !ok {
			break
		}
	}
	if _, ok := <-slow; ok {
		t.Fatal("slow watcher channel should be closed")
	}

	// The store keeps working after dropping the slow watcher.
	if err := s.Set(ctx, "k2", cfg{Limit: 1}); err != nil {
		t.Fatal(err)
	}

	cancel()
	<-fastDone
	if fastCount == 0 {
		t.Fatal("fast watcher saw no events")
	}
}

func TestTombstoneGCAfterTTL(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	s, err := podshare.New[cfg](ctx, "gc", tr,
		podshare.WithNodeID[cfg]("a"),
		podshare.WithSnapshotInterval[cfg](20*time.Millisecond),
		podshare.WithTombstoneTTL[cfg](50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set(ctx, "k", cfg{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}

	// Wait past TTL, then trigger another snapshot via a fresh write.
	time.Sleep(150 * time.Millisecond)
	if err := s.Set(ctx, "other", cfg{Limit: 99}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)

	// Spin up a late joiner and verify the deleted key is fully gone —
	// not even a tombstone in the snapshot.
	c, err := podshare.New[cfg](ctx, "gc", tr, podshare.WithNodeID[cfg]("c"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, ok := c.Get("k"); ok {
		t.Fatal("late joiner should not see GCed key")
	}
	if v, ok := c.Get("other"); !ok || v.Limit != 99 {
		t.Fatalf("late joiner missing live key, got %+v ok=%v", v, ok)
	}
	if len(c.Keys()) != 1 {
		t.Fatalf("expected 1 live key, got %d", len(c.Keys()))
	}
}

func TestSetManyIsAtomicLocally(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	a, _ := podshare.New[cfg](ctx, "batch", tr, podshare.WithNodeID[cfg]("a"))
	defer a.Close()
	b, _ := podshare.New[cfg](ctx, "batch", tr, podshare.WithNodeID[cfg]("b"))
	defer b.Close()

	if err := a.SetMany(ctx, map[string]cfg{
		"k1": {Limit: 1}, "k2": {Limit: 2}, "k3": {Limit: 3},
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		_, ok1 := b.Get("k1")
		_, ok2 := b.Get("k2")
		_, ok3 := b.Get("k3")
		return ok1 && ok2 && ok3
	})
}

func TestSeedDoesNotBroadcast(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	a, _ := podshare.New[cfg](ctx, "seed", tr, podshare.WithNodeID[cfg]("a"))
	defer a.Close()
	b, _ := podshare.New[cfg](ctx, "seed", tr, podshare.WithNodeID[cfg]("b"))
	defer b.Close()

	if err := a.Seed(map[string]cfg{"only-a": {Limit: 99}}); err != nil {
		t.Fatal(err)
	}

	// b should NOT see a's seed (no broadcast). a should still see it locally.
	time.Sleep(50 * time.Millisecond)
	if _, ok := b.Get("only-a"); ok {
		t.Fatal("seed should not broadcast")
	}
	if v, ok := a.Get("only-a"); !ok || v.Limit != 99 {
		t.Fatal("seed should populate local state")
	}
}

func TestTrySetRespectsVersion(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()
	s, _ := podshare.New[cfg](ctx, "cas", tr, podshare.WithNodeID[cfg]("a"))
	defer s.Close()

	_ = s.Set(ctx, "k", cfg{Limit: 1})
	v1 := s.Version("k")

	ok, err := s.TrySet(ctx, "k", cfg{Limit: 2}, v1)
	if err != nil || !ok {
		t.Fatalf("TrySet with current version failed: ok=%v err=%v", ok, err)
	}

	// Try again with the stale version — should be rejected.
	ok, err = s.TrySet(ctx, "k", cfg{Limit: 999}, v1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("TrySet with stale version should have returned false")
	}
	if got, _ := s.Get("k"); got.Limit != 2 {
		t.Fatalf("value clobbered: got %d, want 2", got.Limit)
	}
}

func TestWatchPrefixFilters(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _ := podshare.New[cfg](ctx, "prefix", tr, podshare.WithNodeID[cfg]("a"))
	defer s.Close()

	wsEvents := s.Watch(ctx, podshare.WatchPrefix("ws:"))
	keyEvents := s.Watch(ctx, podshare.WatchKey("exact"))

	go func() {
		for ev := range wsEvents {
			if !strings.HasPrefix(ev.Key, "ws:") {
				t.Errorf("WatchPrefix delivered %q", ev.Key)
			}
		}
	}()
	go func() {
		for ev := range keyEvents {
			if ev.Key != "exact" {
				t.Errorf("WatchKey delivered %q", ev.Key)
			}
		}
	}()

	_ = s.Set(ctx, "ws:alice", cfg{Limit: 1})
	_ = s.Set(ctx, "ws:bob", cfg{Limit: 2})
	_ = s.Set(ctx, "other", cfg{Limit: 3})
	_ = s.Set(ctx, "exact", cfg{Limit: 4})

	time.Sleep(100 * time.Millisecond)
}

func TestMergerCombinesValues(t *testing.T) {
	type Log struct {
		Entries []string `json:"entries"`
	}
	merge := func(existing, incoming Log) Log {
		return Log{Entries: append(append([]string{}, existing.Entries...), incoming.Entries...)}
	}

	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	a, _ := podshare.New[Log](ctx, "merge", tr,
		podshare.WithNodeID[Log]("a"),
		podshare.WithMerger[Log](merge),
	)
	defer a.Close()
	b, _ := podshare.New[Log](ctx, "merge", tr,
		podshare.WithNodeID[Log]("b"),
		podshare.WithMerger[Log](merge),
	)
	defer b.Close()

	_ = a.Set(ctx, "log", Log{Entries: []string{"first"}})
	waitFor(t, func() bool { _, ok := b.Get("log"); return ok })
	_ = b.Set(ctx, "log", Log{Entries: []string{"second"}})

	waitFor(t, func() bool {
		v, _ := a.Get("log")
		return len(v.Entries) == 2
	})

	got, _ := a.Get("log")
	if len(got.Entries) != 2 || got.Entries[0] != "first" || got.Entries[1] != "second" {
		t.Fatalf("merger lost entries: %+v", got)
	}
}

func TestStatsReportsCounters(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()
	s, _ := podshare.New[cfg](ctx, "stats", tr, podshare.WithNodeID[cfg]("a"))
	defer s.Close()

	for i := 0; i < 5; i++ {
		_ = s.Set(ctx, "k", cfg{Limit: i})
	}
	for i := 0; i < 10; i++ {
		_, _ = s.Get("k")
	}

	st := s.Stats()
	if st.WritesLocal != 5 {
		t.Errorf("WritesLocal = %d, want 5", st.WritesLocal)
	}
	if st.Reads != 10 {
		t.Errorf("Reads = %d, want 10", st.Reads)
	}
	if st.Keys != 1 {
		t.Errorf("Keys = %d, want 1", st.Keys)
	}
}

func TestRejectsHigherProtocolVersion(t *testing.T) {
	// Synthesize a wire message claiming a future protocol version and
	// confirm the receiver discards it instead of corrupting state.
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	var errsMu sync.Mutex
	var errs []error
	s, _ := podshare.New[cfg](ctx, "ver", tr,
		podshare.WithNodeID[cfg]("a"),
		podshare.WithErrorHandler[cfg](func(e error) {
			errsMu.Lock()
			errs = append(errs, e)
			errsMu.Unlock()
		}),
	)
	defer s.Close()

	bogus := []byte(`{"v":99,"kind":"set","key":"k","value":"e30=","origin":"x","timestamp":"2100-01-01T00:00:00Z","seq":1}`)
	if err := tr.Publish(ctx, "ver", bogus); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)

	if _, ok := s.Get("k"); ok {
		t.Fatal("higher-version message should not have been applied")
	}
	errsMu.Lock()
	n := len(errs)
	errsMu.Unlock()
	if n == 0 {
		t.Fatal("expected error report for unsupported version")
	}
}

// failingTransport wraps another transport and forces failures on the
// listed methods, so we can exercise error paths that real transports
// only hit in production.
type failingTransport struct {
	inner       podshare.Transport
	failPublish bool
	failSnap    bool
}

func (f *failingTransport) Publish(ctx context.Context, topic string, msg []byte) error {
	if f.failPublish {
		return errors.New("forced publish failure")
	}
	return f.inner.Publish(ctx, topic, msg)
}
func (f *failingTransport) Subscribe(ctx context.Context, topic string) (<-chan []byte, error) {
	return f.inner.Subscribe(ctx, topic)
}
func (f *failingTransport) Snapshot(ctx context.Context, topic string, data []byte) error {
	if f.failSnap {
		return errors.New("forced snapshot failure")
	}
	return f.inner.Snapshot(ctx, topic, data)
}
func (f *failingTransport) FetchSnapshot(ctx context.Context, topic string) ([]byte, error) {
	return f.inner.FetchSnapshot(ctx, topic)
}
func (f *failingTransport) Close() error { return f.inner.Close() }

func TestErrBroadcastWrapsPublishFailure(t *testing.T) {
	tr := &failingTransport{inner: transport.NewMemoryTransport(), failPublish: true}
	defer tr.Close()

	s, err := podshare.New[cfg](context.Background(), "errbroadcast", tr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Set(context.Background(), "k", cfg{Limit: 1})
	if !errors.Is(err, podshare.ErrBroadcast) {
		t.Fatalf("want ErrBroadcast, got %v", err)
	}
	if v, ok := s.Get("k"); !ok || v.Limit != 1 {
		t.Fatalf("local apply expected after broadcast failure, got %+v ok=%v", v, ok)
	}
}

func TestSnapshotFailureGoesToErrorHandler(t *testing.T) {
	tr := &failingTransport{inner: transport.NewMemoryTransport(), failSnap: true}
	defer tr.Close()

	var errsMu sync.Mutex
	var errs []error
	s, err := podshare.New[cfg](context.Background(), "snapfail", tr,
		podshare.WithSnapshotInterval[cfg](20*time.Millisecond),
		podshare.WithErrorHandler[cfg](func(e error) {
			errsMu.Lock()
			errs = append(errs, e)
			errsMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_ = s.Set(context.Background(), "k", cfg{Limit: 1})
	time.Sleep(120 * time.Millisecond)

	errsMu.Lock()
	defer errsMu.Unlock()
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "snapshot") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected snapshot error in handler, got %v", errs)
	}
}

func TestRefreshAdoptsNewSnapshot(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	s, err := podshare.New[cfg](ctx, "refresh", tr, podshare.WithNodeID[cfg]("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Hand-craft a snapshot that should win LWW (future timestamp) and
	// inject it via the transport directly — simulating a peer's
	// snapshot we'd discover on reconnect.
	future := time.Now().UTC().Add(time.Hour)
	val, _ := json.Marshal(cfg{Limit: 42, Tag: "from-peer"})
	snap, _ := json.Marshal(map[string]any{
		"v": 1,
		"entries": map[string]any{
			"x": map[string]any{
				"value":     val,
				"timestamp": future,
				"origin":    "peer",
				"seq":       100,
			},
		},
	})
	if err := tr.Snapshot(ctx, "refresh", snap); err != nil {
		t.Fatal(err)
	}

	// Watch BEFORE refresh to confirm we see the catch-up event.
	events := s.Watch(ctx)

	if err := s.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	select {
	case ev := <-events:
		if ev.Key != "x" || ev.Value.Limit != 42 || ev.Value.Tag != "from-peer" {
			t.Fatalf("unexpected event: %+v", ev)
		}
		if ev.Origin != "peer" {
			t.Errorf("origin = %q, want peer", ev.Origin)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh did not dispatch event")
	}

	v, ok := s.Get("x")
	if !ok || v.Limit != 42 || v.Tag != "from-peer" {
		t.Fatalf("refresh did not adopt new state: %+v ok=%v", v, ok)
	}
}

func TestSeedRejectsNonEmpty(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()
	s, _ := podshare.New[cfg](ctx, "seed-guard", tr)
	defer s.Close()

	_ = s.Set(ctx, "k", cfg{Limit: 1})
	err := s.Seed(map[string]cfg{"other": {Limit: 99}})
	if !errors.Is(err, podshare.ErrSeedNonEmpty) {
		t.Fatalf("want ErrSeedNonEmpty, got %v", err)
	}
}

func TestStatsTracksKeysIncrementally(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()
	s, _ := podshare.New[cfg](ctx, "stats-inc", tr)
	defer s.Close()

	_ = s.Set(ctx, "k1", cfg{Limit: 1})
	_ = s.Set(ctx, "k2", cfg{Limit: 2})
	if got := s.Stats().Keys; got != 2 {
		t.Fatalf("Keys after 2 sets = %d, want 2", got)
	}
	_ = s.Delete(ctx, "k1")
	st := s.Stats()
	if st.Keys != 1 || st.Tombstones != 1 {
		t.Fatalf("after delete: Keys=%d Tomb=%d, want 1/1", st.Keys, st.Tombstones)
	}
}

func TestCloseWithContextRespectsTimeout(t *testing.T) {
	// A transport whose Snapshot blocks forever simulates a stuck peer.
	stuck := &failingTransport{inner: transport.NewMemoryTransport()}
	// Re-wrap Snapshot to block instead of failing.
	stuckSnap := &blockingSnapshotTransport{
		Transport: stuck.inner,
		block:     make(chan struct{}),
	}
	defer stuckSnap.Transport.Close()
	defer close(stuckSnap.block)

	s, err := podshare.New[cfg](context.Background(), "stuck", stuckSnap,
		podshare.WithSnapshotInterval[cfg](10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Set(context.Background(), "k", cfg{Limit: 1})
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = s.CloseWithContext(ctx)
	if err == nil {
		t.Fatal("expected timeout error from CloseWithContext")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("CloseWithContext took %v, expected ~100ms", time.Since(start))
	}
}

type blockingSnapshotTransport struct {
	podshare.Transport
	block chan struct{}
}

func (b *blockingSnapshotTransport) Snapshot(ctx context.Context, topic string, data []byte) error {
	select {
	case <-b.block:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestDispatchRaceVsClose is the regression for the send-on-closed-channel
// race in dispatch. Pre-fix, this test panics intermittently. Post-fix,
// it passes deterministically: dispatch holds watchersMu through the
// send loop, so close cannot race the send.
func TestDispatchRaceVsClose(t *testing.T) {
	for trial := range 25 {
		tr := transport.NewMemoryTransport()
		ctx, cancel := context.WithCancel(context.Background())

		s, err := podshare.New[cfg](ctx, "race", tr,
			podshare.WithNodeID[cfg](fmt.Sprintf("n-%d", trial)),
			podshare.WithSnapshotInterval[cfg](time.Hour),
			podshare.WithWatchBuffer[cfg](1),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Start a watcher that drains slowly to keep its buffer near full.
		ch := s.Watch(ctx)
		go func() {
			for range ch {
				time.Sleep(10 * time.Microsecond)
			}
		}()

		// Background writer fires events as fast as possible.
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			for i := 0; ; i++ {
				if err := s.Set(ctx, "k", cfg{Limit: i}); err != nil {
					return
				}
			}
		}()

		// Let writes pile up briefly, then close. Without the fix this
		// causes a panic in some fraction of trials.
		time.Sleep(2 * time.Millisecond)
		cancel()
		_ = s.Close()
		_ = tr.Close()
		<-writerDone
	}
}

func TestSetAfterCloseFails(t *testing.T) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	s, err := podshare.New[cfg](ctx, "topic", tr)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "k", cfg{}); err == nil {
		t.Fatal("expected error on Set after Close")
	}
}
