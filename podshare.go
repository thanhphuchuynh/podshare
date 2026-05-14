package podshare

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by methods invoked after Close.
var ErrClosed = errors.New("podshare: store closed")

// ErrBroadcast wraps transport publish failures from Set/Delete/SetMany.
// When errors.Is(err, ErrBroadcast) is true, the local replica has
// already applied the write — only the fan-out to peers failed.
// Callers should decide retry semantics based on this distinction:
// retrying a successful local write produces a write with a fresh
// timestamp/seq, which is safe but not idempotent.
var ErrBroadcast = errors.New("podshare: broadcast failed; local state updated")

// EventKind identifies the shape of a change event.
type EventKind int

const (
	EventSet EventKind = iota
	EventDelete
)

func (k EventKind) String() string {
	switch k {
	case EventSet:
		return "set"
	case EventDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Event is a single change applied to the local replica. Origin is the
// node ID that authored the change, including this node's own ID for
// changes produced locally.
type Event[T any] struct {
	Kind      EventKind
	Key       string
	Value     T
	Origin    string
	Timestamp time.Time
}

// Store is the type-safe replicated map. Construct with New, release
// with Close. All methods are safe for concurrent use.
//
// Conflict resolution is last-write-wins ordered by (Timestamp, Origin,
// Seq). Wall-clock skew between pods will therefore reorder writes —
// run NTP/PTP in production. The third tiebreaker, Seq, is a per-node
// monotonic counter that prevents same-instant same-node writes from
// being silently dropped.
type Store[T any] struct {
	topic     string
	nodeID    string
	transport Transport
	codec     Codec

	seq atomic.Uint64

	mu   sync.RWMutex
	data map[string]entry[T]

	watchersMu sync.Mutex
	watchers   map[*watcher[T]]struct{}

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup

	watchBuffer      int
	snapshotInterval time.Duration
	tombstoneTTL     time.Duration
	gcInterval       time.Duration
	maxClockSkew     time.Duration
	onError          func(error)
	logger           *slog.Logger
	merger           func(existing, incoming T) T

	dirtyCh chan struct{}

	stats storeStats
}

// storeStats holds the atomic counters that back Store.Stats. Counters
// are split intentionally:
//   - writesLocal: how often Set/Delete/SetMany/DeleteMany was invoked
//     on this Store. May exceed writesApplied when LWW rejects.
//   - writesApplied: how often the data map actually changed (covers
//     both local writes and peer-originated applyMessage calls).
//   - writesRejected: how often a local *or* peer write lost LWW.
//   - keysLive / tombstones: maintained incrementally to keep Stats()
//     cheap; ground truth is the data map, asserted in tests.
type storeStats struct {
	writesLocal      atomic.Uint64
	writesApplied    atomic.Uint64
	writesRejected   atomic.Uint64
	reads            atomic.Uint64
	eventsDispatched atomic.Uint64
	watchersDropped  atomic.Uint64
	snapshots        atomic.Uint64
	snapshotBytes    atomic.Int64
	tombstonesGCed   atomic.Uint64
	keysLive         atomic.Int64
	tombstones       atomic.Int64
}

// Stats is a point-in-time snapshot of internal counters. Cheap; safe
// to call from a hot path.
type Stats struct {
	WritesLocal      uint64
	WritesApplied    uint64
	WritesRejected   uint64
	Reads            uint64
	EventsDispatched uint64
	WatchersActive   int
	WatchersDropped  uint64
	Snapshots        uint64
	SnapshotBytes    int64
	Keys             int
	Tombstones       int
	TombstonesGCed   uint64
}

type entry[T any] struct {
	Value     T
	Timestamp time.Time
	Origin    string
	Tombstone bool
	Seq       uint64
}

type watcher[T any] struct {
	ch     chan Event[T]
	cancel chan struct{}
	filter func(key string) bool // nil = match all
}

// New constructs a Store, joins topic, and pulls the latest snapshot from
// peers. The returned Store is ready for reads and writes immediately;
// callers should defer Close to release Store-local goroutines. The
// underlying Transport is owned by the caller — close it after every
// Store using it has been closed.
func New[T any](ctx context.Context, topic string, t Transport, opts ...Option[T]) (*Store[T], error) {
	if t == nil {
		return nil, errors.New("podshare: transport is nil")
	}
	if topic == "" {
		return nil, errors.New("podshare: topic is empty")
	}

	s := &Store[T]{
		topic:            topic,
		transport:        t,
		codec:            JSONCodec{},
		data:             make(map[string]entry[T]),
		watchers:         make(map[*watcher[T]]struct{}),
		closeCh:          make(chan struct{}),
		watchBuffer:      64,
		snapshotInterval: 200 * time.Millisecond,
		tombstoneTTL:     24 * time.Hour,
		gcInterval:       5 * time.Minute,
		maxClockSkew:     1 * time.Minute,
		dirtyCh:          make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt.apply(s)
	}

	if s.nodeID == "" {
		id, err := randomID()
		if err != nil {
			return nil, fmt.Errorf("podshare: generate node id: %w", err)
		}
		s.nodeID = id
	}

	sub, err := t.Subscribe(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("podshare: subscribe %q: %w", topic, err)
	}

	if raw, err := t.FetchSnapshot(ctx, topic); err != nil {
		return nil, fmt.Errorf("podshare: fetch snapshot %q: %w", topic, err)
	} else if len(raw) > 0 {
		if err := s.loadSnapshot(raw); err != nil {
			return nil, fmt.Errorf("podshare: load snapshot %q: %w", topic, err)
		}
	}

	s.wg.Add(2)
	go s.recvLoop(sub)
	go s.snapshotLoop()

	if s.logger != nil {
		s.logger.Info("podshare: store started",
			"topic", s.topic,
			"node", s.nodeID,
			"snapshot_interval", s.snapshotInterval,
			"tombstone_ttl", s.tombstoneTTL,
		)
	}

	return s, nil
}

// Refresh re-fetches the snapshot from the transport and merges it
// into local state under LWW. Use this after detecting a transport
// reconnect (P2P drop/restore, Redis failover) to recover events that
// may have been missed during the disconnection window.
//
// Adopted entries fire Watch events so consumers learn about catch-up
// state through the same channel as live events.
func (s *Store[T]) Refresh(ctx context.Context) error {
	if s.isClosed() {
		return ErrClosed
	}
	if s.logger != nil {
		s.logger.Info("podshare: refresh starting", "topic", s.topic)
	}
	raw, err := s.transport.FetchSnapshot(ctx, s.topic)
	if err != nil {
		return fmt.Errorf("podshare: refresh fetch: %w", err)
	}
	if len(raw) == 0 {
		if s.logger != nil {
			s.logger.Info("podshare: refresh skipped — no snapshot", "topic", s.topic)
		}
		return nil
	}
	if err := s.mergeSnapshot(raw); err != nil {
		return err
	}
	if s.logger != nil {
		s.logger.Info("podshare: refresh complete", "topic", s.topic, "bytes", len(raw))
	}
	return nil
}

// NodeID returns this Store's identifier — useful for filtering own-origin
// events from a Watch channel.
func (s *Store[T]) NodeID() string { return s.nodeID }

// Topic returns the topic this Store is bound to.
func (s *Store[T]) Topic() string { return s.topic }

// Get returns the value stored under key. The boolean is false when the
// key is absent or has been tombstoned.
func (s *Store[T]) Get(key string) (T, bool) {
	s.stats.reads.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Tombstone {
		var zero T
		return zero, false
	}
	return e.Value, true
}

// Keys returns the live (non-tombstoned) keys. Order is undefined.
func (s *Store[T]) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if !e.Tombstone {
			out = append(out, k)
		}
	}
	return out
}

// Snapshot returns a copy of the live key-value pairs.
func (s *Store[T]) Snapshot() map[string]T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]T, len(s.data))
	for k, e := range s.data {
		if !e.Tombstone {
			out[k] = e.Value
		}
	}
	return out
}

// Set writes value under key and broadcasts the change to peers. The
// write is applied locally first; the broadcast is best-effort against
// the transport. A returned error means the broadcast failed — the local
// write has already taken effect. Snapshot persistence is asynchronous:
// it is debounced through the snapshotter goroutine and flushed on Close.
func (s *Store[T]) Set(ctx context.Context, key string, value T) error {
	if s.isClosed() {
		return ErrClosed
	}
	s.stats.writesLocal.Add(1)

	payload, err := s.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("podshare: marshal value for %q: %w", key, err)
	}

	msg := wireMessage{
		V:         ProtocolVersion,
		Kind:      kindSet,
		Key:       key,
		Value:     payload,
		Origin:    s.nodeID,
		Timestamp: time.Now().UTC(),
		Seq:       s.seq.Add(1),
	}

	if !s.applyMessage(msg, value) {
		return nil
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("podshare: encode wire: %w", err)
	}
	if err := s.transport.Publish(ctx, s.topic, raw); err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcast, err)
	}
	s.markDirty()
	return nil
}

// SetMany applies kvs atomically to the local view (other readers see
// either the pre-batch or post-batch state, never partial). Each entry
// is broadcast individually; a transport-level failure on any item is
// returned, but earlier items have already been applied locally and
// (possibly) published.
func (s *Store[T]) SetMany(ctx context.Context, kvs map[string]T) error {
	if s.isClosed() {
		return ErrClosed
	}
	if len(kvs) == 0 {
		return nil
	}
	s.stats.writesLocal.Add(uint64(len(kvs)))

	ts := time.Now().UTC()
	applied := make([]wireMessage, 0, len(kvs))
	events := make([]Event[T], 0, len(kvs))

	s.mu.Lock()
	for k, v := range kvs {
		payload, err := s.codec.Marshal(v)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("podshare: marshal %q: %w", k, err)
		}
		seq := s.seq.Add(1)
		msg := wireMessage{
			V: ProtocolVersion, Kind: kindSet, Key: k, Value: payload,
			Origin: s.nodeID, Timestamp: ts, Seq: seq,
		}
		if !s.lwwAccept(k, msg) {
			s.stats.writesRejected.Add(1)
			continue
		}
		prev, hadPrev := s.data[k]
		prevLive := hadPrev && !prev.Tombstone
		prevTomb := hadPrev && prev.Tombstone
		newValue := v
		if s.merger != nil && prevLive {
			newValue = s.merger(prev.Value, v)
		}
		s.data[k] = entry[T]{Value: newValue, Timestamp: ts, Origin: s.nodeID, Seq: seq}
		s.stats.writesApplied.Add(1)
		s.trackTransition(prevLive, prevTomb, true, false)
		applied = append(applied, msg)
		events = append(events, Event[T]{Kind: EventSet, Key: k, Value: newValue, Origin: s.nodeID, Timestamp: ts})
	}
	s.mu.Unlock()

	// Dispatch outside the data lock so a large batch doesn't wedge
	// readers for the full duration of the fan-out.
	for _, ev := range events {
		s.dispatch(ev)
	}

	for _, msg := range applied {
		raw, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("podshare: encode wire: %w", err)
		}
		if err := s.transport.Publish(ctx, s.topic, raw); err != nil {
			return fmt.Errorf("%w (%q): %v", ErrBroadcast, msg.Key, err)
		}
	}
	s.markDirty()
	return nil
}

// DeleteMany tombstones keys atomically. Same atomicity guarantee as
// SetMany — readers see all-or-none on the local view.
func (s *Store[T]) DeleteMany(ctx context.Context, keys []string) error {
	if s.isClosed() {
		return ErrClosed
	}
	if len(keys) == 0 {
		return nil
	}
	s.stats.writesLocal.Add(uint64(len(keys)))

	ts := time.Now().UTC()
	var zero T
	applied := make([]wireMessage, 0, len(keys))
	events := make([]Event[T], 0, len(keys))

	s.mu.Lock()
	for _, k := range keys {
		seq := s.seq.Add(1)
		msg := wireMessage{
			V: ProtocolVersion, Kind: kindDelete, Key: k,
			Origin: s.nodeID, Timestamp: ts, Seq: seq,
		}
		if !s.lwwAccept(k, msg) {
			s.stats.writesRejected.Add(1)
			continue
		}
		prev, hadPrev := s.data[k]
		prevLive := hadPrev && !prev.Tombstone
		prevTomb := hadPrev && prev.Tombstone
		s.data[k] = entry[T]{Timestamp: ts, Origin: s.nodeID, Seq: seq, Tombstone: true}
		s.stats.writesApplied.Add(1)
		s.trackTransition(prevLive, prevTomb, false, true)
		applied = append(applied, msg)
		events = append(events, Event[T]{Kind: EventDelete, Key: k, Value: zero, Origin: s.nodeID, Timestamp: ts})
	}
	s.mu.Unlock()

	for _, ev := range events {
		s.dispatch(ev)
	}

	for _, msg := range applied {
		raw, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("podshare: encode wire: %w", err)
		}
		if err := s.transport.Publish(ctx, s.topic, raw); err != nil {
			return fmt.Errorf("%w (%q): %v", ErrBroadcast, msg.Key, err)
		}
	}
	s.markDirty()
	return nil
}

// ErrSeedNonEmpty is returned by Seed when the store already holds data.
// Seed is meant to populate a fresh Store before any Set/Watch.
var ErrSeedNonEmpty = errors.New("podshare: Seed called on non-empty store")

// Seed loads initial state without broadcasting. Use it during pod
// startup to hydrate from a database before announcing presence. Entries
// are inserted with timestamp now() and this node's origin/seq so peers
// converge through normal LWW; Seed does NOT publish messages or run
// watchers. Snapshot persistence is triggered once via markDirty.
//
// Seed must be called on an empty Store, before any Set or Watch. It
// returns ErrSeedNonEmpty otherwise — silently overwriting after the
// store has merged peer state is a footgun we'd rather make loud.
func (s *Store[T]) Seed(initial map[string]T) error {
	if s.isClosed() {
		return ErrClosed
	}
	ts := time.Now().UTC()
	s.mu.Lock()
	if len(s.data) > 0 {
		s.mu.Unlock()
		return ErrSeedNonEmpty
	}
	n := uint64(len(initial))
	for k, v := range initial {
		seq := s.seq.Add(1)
		s.data[k] = entry[T]{Value: v, Timestamp: ts, Origin: s.nodeID, Seq: seq}
		s.stats.keysLive.Add(1)
	}
	s.mu.Unlock()
	// Seed is structurally a batch write — count it that way so the
	// Stats invariant (writesApplied >= keysLive on a fresh store) holds.
	s.stats.writesLocal.Add(n)
	s.stats.writesApplied.Add(n)
	s.markDirty()
	return nil
}

// lwwAccept returns true iff a wireMessage would win against the
// current data[key] entry under the LWW comparison. Must be called with
// s.mu held.
func (s *Store[T]) lwwAccept(key string, msg wireMessage) bool {
	existing, ok := s.data[key]
	if !ok {
		return true
	}
	if existing.Timestamp.After(msg.Timestamp) {
		return false
	}
	if existing.Timestamp.Equal(msg.Timestamp) {
		if existing.Origin > msg.Origin {
			return false
		}
		if existing.Origin == msg.Origin && existing.Seq >= msg.Seq {
			return false
		}
	}
	return true
}

// Version identifies a specific revision of a key, useful for
// optimistic-concurrency Set/Delete operations via SetIf/DeleteIf.
//
// Note: distributed CAS over an eventually-consistent transport is
// best-effort. Two pods can both observe Version V, both pass the local
// SetIf check, and both broadcast. LWW on (Timestamp, Origin, Seq) then
// chooses a winner. Use SetIf to guard against local stale reads and
// to short-circuit obviously-out-of-date writes — not as a true lock.
type Version struct {
	Timestamp time.Time
	Origin    string
	Seq       uint64
	Exists    bool
}

// Version returns the current Version for key. The Exists field is
// false when the key is absent or tombstoned.
func (s *Store[T]) Version(key string) Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Tombstone {
		return Version{}
	}
	return Version{Timestamp: e.Timestamp, Origin: e.Origin, Seq: e.Seq, Exists: true}
}

// TrySet writes value under key only if the current local Version
// matches prev. Returns (true, nil) on success, (false, nil) if the
// precondition failed, or an error for transport problems.
//
// IMPORTANT — this is NOT a distributed compare-and-swap. Two pods can
// both observe Version V, both pass their local TrySet check, and both
// broadcast; LWW then picks one winner. Use TrySet to short-circuit
// obviously-stale writes — not as a lock primitive. For coordination
// that must not split-brain, reach for etcd.
//
// Local atomicity is also weak: the version read and the subsequent
// Set are not held under one lock. A concurrent local Set on the same
// key can land between the check and the write, in which case TrySet
// still issues its write (LWW-ordered against the concurrent one).
func (s *Store[T]) TrySet(ctx context.Context, key string, value T, prev Version) (bool, error) {
	if s.isClosed() {
		return false, ErrClosed
	}

	s.mu.RLock()
	cur, ok := s.data[key]
	curV := Version{}
	if ok && !cur.Tombstone {
		curV = Version{Timestamp: cur.Timestamp, Origin: cur.Origin, Seq: cur.Seq, Exists: true}
	}
	s.mu.RUnlock()

	if curV != prev {
		return false, nil
	}
	return true, s.Set(ctx, key, value)
}

// Delete tombstones key locally and broadcasts the deletion. Subsequent
// Get calls return the zero value with ok=false. Tombstones are kept for
// TombstoneTTL so concurrent late writes from peers cannot resurrect a
// deleted key.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	if s.isClosed() {
		return ErrClosed
	}
	s.stats.writesLocal.Add(1)

	msg := wireMessage{
		V:         ProtocolVersion,
		Kind:      kindDelete,
		Key:       key,
		Origin:    s.nodeID,
		Timestamp: time.Now().UTC(),
		Seq:       s.seq.Add(1),
	}

	var zero T
	if !s.applyMessage(msg, zero) {
		return nil
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("podshare: encode wire: %w", err)
	}
	if err := s.transport.Publish(ctx, s.topic, raw); err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcast, err)
	}
	s.markDirty()
	return nil
}

// WatchOption configures a Watch call. Compose with the helpers:
// WatchFilter, WatchKey, WatchPrefix.
type WatchOption func(*watchConfig)

type watchConfig struct {
	filter func(key string) bool
}

// WatchFilter delivers only events whose Key satisfies fn. Filtering is
// server-side — events that don't match are skipped without consuming
// the watcher's buffer.
func WatchFilter(fn func(key string) bool) WatchOption {
	return func(c *watchConfig) { c.filter = fn }
}

// WatchKey delivers only events for the named key.
func WatchKey(key string) WatchOption {
	return WatchFilter(func(k string) bool { return k == key })
}

// WatchPrefix delivers only events whose key starts with prefix.
// Useful for grouping ("ws:", "feature:checkout:", etc.).
func WatchPrefix(prefix string) WatchOption {
	return WatchFilter(func(k string) bool { return strings.HasPrefix(k, prefix) })
}

// Watch returns a channel that receives changes applied to the local
// replica, including changes authored by this node. Filter options
// narrow what's delivered. The channel is closed when ctx is cancelled
// or the Store is closed.
//
// Slow consumers are dropped: if the watcher's buffer is full at
// dispatch time, the channel is closed and an error is reported through
// the configured error handler. Treat a closed channel as "fell
// behind — re-subscribe and call Refresh or Get."
//
// Examples:
//
//	store.Watch(ctx)
//	store.Watch(ctx, podshare.WatchKey("alice"))
//	store.Watch(ctx, podshare.WatchPrefix("ws:"))
func (s *Store[T]) Watch(ctx context.Context, opts ...WatchOption) <-chan Event[T] {
	cfg := watchConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	w := &watcher[T]{
		ch:     make(chan Event[T], s.watchBuffer),
		cancel: make(chan struct{}),
		filter: cfg.filter,
	}

	s.watchersMu.Lock()
	if s.isClosed() {
		s.watchersMu.Unlock()
		close(w.ch)
		return w.ch
	}
	s.watchers[w] = struct{}{}
	s.watchersMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-ctx.Done():
		case <-s.closeCh:
		case <-w.cancel:
		}
		s.removeWatcher(w)
	}()

	return w.ch
}

// Close stops background work and closes every Watch channel. The
// underlying Transport is not closed — callers own its lifetime. Safe
// to call more than once.
//
// Close blocks until background goroutines exit, which includes a final
// snapshot flush via the configured Transport. If the transport may be
// stuck, use CloseWithContext to bound the wait.
func (s *Store[T]) Close() error {
	return s.CloseWithContext(context.Background())
}

// CloseWithContext is like Close but stops waiting once ctx is done.
// Returns ctx.Err() if shutdown didn't complete in time; the store is
// still marked closed (writes return ErrClosed) and watcher channels
// are closed, but the snapshot loop may not have flushed.
func (s *Store[T]) CloseWithContext(ctx context.Context) error {
	var ctxErr error
	s.closeOnce.Do(func() {
		if s.logger != nil {
			s.logger.Info("podshare: store closing", "topic", s.topic, "node", s.nodeID)
		}
		close(s.closeCh)

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			ctxErr = ctx.Err()
			s.reportError(fmt.Errorf("podshare: close timed out: %w", ctxErr))
		}

		// In the normal path, the per-Watch cleanup goroutines already
		// drained s.watchers after closeCh fired (their removeWatcher
		// runs in the same goroutine that wg.Wait was waiting on). In
		// the timeout path, some may still be in flight — we use
		// removeWatcher rather than raw close to inherit its
		// membership check and avoid the double-close panic.
		s.watchersMu.Lock()
		remaining := make([]*watcher[T], 0, len(s.watchers))
		for w := range s.watchers {
			remaining = append(remaining, w)
		}
		s.watchersMu.Unlock()
		for _, w := range remaining {
			s.removeWatcher(w)
		}
	})
	return ctxErr
}

func (s *Store[T]) recvLoop(sub <-chan []byte) {
	defer s.wg.Done()
	for {
		select {
		case <-s.closeCh:
			return
		case raw, ok := <-sub:
			if !ok {
				return
			}
			s.handleRaw(raw)
		}
	}
}

func (s *Store[T]) handleRaw(raw []byte) {
	var msg wireMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		s.reportError(fmt.Errorf("podshare: decode wire: %w", err))
		return
	}
	if msg.V > ProtocolVersion {
		s.reportError(fmt.Errorf("podshare: rejecting wire v=%d (we speak v=%d)", msg.V, ProtocolVersion))
		return
	}
	if msg.Origin == s.nodeID {
		return
	}
	if s.maxClockSkew > 0 {
		skew := time.Since(msg.Timestamp)
		if skew < 0 {
			skew = -skew
		}
		if skew > s.maxClockSkew {
			s.reportError(fmt.Errorf("podshare: clock skew %s from origin %q exceeds %s",
				skew, msg.Origin, s.maxClockSkew))
			// Do not drop the message; LWW still applies. Skew here is a
			// signal to the operator (NTP/PTP misconfigured), not a
			// reason to break replication.
		}
	}
	var value T
	if msg.Kind == kindSet {
		if err := s.codec.Unmarshal(msg.Value, &value); err != nil {
			s.reportError(fmt.Errorf("podshare: decode value for %q: %w", msg.Key, err))
			return
		}
	}
	s.applyMessage(msg, value)
}

// applyMessage runs last-write-wins reconciliation and dispatches the
// resulting Event to watchers. Returns true when the message was applied.
//
// Ordering: incoming wins iff (msg.Timestamp, msg.Origin, msg.Seq) >
// (existing.Timestamp, existing.Origin, existing.Seq). Seq is meaningful
// only when Origin matches — different origins use Origin as the tiebreak.
//
// Dispatch runs while s.mu is held. Because dispatch uses non-blocking
// sends (a slow watcher is dropped, not awaited), holding the lock for
// the duration is cheap and guarantees watchers observe events in the
// same order they were applied to the data map.
func (s *Store[T]) applyMessage(msg wireMessage, value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.data[msg.Key]; ok {
		if existing.Timestamp.After(msg.Timestamp) {
			s.stats.writesRejected.Add(1)
			return false
		}
		if existing.Timestamp.Equal(msg.Timestamp) {
			if existing.Origin > msg.Origin {
				s.stats.writesRejected.Add(1)
				return false
			}
			if existing.Origin == msg.Origin && existing.Seq >= msg.Seq {
				s.stats.writesRejected.Add(1)
				return false
			}
		}
	}

	prev, hadPrev := s.data[msg.Key]
	if s.merger != nil && msg.Kind == kindSet && hadPrev && !prev.Tombstone {
		value = s.merger(prev.Value, value)
	}

	s.data[msg.Key] = entry[T]{
		Value:     value,
		Timestamp: msg.Timestamp,
		Origin:    msg.Origin,
		Tombstone: msg.Kind == kindDelete,
		Seq:       msg.Seq,
	}
	s.stats.writesApplied.Add(1)
	prevLive := hadPrev && !prev.Tombstone
	prevTomb := hadPrev && prev.Tombstone
	nowTomb := msg.Kind == kindDelete
	s.trackTransition(prevLive, prevTomb, !nowTomb, nowTomb)

	kind := EventSet
	if msg.Kind == kindDelete {
		kind = EventDelete
	}
	s.dispatch(Event[T]{
		Kind:      kind,
		Key:       msg.Key,
		Value:     value,
		Origin:    msg.Origin,
		Timestamp: msg.Timestamp,
	})
	return true
}

// dispatch fans an event out to live watchers using non-blocking sends.
// A watcher whose buffer is full is dropped — the store survives, the
// slow consumer learns via channel close.
//
// The watchersMu lock is held through the entire send loop. Without
// this, a concurrent watcher-cleanup goroutine (woken by closeCh or
// ctx.Done) could close(w.ch) between our slice snapshot and our send,
// and the send would panic on "send on closed channel". The slow-
// watcher removal is inlined here for the same reason — calling
// removeWatcher would try to re-acquire the same mutex.
//
// Lock order is s.mu → watchersMu (applyMessage takes s.mu, then calls
// dispatch which takes watchersMu). No other site acquires watchersMu
// while holding s.mu, so no deadlock.
//
// Each watcher carries an optional filter; events that fail the filter
// are skipped without consuming buffer space.
func (s *Store[T]) dispatch(ev Event[T]) {
	s.watchersMu.Lock()
	defer s.watchersMu.Unlock()

	s.stats.eventsDispatched.Add(1)

	var toDrop []*watcher[T]
	for w := range s.watchers {
		if w.filter != nil && !w.filter(ev.Key) {
			continue
		}
		select {
		case w.ch <- ev:
		default:
			toDrop = append(toDrop, w)
		}
	}
	for _, w := range toDrop {
		delete(s.watchers, w)
		close(w.ch)
		close(w.cancel)
		s.stats.watchersDropped.Add(1)
		s.reportError(fmt.Errorf("podshare: dropped slow watcher (buffer=%d full)", cap(w.ch)))
	}
}

func (s *Store[T]) removeWatcher(w *watcher[T]) {
	s.watchersMu.Lock()
	defer s.watchersMu.Unlock()
	if _, ok := s.watchers[w]; !ok {
		return
	}
	delete(s.watchers, w)
	close(w.ch)
	close(w.cancel)
}

// snapshotLoop debounces snapshot writes. A Set/Delete signals dirtyCh;
// the loop waits snapshotInterval before persisting so bursts coalesce
// into one snapshot. A periodic gcInterval ticker also fires so idle
// topics still compact expired tombstones. On Close it flushes any
// pending state and exits.
func (s *Store[T]) snapshotLoop() {
	defer s.wg.Done()

	var gcTick <-chan time.Time
	if s.gcInterval > 0 {
		t := time.NewTicker(s.gcInterval)
		defer t.Stop()
		gcTick = t.C
	}

	for {
		select {
		case <-s.closeCh:
			s.finalFlush()
			return
		case <-s.dirtyCh:
		case <-gcTick:
			s.gcTombstones()
			continue
		}

		timer := time.NewTimer(s.snapshotInterval)
		select {
		case <-s.closeCh:
			timer.Stop()
			s.finalFlush()
			return
		case <-timer.C:
		}

		select {
		case <-s.dirtyCh:
		default:
		}

		if err := s.persistSnapshot(context.Background()); err != nil {
			s.reportError(fmt.Errorf("podshare: snapshot: %w", err))
		}
	}
}

// gcTombstones runs a write-free compaction pass: tombstones older than
// tombstoneTTL are removed from the in-memory map without re-encoding
// the snapshot. The next snapshot will also reflect the change. Called
// on the periodic gcInterval tick.
func (s *Store[T]) gcTombstones() {
	cutoff := time.Now().UTC().Add(-s.tombstoneTTL)
	var gced int64
	s.mu.Lock()
	for k, e := range s.data {
		if e.Tombstone && e.Timestamp.Before(cutoff) {
			delete(s.data, k)
			gced++
		}
	}
	s.mu.Unlock()
	if gced > 0 {
		s.stats.tombstones.Add(-gced)
		s.stats.tombstonesGCed.Add(uint64(gced))
		s.logf("podshare: gc'd %d tombstones (idle)", gced)
		s.markDirty()
	}
}

func (s *Store[T]) markDirty() {
	select {
	case s.dirtyCh <- struct{}{}:
	default:
	}
}

func (s *Store[T]) finalFlush() {
	select {
	case <-s.dirtyCh:
	default:
	}
	if err := s.persistSnapshot(context.Background()); err != nil {
		s.reportError(fmt.Errorf("podshare: final snapshot: %w", err))
	}
}

// mergeSnapshot is the LWW-aware counterpart to loadSnapshot, used by
// Refresh. Snapshot entries that lose LWW against current state are
// skipped; adopted entries fire watch events.
//
// To bound lock-hold time on large snapshots, the merge runs in two
// phases: the data update happens under s.mu (so readers see a
// consistent point-in-time view), then events fan out to watchers
// with the lock released. Watch consumers therefore see Refresh
// events as a burst, possibly interleaved with concurrent normal
// writes — but each individual key's history is still monotonic.
func (s *Store[T]) mergeSnapshot(raw []byte) error {
	var snap wireSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	if snap.V > ProtocolVersion {
		return fmt.Errorf("podshare: snapshot v=%d not supported (we speak v=%d)", snap.V, ProtocolVersion)
	}

	events := make([]Event[T], 0, len(snap.Entries))

	s.mu.Lock()
	for k, e := range snap.Entries {
		kind := kindSet
		if e.Tombstone {
			kind = kindDelete
		}
		msg := wireMessage{Origin: e.Origin, Timestamp: e.Timestamp, Seq: e.Seq, Kind: kind}
		if !s.lwwAccept(k, msg) {
			continue
		}
		var v T
		if !e.Tombstone {
			if err := s.codec.Unmarshal(e.Value, &v); err != nil {
				s.mu.Unlock()
				return fmt.Errorf("decode key %q: %w", k, err)
			}
		}
		prev, hadPrev := s.data[k]
		prevLive := hadPrev && !prev.Tombstone
		prevTomb := hadPrev && prev.Tombstone
		s.data[k] = entry[T]{
			Value: v, Timestamp: e.Timestamp, Origin: e.Origin,
			Tombstone: e.Tombstone, Seq: e.Seq,
		}
		s.stats.writesApplied.Add(1)
		s.trackTransition(prevLive, prevTomb, !e.Tombstone, e.Tombstone)
		evKind := EventSet
		if e.Tombstone {
			evKind = EventDelete
		}
		events = append(events, Event[T]{Kind: evKind, Key: k, Value: v, Origin: e.Origin, Timestamp: e.Timestamp})
	}
	s.mu.Unlock()

	for _, ev := range events {
		s.dispatch(ev)
	}
	return nil
}

func (s *Store[T]) loadSnapshot(raw []byte) error {
	var snap wireSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	if snap.V > ProtocolVersion {
		return fmt.Errorf("podshare: snapshot v=%d not supported (we speak v=%d)", snap.V, ProtocolVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range snap.Entries {
		var v T
		if !e.Tombstone {
			if err := s.codec.Unmarshal(e.Value, &v); err != nil {
				return fmt.Errorf("decode key %q: %w", k, err)
			}
		}
		s.data[k] = entry[T]{
			Value:     v,
			Timestamp: e.Timestamp,
			Origin:    e.Origin,
			Tombstone: e.Tombstone,
			Seq:       e.Seq,
		}
		if e.Tombstone {
			s.stats.tombstones.Add(1)
		} else {
			s.stats.keysLive.Add(1)
		}
	}
	return nil
}

// persistSnapshot serializes the current state and hands it to the
// transport. It also garbage-collects tombstones older than TombstoneTTL,
// dropping them from both the snapshot and the in-memory map.
func (s *Store[T]) persistSnapshot(ctx context.Context) error {
	cutoff := time.Now().UTC().Add(-s.tombstoneTTL)

	s.mu.Lock()
	snap := wireSnapshot{V: ProtocolVersion, Entries: make(map[string]wireSnapshotEntry, len(s.data))}
	var gced int64
	for k, e := range s.data {
		if e.Tombstone && e.Timestamp.Before(cutoff) {
			delete(s.data, k)
			gced++
			continue
		}
		var raw []byte
		if !e.Tombstone {
			b, err := s.codec.Marshal(e.Value)
			if err != nil {
				s.mu.Unlock()
				return fmt.Errorf("encode key %q: %w", k, err)
			}
			raw = b
		}
		snap.Entries[k] = wireSnapshotEntry{
			Value:     raw,
			Timestamp: e.Timestamp,
			Origin:    e.Origin,
			Tombstone: e.Tombstone,
			Seq:       e.Seq,
		}
	}
	s.mu.Unlock()

	body, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := s.transport.Snapshot(ctx, s.topic, body); err != nil {
		return err
	}
	s.stats.snapshots.Add(1)
	s.stats.snapshotBytes.Store(int64(len(body)))
	if gced > 0 {
		s.stats.tombstones.Add(-gced)
		s.stats.tombstonesGCed.Add(uint64(gced))
		s.logf("podshare: gc'd %d tombstones", gced)
	}
	return nil
}

// Stats returns a point-in-time snapshot of internal counters. O(1) —
// all counts are maintained incrementally as writes apply, so calling
// Stats in a tight loop (e.g. a metrics exporter) is cheap.
func (s *Store[T]) Stats() Stats {
	s.watchersMu.Lock()
	watchers := len(s.watchers)
	s.watchersMu.Unlock()

	return Stats{
		WritesLocal:      s.stats.writesLocal.Load(),
		WritesApplied:    s.stats.writesApplied.Load(),
		WritesRejected:   s.stats.writesRejected.Load(),
		Reads:            s.stats.reads.Load(),
		EventsDispatched: s.stats.eventsDispatched.Load(),
		WatchersActive:   watchers,
		WatchersDropped:  s.stats.watchersDropped.Load(),
		Snapshots:        s.stats.snapshots.Load(),
		SnapshotBytes:    s.stats.snapshotBytes.Load(),
		Keys:             int(s.stats.keysLive.Load()),
		Tombstones:       int(s.stats.tombstones.Load()),
		TombstonesGCed:   s.stats.tombstonesGCed.Load(),
	}
}

// trackTransition keeps keysLive and tombstones synchronized with the
// data map. Called once per applied write or compaction with the
// before/after states. Caller passes (live, tombstone) deltas
// implicitly via the booleans.
func (s *Store[T]) trackTransition(prevLive, prevTomb, nowLive, nowTomb bool) {
	if prevLive && !nowLive {
		s.stats.keysLive.Add(-1)
	}
	if !prevLive && nowLive {
		s.stats.keysLive.Add(1)
	}
	if prevTomb && !nowTomb {
		s.stats.tombstones.Add(-1)
	}
	if !prevTomb && nowTomb {
		s.stats.tombstones.Add(1)
	}
}

func (s *Store[T]) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Info(fmt.Sprintf(format, args...))
	}
}

func (s *Store[T]) reportError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

func (s *Store[T]) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

func randomID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

