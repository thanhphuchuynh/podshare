package podshare

import (
	"log/slog"
	"time"
)

// Option configures a Store at construction. Pass options to New.
type Option[T any] interface {
	apply(*Store[T])
}

type optionFunc[T any] func(*Store[T])

func (f optionFunc[T]) apply(s *Store[T]) { f(s) }

// WithCodec replaces the default JSON codec.
func WithCodec[T any](c Codec) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if c != nil {
			s.codec = c
		}
	})
}

// WithNodeID overrides the auto-generated node identifier. Node IDs must
// be unique per process; collisions break last-write-wins tiebreaking.
func WithNodeID[T any](id string) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if id != "" {
			s.nodeID = id
		}
	})
}

// WithWatchBuffer sets the capacity of channels returned by Watch.
// A watcher whose buffer fills at dispatch time is dropped (channel
// closed); size this to comfortably absorb your peak event burst.
func WithWatchBuffer[T any](n int) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if n >= 0 {
			s.watchBuffer = n
		}
	})
}

// WithSnapshotInterval sets the debounce window between snapshot writes.
// Bursty writes inside the window collapse into a single Transport.Snapshot
// call. Default 200ms. Lower values shorten the catch-up gap for new
// joiners but raise transport pressure.
func WithSnapshotInterval[T any](d time.Duration) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if d > 0 {
			s.snapshotInterval = d
		}
	})
}

// WithTombstoneTTL sets how long deleted keys are retained as tombstones.
// During this window, late-arriving writes for a deleted key are still
// rejected by LWW. After it, tombstones are GCed at the next snapshot.
// Default 24h.
func WithTombstoneTTL[T any](d time.Duration) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if d > 0 {
			s.tombstoneTTL = d
		}
	})
}

// WithErrorHandler installs a callback that receives non-fatal errors:
// undecodable peer messages, snapshot failures, and dropped slow watchers.
// The callback runs on Store-internal goroutines and must be cheap and
// non-blocking. Default discards errors silently.
func WithErrorHandler[T any](fn func(error)) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		s.onError = fn
	})
}

// WithLogger installs a slog.Logger for non-error operational events:
// snapshot writes, tombstone GC, watcher lifecycle. Errors still flow
// through WithErrorHandler. Default nil (no logging).
func WithLogger[T any](l *slog.Logger) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		s.logger = l
	})
}

// WithMerger installs a per-value merge function. When set, every
// applied Set merges the incoming value with the existing value (if any
// non-tombstoned value is present); the result is stored under the
// incoming write's timestamp/seq.
//
// CRDT REQUIREMENTS — read carefully.
//
// The wire format carries the unmerged incoming value, not the
// post-merge result. Each peer runs the merger against its own local
// state. For replicas to converge, the merger MUST form a join
// semilattice over T:
//
//	commutative:   fn(a, b) == fn(b, a)
//	associative:   fn(fn(a, b), c) == fn(a, fn(b, c))
//	idempotent:    fn(a, a) == a
//
// Good fits: G-Set unions, OR-Set merges, monotonic counters, "max"
// over a numeric field, last-writer-wins per nested field where the
// nested timestamps converge under the same rules.
//
// Bad fits — replicas WILL diverge: list append (order depends on
// arrival), "newest by client-supplied timestamp" (skew-sensitive),
// any operation that needs the merged history of inputs.
//
// For an ordered chat-message log, model the value as a G-Set of
// {ID, Timestamp, Content}, deduplicate by ID, and sort on read; do
// NOT use a list-append merger.
//
// Locking: the merger runs while the Store mutex is held. It must not
// call back into the Store (Get/Set/Watch/Refresh/Close) — doing so
// will deadlock. Keep it pure and fast.
func WithMerger[T any](fn func(existing, incoming T) T) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		s.merger = fn
	})
}

// WithGCInterval sets how often the snapshotter wakes up to compact
// expired tombstones even when no writes are happening. Default 5m;
// set to 0 to GC only on writes.
func WithGCInterval[T any](d time.Duration) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		s.gcInterval = d
	})
}

// WithMaxClockSkew sets the threshold above which an incoming peer
// message's timestamp triggers an OnError report. Default 1 minute.
// Set to 0 to disable the check.
//
// LWW depends on wall-clock time, so sustained skew silently reorders
// writes; this option turns the issue into an observable signal
// instead of a mystery. Skewed messages are still applied — replication
// continues, the operator just learns something is wrong.
func WithMaxClockSkew[T any](d time.Duration) Option[T] {
	return optionFunc[T](func(s *Store[T]) {
		if d >= 0 {
			s.maxClockSkew = d
		}
	})
}
