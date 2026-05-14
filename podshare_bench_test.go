package podshare_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

// BenchmarkSet measures the local write hot path: marshal, LWW, dispatch,
// publish to MemoryTransport. Snapshots are pushed past the bench window
// via a long SnapshotInterval so we don't include serialization here.
func BenchmarkSet(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	s, err := podshare.New[int](context.Background(), "bench", tr,
		podshare.WithSnapshotInterval[int](time.Hour),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		_ = s.Set(ctx, "k", i)
	}
}

// BenchmarkGet measures the read hot path — RLock + map lookup.
func BenchmarkGet(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	s, _ := podshare.New[int](context.Background(), "bench", tr)
	defer s.Close()
	_ = s.Set(context.Background(), "k", 42)

	b.ResetTimer()
	for range b.N {
		_, _ = s.Get("k")
	}
}

// BenchmarkGetParallel measures contention on the RW lock under read load.
func BenchmarkGetParallel(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	s, _ := podshare.New[int](context.Background(), "bench", tr)
	defer s.Close()
	_ = s.Set(context.Background(), "k", 42)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Get("k")
		}
	})
}

// BenchmarkDispatchByWatchers measures how dispatch cost scales with
// the number of attached watchers. Each Set fans out to N channels.
func BenchmarkDispatchByWatchers(b *testing.B) {
	for _, n := range []int{0, 1, 10, 100} {
		b.Run(fmt.Sprintf("watchers=%d", n), func(b *testing.B) {
			tr := transport.NewMemoryTransport()
			defer tr.Close()
			s, _ := podshare.New[int](context.Background(), "bench", tr,
				podshare.WithSnapshotInterval[int](time.Hour),
				podshare.WithWatchBuffer[int](b.N+8),
			)
			defer s.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for j := 0; j < n; j++ {
				ch := s.Watch(ctx)
				go func(c <-chan podshare.Event[int]) {
					for range c {
					}
				}(ch)
			}

			b.ResetTimer()
			for i := range b.N {
				_ = s.Set(ctx, "k", i)
			}
		})
	}
}

// BenchmarkCrossPodPropagation measures end-to-end latency: write on pod
// A, see it on pod B. The reported ns/op is the full round trip through
// the transport's pub/sub.
func BenchmarkCrossPodPropagation(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := podshare.New[int](context.Background(), "bench", tr,
		podshare.WithNodeID[int]("a"),
		podshare.WithSnapshotInterval[int](time.Hour),
	)
	defer a.Close()
	bb, _ := podshare.New[int](context.Background(), "bench", tr,
		podshare.WithNodeID[int]("b"),
		podshare.WithSnapshotInterval[int](time.Hour),
		podshare.WithWatchBuffer[int](8),
	)
	defer bb.Close()

	events := bb.Watch(context.Background())
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		_ = a.Set(ctx, "k", i)
		<-events
	}
}

