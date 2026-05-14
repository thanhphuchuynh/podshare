package podshare

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// noopTransport lets us isolate persistSnapshot cost from real I/O.
type noopTransport struct{}

func (noopTransport) Publish(context.Context, string, []byte) error      { return nil }
func (noopTransport) Subscribe(context.Context, string) (<-chan []byte, error) {
	ch := make(chan []byte)
	return ch, nil
}
func (noopTransport) Snapshot(context.Context, string, []byte) error       { return nil }
func (noopTransport) FetchSnapshot(context.Context, string) ([]byte, error) { return nil, nil }
func (noopTransport) Close() error                                          { return nil }

// BenchmarkPersistSnapshot measures the snapshot-build cost at varying
// key counts. This is the work that the async snapshotter performs on
// every debounce tick; it's also the per-write cost users would pay if
// they shipped without WithSnapshotInterval.
func BenchmarkPersistSnapshot(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			s, err := New[int](context.Background(), "bench", noopTransport{},
				WithSnapshotInterval[int](time.Hour),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()

			ctx := context.Background()
			for j := range n {
				_ = s.Set(ctx, fmt.Sprintf("k-%d", j), j)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				if err := s.persistSnapshot(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
