package callreply_test

import (
	"context"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare/callreply"
	"github.com/thanhphuchuynh/podshare/transport"
)

func waitOwner(b *testing.B, ep *callreply.Endpoint, target string) {
	b.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ep.Owner(target); ok {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatal("route never appeared")
}

// BenchmarkCall is the end-to-end RPC round trip over MemoryTransport:
// publish call, dispatch to handler, publish reply, deliver to caller.
func BenchmarkCall(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A"})
	defer a.Close()
	bb, _ := callreply.New(tr, callreply.Options{SelfID: "B"})
	defer bb.Close()

	if err := a.Register("svc", "echo", func(_ context.Context, args []byte) ([]byte, error) {
		return args, nil
	}); err != nil {
		b.Fatal(err)
	}
	waitOwner(b, bb, "svc")

	ctx := context.Background()
	args := []byte(`"hello"`)

	b.ResetTimer()
	for range b.N {
		if _, err := bb.Call(ctx, "svc", "echo", args); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCallParallel measures throughput when many goroutines hammer
// the same target. Each Call gets its own pending-reply slot, so they
// run independently except for shared pubsub fan-out.
func BenchmarkCallParallel(b *testing.B) {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	a, _ := callreply.New(tr, callreply.Options{SelfID: "A"})
	defer a.Close()
	bb, _ := callreply.New(tr, callreply.Options{SelfID: "B"})
	defer bb.Close()

	if err := a.Register("svc", "echo", func(_ context.Context, args []byte) ([]byte, error) {
		return args, nil
	}); err != nil {
		b.Fatal(err)
	}
	waitOwner(b, bb, "svc")

	args := []byte(`"hello"`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := bb.Call(ctx, "svc", "echo", args); err != nil {
				b.Fatal(err)
			}
		}
	})
}
