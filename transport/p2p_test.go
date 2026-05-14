package transport_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type kv struct {
	N int `json:"n"`
}

func TestP2PReplicates(t *testing.T) {
	t.Parallel()

	ta, err := transport.NewP2PTransport(transport.P2POptions{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()

	tb, err := transport.NewP2PTransport(transport.P2POptions{
		Listen:            "127.0.0.1:0",
		Peers:             []string{ta.Addr()},
		ReconnectInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()

	// Give the mesh a moment to settle.
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	a, err := podshare.New[kv](ctx, "p2p", ta, podshare.WithNodeID[kv]("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b, err := podshare.New[kv](ctx, "p2p", tb, podshare.WithNodeID[kv]("b"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Set(ctx, "x", kv{N: 7}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := b.Get("x"); ok && v.N == 7 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("b never saw the update from a")
}

func TestP2POnConnectFires(t *testing.T) {
	t.Parallel()

	ta, err := transport.NewP2PTransport(transport.P2POptions{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()

	var mu sync.Mutex
	connects, disconnects := 0, 0
	connected := make(chan struct{}, 1)

	tb, err := transport.NewP2PTransport(transport.P2POptions{
		Listen:            "127.0.0.1:0",
		Peers:             []string{ta.Addr()},
		ReconnectInterval: 50 * time.Millisecond,
		OnConnect: func(remote string, inbound bool) {
			mu.Lock()
			connects++
			mu.Unlock()
			select {
			case connected <- struct{}{}:
			default:
			}
		},
		OnDisconnect: func(remote string, inbound bool) {
			mu.Lock()
			disconnects++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnect did not fire within timeout")
	}

	_ = tb.Close()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if connects < 1 {
		t.Fatalf("connects = %d, want ≥1", connects)
	}
	if disconnects < 1 {
		t.Fatalf("disconnects = %d, want ≥1", disconnects)
	}
}

func TestP2PSnapshotHydration(t *testing.T) {
	t.Parallel()

	ta, err := transport.NewP2PTransport(transport.P2POptions{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()

	// Pre-load a snapshot directly so a fresh joiner can pull it.
	snap, _ := json.Marshal(map[string]any{
		"entries": map[string]any{
			"k": map[string]any{
				"value":     []byte(`{"n":99}`),
				"timestamp": time.Now().UTC(),
				"origin":    "seed",
			},
		},
	})
	if err := ta.Snapshot(context.Background(), "p2psnap", snap); err != nil {
		t.Fatal(err)
	}

	tb, err := transport.NewP2PTransport(transport.P2POptions{
		Listen:            "127.0.0.1:0",
		Peers:             []string{ta.Addr()},
		ReconnectInterval: 100 * time.Millisecond,
		SnapshotTimeout:   1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()

	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	b, err := podshare.New[kv](ctx, "p2psnap", tb)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	v, ok := b.Get("k")
	if !ok {
		t.Fatal("expected key from peer snapshot")
	}
	if v.N != 99 {
		t.Fatalf("got %+v, want N=99", v)
	}
}
