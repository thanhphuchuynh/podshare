package transport_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare/transport"
)

func TestAddRemovePeerIdempotent(t *testing.T) {
	t.Parallel()
	tr, err := transport.NewP2PTransport(transport.P2POptions{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	if !tr.AddPeer("127.0.0.1:65111") {
		t.Fatal("first AddPeer should return true")
	}
	if tr.AddPeer("127.0.0.1:65111") {
		t.Fatal("repeat AddPeer should return false")
	}
	peers := tr.Peers()
	if len(peers) != 1 || peers[0] != "127.0.0.1:65111" {
		t.Fatalf("Peers() = %v, want [127.0.0.1:65111]", peers)
	}

	if !tr.RemovePeer("127.0.0.1:65111") {
		t.Fatal("first RemovePeer should return true")
	}
	if tr.RemovePeer("127.0.0.1:65111") {
		t.Fatal("repeat RemovePeer should return false")
	}
	if got := tr.Peers(); len(got) != 0 {
		t.Fatalf("Peers() after remove = %v, want []", got)
	}
}

func TestSyncPeersReconciles(t *testing.T) {
	t.Parallel()
	tr, err := transport.NewP2PTransport(transport.P2POptions{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	sync := transport.SyncPeers(tr)

	// First tick: add two peers.
	sync([]string{"127.0.0.1:65211", "127.0.0.1:65212"})
	if got := sorted(tr.Peers()); !equal(got, []string{"127.0.0.1:65211", "127.0.0.1:65212"}) {
		t.Fatalf("after first sync: %v", got)
	}

	// Second tick: one peer goes away, one stays, one is new.
	sync([]string{"127.0.0.1:65212", "127.0.0.1:65213"})
	if got := sorted(tr.Peers()); !equal(got, []string{"127.0.0.1:65212", "127.0.0.1:65213"}) {
		t.Fatalf("after second sync: %v", got)
	}

	// Empty tick: all peers removed.
	sync(nil)
	if got := tr.Peers(); len(got) != 0 {
		t.Fatalf("after empty sync: %v", got)
	}
}

func TestDNSDiscovererInvokesSyncWithResolvedPeers(t *testing.T) {
	t.Parallel()

	// Inject a fake resolver so the test doesn't depend on real DNS.
	resolved := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

	var (
		seenMu sync.Mutex
		seen   [][]string
		got    = make(chan struct{}, 1)
	)
	syncFn := func(peers []string) {
		seenMu.Lock()
		defer seenMu.Unlock()
		seen = append(seen, append([]string(nil), peers...))
		select {
		case got <- struct{}{}:
		default:
		}
	}

	d := &transport.DNSDiscoverer{
		Host:     "podshare.svc",
		Port:     9100,
		Self:     "10.0.0.2:9100", // we are 10.0.0.2; expect it filtered out
		Interval: 50 * time.Millisecond,
		LookupHost: func(_ context.Context, host string) ([]string, error) {
			if host != "podshare.svc" {
				return nil, errors.New("wrong host")
			}
			return resolved, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go d.Run(ctx, syncFn)

	select {
	case <-got:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("sync was not invoked")
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) == 0 {
		t.Fatal("no sync calls")
	}
	first := sorted(seen[0])
	want := []string{"10.0.0.1:9100", "10.0.0.3:9100"}
	if !equal(first, want) {
		t.Fatalf("first sync peers = %v, want %v (Self should be filtered)", first, want)
	}
}

func TestDNSDiscovererReportsLookupErrors(t *testing.T) {
	t.Parallel()
	var (
		errsMu sync.Mutex
		errs   []error
		got    = make(chan struct{}, 1)
	)
	d := &transport.DNSDiscoverer{
		Host:     "nope.svc",
		Port:     9100,
		Interval: 30 * time.Millisecond,
		OnError: func(err error) {
			errsMu.Lock()
			defer errsMu.Unlock()
			errs = append(errs, err)
			select {
			case got <- struct{}{}:
			default:
			}
		},
		LookupHost: func(_ context.Context, _ string) ([]string, error) {
			return nil, errors.New("nxdomain")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = d.Run(ctx, func(_ []string) {})

	select {
	case <-got:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("OnError was never called")
	}
	errsMu.Lock()
	defer errsMu.Unlock()
	if len(errs) == 0 {
		t.Fatal("expected at least one error")
	}
}

// --- helpers ----------------------------------------------------------

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
