package transport

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"
)

// DNSDiscoverer periodically resolves a DNS name and notifies a callback
// of currently-resolved addresses. Designed for Kubernetes headless
// Services where the A records list every Pod IP backing the Service.
//
// Wire it to a P2PTransport via SyncPeers:
//
//	d := &transport.DNSDiscoverer{
//	    Host:     "podshare-headless.default.svc.cluster.local",
//	    Port:     9101,
//	    Self:     net.JoinHostPort(os.Getenv("POD_IP"), "9101"),
//	    Interval: 10 * time.Second,
//	    OnError:  func(err error) { slog.Warn("discovery", "err", err) },
//	}
//	go d.Run(ctx, transport.SyncPeers(tr))
//
// The Run loop owns its own ticker; cancel ctx to stop it.
type DNSDiscoverer struct {
	// Host is the DNS name to resolve. For a Kubernetes headless Service
	// named "podshare-headless" in namespace "default", that's
	// "podshare-headless.default.svc.cluster.local".
	Host string

	// Port is appended to every resolved IP to form a peer address.
	Port int

	// Self is excluded from the discovered set. Typically your own pod's
	// IP:port — set it to net.JoinHostPort(POD_IP, port). Leave empty if
	// you don't need self-filtering (e.g. you're already binding to a
	// different port than peers).
	Self string

	// Interval between DNS resolutions. Default 10s. Trade-off: shorter
	// = faster reaction to scale events, more DNS load.
	Interval time.Duration

	// OnError receives non-fatal DNS errors. Cheap, non-blocking.
	OnError func(error)

	// LookupHost lets tests inject a fake resolver. Default is
	// net.DefaultResolver.LookupHost.
	LookupHost func(ctx context.Context, host string) ([]string, error)
}

// Run blocks, resolving Host every Interval and invoking sync with the
// current peer set. Returns ctx.Err() when ctx is cancelled.
//
// sync is called with the full current set on each tick (not a delta);
// use SyncPeers to reconcile against a P2PTransport, or write your own
// reducer for other discovery sinks.
func (d *DNSDiscoverer) Run(ctx context.Context, sync func(peers []string)) error {
	if d.Host == "" {
		return errors.New("transport: DNSDiscoverer.Host is required")
	}
	if d.Port <= 0 {
		return errors.New("transport: DNSDiscoverer.Port is required")
	}
	if sync == nil {
		return errors.New("transport: DNSDiscoverer.Run: sync is nil")
	}

	interval := d.Interval
	if interval == 0 {
		interval = 10 * time.Second
	}
	lookup := d.LookupHost
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}

	for {
		addrs, err := lookup(ctx, d.Host)
		switch {
		case err != nil && d.OnError != nil:
			d.OnError(err)
		case err == nil:
			peers := make([]string, 0, len(addrs))
			for _, a := range addrs {
				peer := net.JoinHostPort(a, strconv.Itoa(d.Port))
				if peer == d.Self {
					continue
				}
				peers = append(peers, peer)
			}
			sync(peers)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// SyncPeers returns a callback suitable for DNSDiscoverer.Run that
// reconciles a P2PTransport's peer set with the discovered set on every
// tick: peers no longer resolved are removed, new peers are added.
//
// Idempotent. Safe to wire multiple discoverers into the same transport
// if they cover disjoint hosts.
func SyncPeers(t *P2PTransport) func(peers []string) {
	return func(peers []string) {
		wanted := make(map[string]struct{}, len(peers))
		for _, p := range peers {
			wanted[p] = struct{}{}
		}
		for _, existing := range t.Peers() {
			if _, ok := wanted[existing]; !ok {
				t.RemovePeer(existing)
			}
		}
		for p := range wanted {
			t.AddPeer(p)
		}
	}
}
