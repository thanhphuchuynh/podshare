// Feature flags with live propagation. One operator pod publishes flag
// changes; every other pod reacts via Watch with millisecond latency.
// Demonstrates WithErrorHandler and the recommended "treat closed
// channel as 'fell behind'" pattern.
//
//	go run ./examples/feature-flags
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type Flag struct {
	Enabled bool   `json:"enabled"`
	Rollout int    `json:"rollout"` // percentage 0..100
	Note    string `json:"note,omitempty"`
}

func main() {
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	operator := mustNew(ctx, tr, "operator")
	defer operator.Close()

	worker := mustNew(ctx, tr, "worker-1")
	defer worker.Close()

	// Worker listens to flag changes and updates an in-process atomic
	// pointer. Application code reads the pointer with no locking.
	var current atomic.Pointer[Flag]

	go func() {
		events := worker.Watch(ctx)
		for ev := range events {
			fmt.Printf("[worker] flag %q -> %s by %s (rollout=%d%%, %q)\n",
				ev.Key, ev.Kind, ev.Origin, ev.Value.Rollout, ev.Value.Note)
			f := ev.Value
			current.Store(&f)
		}
		// Channel closed: either ctx cancelled or we fell behind. Real
		// applications should re-Watch and re-snapshot via Get here.
		fmt.Println("[worker] watch channel closed")
	}()

	// Operator rolls out a flag in stages.
	stages := []Flag{
		{Enabled: true, Rollout: 1, Note: "canary"},
		{Enabled: true, Rollout: 10, Note: "early access"},
		{Enabled: true, Rollout: 50, Note: "wide"},
		{Enabled: true, Rollout: 100, Note: "GA"},
	}
	for _, s := range stages {
		if err := operator.Set(ctx, "new-checkout", s); err != nil {
			log.Printf("set: %v", err)
		}
		time.Sleep(150 * time.Millisecond)

		// Worker's atomic pointer reflects the latest flag — show it.
		if f := current.Load(); f != nil {
			fmt.Printf("  ↳ application code reads rollout=%d%% (lock-free)\n", f.Rollout)
		}
	}

	// Roll back via Delete — workers see ev.Kind == EventDelete and can
	// clear their atomic pointer.
	if err := operator.Delete(ctx, "new-checkout"); err != nil {
		log.Printf("delete: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func mustNew(ctx context.Context, tr podshare.Transport, nodeID string) *podshare.Store[Flag] {
	s, err := podshare.New[Flag](ctx, "feature-flags", tr,
		podshare.WithNodeID[Flag](nodeID),
		podshare.WithWatchBuffer[Flag](256),
		podshare.WithErrorHandler[Flag](func(e error) {
			log.Printf("[%s] %v", nodeID, e)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	return s
}
