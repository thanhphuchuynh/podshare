// Basic example: two Stores share state through an in-process MemoryTransport.
//
//	go run ./examples/basic
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type Config struct {
	RateLimit    int             `json:"rate_limit"`
	FeatureFlags map[string]bool `json:"feature_flags"`
}

func main() {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	ctx := context.Background()
	pod1, err := podshare.New[Config](ctx, "rate-limits", tr, podshare.WithNodeID[Config]("pod-1"))
	if err != nil {
		log.Fatal(err)
	}
	defer pod1.Close()

	pod2, err := podshare.New[Config](ctx, "rate-limits", tr, podshare.WithNodeID[Config]("pod-2"))
	if err != nil {
		log.Fatal(err)
	}
	defer pod2.Close()

	go func() {
		for ev := range pod2.Watch(ctx) {
			fmt.Printf("[pod-2] %s key=%q origin=%s value=%+v\n",
				ev.Kind, ev.Key, ev.Origin, ev.Value)
		}
	}()

	if err := pod1.Set(ctx, "global", Config{
		RateLimit:    1000,
		FeatureFlags: map[string]bool{"beta_chat": true},
	}); err != nil {
		log.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	cfg, ok := pod2.Get("global")
	fmt.Printf("[pod-2] Get(global) ok=%v cfg=%+v\n", ok, cfg)
}
