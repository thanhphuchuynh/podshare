// Redis-backed example. Run two copies in two terminals — they will
// converge on the same shared state through Redis Pub/Sub.
//
//	go run ./examples/redis -id pod-1
//	go run ./examples/redis -id pod-2
//
// Requires a Redis server reachable at REDIS_ADDR (default localhost:6379).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type RateLimits struct {
	PerUser   int    `json:"per_user"`
	PerIP     int    `json:"per_ip"`
	BurstSize int    `json:"burst_size"`
	UpdatedBy string `json:"updated_by"`
}

func main() {
	id := flag.String("id", "pod-1", "node identifier")
	addr := flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "redis address")
	flag.Parse()

	tr, err := transport.NewRedisTransport(transport.RedisOptions{Addr: *addr})
	if err != nil {
		log.Fatalf("redis: %v (is redis running on %s?)", err, *addr)
	}
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := podshare.New[RateLimits](ctx, "rate-limits", tr,
		podshare.WithNodeID[RateLimits](*id),
		podshare.WithErrorHandler[RateLimits](func(e error) {
			log.Printf("[%s] error: %v", *id, e)
		}),
	)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer store.Close()

	go func() {
		for ev := range store.Watch(ctx) {
			log.Printf("[%s] %s key=%q origin=%s value=%+v",
				*id, ev.Kind, ev.Key, ev.Origin, ev.Value)
		}
	}()

	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("[%s] connected to redis %s — ctrl-c to exit", *id, *addr)

	step := 0
	for {
		select {
		case <-tick.C:
			step++
			err := store.Set(ctx, *id+"-policy", RateLimits{
				PerUser:   100 * step,
				PerIP:     1000 * step,
				BurstSize: 50,
				UpdatedBy: *id,
			})
			if err != nil {
				log.Printf("[%s] set: %v", *id, err)
			}
		case <-sig:
			log.Printf("[%s] shutting down", *id)
			return
		}
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
