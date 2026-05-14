// P2P mesh example. Three "pods" run in one process, fully meshed over
// loopback TCP. In production each pod is a separate process and Peers
// is populated from a Kubernetes headless Service or similar discovery.
//
//	go run ./examples/p2p
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type Presence struct {
	UserID   string `json:"user_id"`
	Status   string `json:"status"`
	LastSeen string `json:"last_seen"`
}

type pod struct {
	id    string
	addr  string
	tr    *transport.P2PTransport
	store *podshare.Store[Presence]
}

func main() {
	addrs := []string{"127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103"}

	pods := make([]*pod, len(addrs))
	for i, addr := range addrs {
		peers := otherAddrs(addrs, i)
		tr, err := transport.NewP2PTransport(transport.P2POptions{
			Listen:            addr,
			Peers:             peers,
			ReconnectInterval: 200 * time.Millisecond,
			OnError: func(e error) {
				// Dial errors during startup are expected — the peer
				// hasn't bound its port yet. Suppress in this demo.
			},
		})
		if err != nil {
			log.Fatal(err)
		}

		id := fmt.Sprintf("pod-%d", i+1)
		s, err := podshare.New[Presence](context.Background(), "presence", tr,
			podshare.WithNodeID[Presence](id),
		)
		if err != nil {
			log.Fatal(err)
		}
		pods[i] = &pod{id: id, addr: addr, tr: tr, store: s}
	}

	defer func() {
		for _, p := range pods {
			_ = p.store.Close()
			_ = p.tr.Close()
		}
	}()

	// Let dials connect.
	time.Sleep(500 * time.Millisecond)

	// pod-1 watches every change in the mesh.
	go func() {
		for ev := range pods[0].store.Watch(context.Background()) {
			fmt.Printf("[pod-1 watch] %s %s origin=%s status=%s\n",
				ev.Kind, ev.Key, ev.Origin, ev.Value.Status)
		}
	}()

	// Each pod registers its own presence.
	for i, p := range pods {
		err := p.store.Set(context.Background(), p.id, Presence{
			UserID:   fmt.Sprintf("user-%d", 100+i),
			Status:   "online",
			LastSeen: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			log.Printf("[%s] set: %v", p.id, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	fmt.Println("\nfinal state on each pod:")
	for _, p := range pods {
		fmt.Printf("[%s] sees %d users\n", p.id, len(p.store.Keys()))
		for _, k := range p.store.Keys() {
			v, _ := p.store.Get(k)
			fmt.Printf("  %s -> %s (%s)\n", k, v.UserID, v.Status)
		}
	}

	// pod-2 goes offline (Delete tombstones it everywhere).
	fmt.Println("\npod-2 going offline …")
	if err := pods[1].store.Delete(context.Background(), "pod-2"); err != nil {
		log.Printf("delete: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\nstate after pod-2 left:")
	for _, p := range pods {
		fmt.Printf("[%s] sees %d users\n", p.id, len(p.store.Keys()))
	}
}

func otherAddrs(all []string, self int) []string {
	out := make([]string, 0, len(all)-1)
	for i, a := range all {
		if i != self {
			out = append(out, a)
		}
	}
	return out
}
