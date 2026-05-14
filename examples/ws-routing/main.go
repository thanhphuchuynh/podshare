// WebSocket routing across pods. A user opens a WS connection on
// whichever pod the load balancer picked; any other pod can push a
// message to that user via callreply, even though only the owning pod
// holds the actual *websocket.Conn.
//
// Two pods run in one process for the demo. The "WebSocket" is faked
// (it's just a struct with a Send method); the routing + RPC are real.
//
//	go run ./examples/ws-routing
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/callreply"
	"github.com/thanhphuchuynh/podshare/transport"
)

// fakeWS stands in for *websocket.Conn — the thing that *cannot* cross
// pods. Each pod holds its own and never serializes it.
type fakeWS struct {
	user string
	mu   sync.Mutex
	sent []string
}

func (w *fakeWS) Send(msg string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, msg)
	fmt.Printf("    [ws to %s] received %q\n", w.user, msg)
	return nil
}

// pod is one application instance — it owns local connections and an
// Endpoint that exposes them to the cluster.
type pod struct {
	id    string
	ep    *callreply.Endpoint
	connsMu sync.Mutex
	conns map[string]*fakeWS // userID -> WS (process-local!)
}

func newPod(id string, tr podshare.Transport) (*pod, error) {
	ep, err := callreply.New(tr, callreply.Options{
		SelfID:      id,
		CallTimeout: 2 * time.Second,
		OnError: func(e error) {
			log.Printf("[%s] %v", id, e)
		},
	})
	if err != nil {
		return nil, err
	}
	return &pod{id: id, ep: ep, conns: map[string]*fakeWS{}}, nil
}

// userConnects is what runs when a real WebSocket handshake completes
// on this pod. We stash the conn locally and register the handler so
// any pod in the cluster can reach the user via callreply.
func (p *pod) userConnects(user string) error {
	conn := &fakeWS{user: user}
	p.connsMu.Lock()
	p.conns[user] = conn
	p.connsMu.Unlock()

	target := "ws:" + user
	return p.ep.Register(target, "Send", func(ctx context.Context, args []byte) ([]byte, error) {
		var msg string
		if err := json.Unmarshal(args, &msg); err != nil {
			return nil, fmt.Errorf("decode msg: %w", err)
		}
		p.connsMu.Lock()
		c := p.conns[user]
		p.connsMu.Unlock()
		if c == nil {
			return nil, fmt.Errorf("user %s not connected here anymore", user)
		}
		if err := c.Send(msg); err != nil {
			return nil, err
		}
		return json.Marshal("ok")
	})
}

func (p *pod) userDisconnects(user string) {
	p.connsMu.Lock()
	delete(p.conns, user)
	p.connsMu.Unlock()
	_ = p.ep.Unregister("ws:" + user)
}

// SendTo is what application code calls regardless of which pod owns
// the user's connection. The callreply layer forwards transparently.
func (p *pod) SendTo(ctx context.Context, user, msg string) error {
	args, _ := json.Marshal(msg)
	_, err := p.ep.Call(ctx, "ws:"+user, "Send", args)
	return err
}

func main() {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	podA, err := newPod("pod-A", tr)
	if err != nil {
		log.Fatal(err)
	}
	defer podA.ep.Close()

	podB, err := newPod("pod-B", tr)
	if err != nil {
		log.Fatal(err)
	}
	defer podB.ep.Close()

	// Let routes propagate.
	settle := func() { time.Sleep(50 * time.Millisecond) }

	// alice opens her WS on pod-A.
	fmt.Println("alice connects to pod-A")
	if err := podA.userConnects("alice"); err != nil {
		log.Fatal(err)
	}
	settle()

	// pod-B wants to push a notification to alice. pod-B has no local
	// connection for alice — it doesn't matter; callreply forwards.
	fmt.Println("pod-B → alice: \"new message from bob\"")
	if err := podB.SendTo(context.Background(), "alice", "new message from bob"); err != nil {
		log.Fatalf("send: %v", err)
	}

	// pod-B asks the routing table where alice lives.
	if owner, ok := podB.ep.Owner("ws:alice"); ok {
		fmt.Printf("pod-B knows alice is on %s\n", owner)
	}

	// alice migrates: closes on A, reconnects to B (sticky session
	// changed, deploy rolled, whatever).
	fmt.Println("\nalice migrates from pod-A to pod-B")
	podA.userDisconnects("alice")
	if err := podB.userConnects("alice"); err != nil {
		log.Fatal(err)
	}
	settle()

	// pod-A pushes to alice; the route now points to pod-B.
	fmt.Println("pod-A → alice: \"welcome on the new pod\"")
	if err := podA.SendTo(context.Background(), "alice", "welcome on the new pod"); err != nil {
		log.Fatalf("send: %v", err)
	}

	if owner, ok := podB.ep.Owner("ws:alice"); ok {
		fmt.Printf("pod-A sees alice now on %s\n", owner)
	}

	// alice fully disconnects.
	fmt.Println("\nalice disconnects entirely")
	podB.userDisconnects("alice")
	settle()

	if err := podA.SendTo(context.Background(), "alice", "are you there?"); err != nil {
		fmt.Printf("expected error (alice offline): %v\n", err)
	}
}
