// Chat-history hot cache. Each pod replicates the recent conversations
// it has seen, so any pod in the fleet can answer a follow-up turn for
// any user without touching the database.
//
// Pattern recap: writes go through whichever pod the user is currently
// pinned to (via consistent-hash, sticky session, etc.) — single-writer
// per conversation avoids the LWW append-race. Reads are free on every
// pod because state is replicated.
//
//	go run ./examples/chat-cache
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

type Message struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"` // "user" | "assistant"
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type Conversation struct {
	ID       string    `json:"id"`
	UserID   string    `json:"user_id"`
	Messages []Message `json:"messages"`
}

func main() {
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	ctx := context.Background()
	podA := mustNew(ctx, tr, "chat-pod-a")
	defer podA.Close()
	podB := mustNew(ctx, tr, "chat-pod-b")
	defer podB.Close()
	podC := mustNew(ctx, tr, "chat-pod-c")
	defer podC.Close()

	convID := "conv-42"

	// Turn 1: alice talks to pod-a (the conversation's owner).
	conv := Conversation{
		ID:     convID,
		UserID: "alice",
		Messages: []Message{
			{ID: "m1", Role: "user", Content: "what's the capital of France?", Timestamp: time.Now()},
			{ID: "m2", Role: "assistant", Content: "Paris.", Timestamp: time.Now()},
		},
	}
	must(podA.Set(ctx, conv.ID, conv))

	// Turn 2: alice's next request lands on pod-b (load balancer choice).
	// pod-b reads the cached conversation locally — no DB hop.
	time.Sleep(50 * time.Millisecond)
	if cached, ok := podB.Get(convID); !ok {
		log.Fatal("pod-b missed cache")
	} else {
		fmt.Printf("[pod-b] cache hit, %d messages already loaded\n", len(cached.Messages))
	}

	// pod-c, which has never directly served alice, also has the state.
	if cached, ok := podC.Get(convID); !ok {
		log.Fatal("pod-c missed cache")
	} else {
		fmt.Printf("[pod-c] cache hit, %d messages\n", len(cached.Messages))
	}

	// Turn 3: alice keeps talking, still pinned to pod-a (single writer).
	conv.Messages = append(conv.Messages,
		Message{ID: "m3", Role: "user", Content: "what about Germany?", Timestamp: time.Now()},
		Message{ID: "m4", Role: "assistant", Content: "Berlin.", Timestamp: time.Now()},
	)
	must(podA.Set(ctx, conv.ID, conv))

	time.Sleep(50 * time.Millisecond)

	finalOnB, _ := podB.Get(convID)
	out, _ := json.MarshalIndent(finalOnB, "", "  ")
	fmt.Printf("\n[pod-b] full conversation after replication:\n%s\n", out)

	// Conversation ends; tombstone everywhere.
	must(podA.Delete(ctx, convID))
	time.Sleep(50 * time.Millisecond)

	if _, ok := podC.Get(convID); ok {
		log.Fatal("pod-c still sees the deleted conversation")
	}
	fmt.Println("\n[pod-c] conversation evicted everywhere after Delete")
}

func mustNew(ctx context.Context, tr podshare.Transport, nodeID string) *podshare.Store[Conversation] {
	s, err := podshare.New[Conversation](ctx, "conversations", tr,
		podshare.WithNodeID[Conversation](nodeID),
		podshare.WithErrorHandler[Conversation](func(e error) {
			log.Printf("[%s] %v", nodeID, e)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
