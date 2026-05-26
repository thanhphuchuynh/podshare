// Interactive web demo: 3 pods share state in one process, each exposing
// its own HTTP UI. Open three browser tabs (one per port) and watch
// writes propagate across them in real time.
//
//	go run ./examples/web
//	# then open http://localhost:9080, :9081, :9082 in 3 tabs
//
// Override the starting port with PORT_START=N (defaults to 9080).
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

//go:embed static/index.html
var staticFS embed.FS

// Item is the shared value type. Anything JSON-serializable works.
type Item struct {
	Description string    `json:"description"`
	Counter     int       `json:"counter"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type pod struct {
	ID    string
	Port  int
	Color string // "teal" | "purple" | "amber"
	Store *podshare.Store[Item]
}

type peerInfo struct {
	ID    string
	Port  int
	Color string
}

var pods []*pod

func main() {
	// Single in-process broker shared by all three pods. In production
	// you'd swap this for the Redis or P2P transport — the rest of the
	// code is identical.
	tr := transport.NewMemoryTransport()
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	portStart := 9080
	if v := os.Getenv("PORT_START"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			portStart = n
		}
	}
	flag.IntVar(&portStart, "port", portStart, "starting port (uses port, port+1, port+2)")
	flag.Parse()

	colors := []string{"teal", "purple", "amber"}

	for i := range 3 {
		podID := fmt.Sprintf("pod-%d", i+1)
		store, err := podshare.New[Item](ctx, "web-demo", tr,
			podshare.WithNodeID[Item](podID),
			podshare.WithErrorHandler[Item](func(e error) {
				log.Printf("[%s] err: %v", podID, e)
			}),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer store.Close()

		pods = append(pods, &pod{
			ID:    podID,
			Port:  portStart + i,
			Color: colors[i],
			Store: store,
		})
	}

	// Bring up each pod's HTTP server in its own goroutine.
	var wg sync.WaitGroup
	servers := make([]*http.Server, len(pods))
	for i, p := range pods {
		srv := &http.Server{Addr: fmt.Sprintf(":%d", p.Port), Handler: routes(p)}
		servers[i] = srv
		wg.Add(1)
		go func(p *pod, srv *http.Server) {
			defer wg.Done()
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("[%s] http: %v", p.ID, err)
			}
		}(p, srv)
	}

	log.Println()
	log.Println("podshare web demo running — open these in 3 tabs:")
	for _, p := range pods {
		log.Printf("  %s  →  http://localhost:%d", p.ID, p.Port)
	}
	log.Println("\nctrl-c to stop")

	// Wait for SIGINT.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("\nshutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	wg.Wait()
}

func routes(p *pod) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", indexHandler(p))
	mux.HandleFunc("GET /api/state", stateHandler(p))
	mux.HandleFunc("POST /api/set", setHandler(p))
	mux.HandleFunc("POST /api/delete", deleteHandler(p))
	mux.HandleFunc("GET /api/events", eventsSSEHandler(p))
	mux.HandleFunc("GET /api/stats", statsHandler(p))
	return mux
}

func indexHandler(p *pod) http.HandlerFunc {
	tmpl := template.Must(template.ParseFS(staticFS, "static/index.html"))
	return func(w http.ResponseWriter, r *http.Request) {
		peers := make([]peerInfo, 0, len(pods)-1)
		for _, op := range pods {
			if op.ID != p.ID {
				peers = append(peers, peerInfo{ID: op.ID, Port: op.Port, Color: op.Color})
			}
		}
		data := map[string]any{
			"PodID": p.ID,
			"Port":  p.Port,
			"Color": p.Color,
			"Peers": peers,
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmpl.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func stateHandler(p *pod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pod_id": p.ID,
			"state":  p.Store.Snapshot(),
		})
	}
}

func setHandler(p *pod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key   string `json:"key"`
			Value Item   `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}
		req.Value.UpdatedAt = time.Now().UTC()
		if err := p.Store.Set(r.Context(), req.Key, req.Value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func deleteHandler(p *pod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := p.Store.Delete(r.Context(), req.Key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func statsHandler(p *pod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Store.Stats())
	}
}

// eventsSSEHandler streams Watch events as Server-Sent Events. The
// browser's EventSource reconnects automatically if the connection
// drops (e.g., if podshare drops this watcher for being slow).
func eventsSSEHandler(p *pod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		// Tell the client which pod they're attached to.
		fmt.Fprintf(w, "event: hello\ndata: %q\n\n", p.ID)
		flusher.Flush()

		events := p.Store.Watch(r.Context())
		for ev := range events {
			body, _ := json.Marshal(map[string]any{
				"kind":      ev.Kind.String(),
				"key":       ev.Key,
				"value":     ev.Value,
				"origin":    ev.Origin,
				"timestamp": ev.Timestamp,
			})
			if _, err := fmt.Fprintf(w, "event: change\ndata: %s\n\n", body); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
