// Package callreply is a small RPC layer that runs over any
// podshare.Transport. It pairs with podshare's routing-table pattern:
// each Endpoint owns a set of targets (e.g., "ws:alice"), publishes
// the routes via a podshare.Store, and accepts calls into them.
//
// Wire shape:
//
//	Pod B                     Transport pub/sub               Pod A
//	  │                              │                          │
//	  │  envelope{type:call, id,     │                          │
//	  │     reply:"inbox:B",         │                          │
//	  │     target:"ws:alice",      ─┼─►                        │
//	  │     method:"Send",           │   handlers["ws:alice"]   │
//	  │     args:"hello"}            │     ["Send"](args) ─► WS │
//	  │                              │                          │
//	  │◄─ envelope{type:reply, id,  ─┼── publishes to "inbox:B" │
//	  │     result/error}            │                          │
//
// Routing is maintained inside a podshare.Store[Route] keyed by target.
// Register publishes a route claim; Unregister deletes it. LWW on the
// routing table means moves between pods (e.g., reconnect to a different
// pod) overwrite the old entry automatically.
package callreply

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thanhphuchuynh/podshare"
)

// Handler executes a method call on the owning pod. Args is the raw
// payload bytes sent by the caller; the returned bytes become the
// reply Result. Returning an error sends the error string back to the
// caller as a remote-side failure.
type Handler func(ctx context.Context, args []byte) ([]byte, error)

// Route describes which pod currently owns a target. Stored in the
// shared routing table so callers can locate the owner.
type Route struct {
	PodID     string    `json:"pod_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Options configures an Endpoint.
type Options struct {
	// SelfID is this pod's identifier — must be unique per process.
	SelfID string

	// RoutingTopic is the podshare topic used for the target → pod map.
	// Default "callreply:routing".
	RoutingTopic string

	// InboxPrefix is prepended to per-pod inbox channel names. Default
	// "callreply:inbox:". An Endpoint subscribes to InboxPrefix+SelfID.
	InboxPrefix string

	// CallTimeout caps how long Call waits for a reply. Default 5s.
	// Pass a smaller value with context to override per call.
	CallTimeout time.Duration

	// OnError, if set, receives non-fatal errors: bad envelopes, replies
	// for unknown call IDs, publish failures from handlers. Must be
	// cheap and non-blocking.
	OnError func(error)

	// MaxConcurrentHandlers caps simultaneous handler executions on this
	// Endpoint. When the cap is reached, additional inbound calls
	// reply immediately with "endpoint busy" without invoking their
	// handler. Default 1024; pass a higher value to raise it. There is
	// no unlimited mode — open backpressure is the point.
	MaxConcurrentHandlers int
}

// Endpoint is one pod's RPC peer. Construct with New, register handlers
// for owned targets, and Call into targets owned elsewhere.
//
// All methods are safe for concurrent use.
type Endpoint struct {
	transport   podshare.Transport
	selfID      string
	inboxPrefix string // e.g., "callreply:inbox:"
	inbox       string // inboxPrefix + selfID
	timeout     time.Duration
	onError     func(error)

	// handlerSem bounds concurrent handler executions. Buffered chan
	// of capacity MaxConcurrentHandlers; serveCall and the self-call
	// fast path both pass through it. nil means no limit (constructor
	// never sets nil — default cap is 1024).
	handlerSem chan struct{}

	routing *podshare.Store[Route]

	handlersMu sync.RWMutex
	handlers   map[string]map[string]Handler // target -> method -> handler

	pendingMu sync.Mutex
	pending   map[string]chan envelope

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// envelope is the wire format. Both calls and replies share it; Type
// discriminates. DeadlineNanos carries the caller's context deadline so
// the receiver can bound its handler's runtime to the same bound — see
// serveCall.
type envelope struct {
	Type          string `json:"type"`
	ID            string `json:"id"`
	Reply         string `json:"reply,omitempty"`
	Target        string `json:"target,omitempty"`
	Method        string `json:"method,omitempty"`
	Args          []byte `json:"args,omitempty"`
	Error         string `json:"error,omitempty"`
	Result        []byte `json:"result,omitempty"`
	DeadlineNanos int64  `json:"deadline,omitempty"`
}

// New constructs an Endpoint, joins the routing table, and starts
// listening on its inbox. Callers own the Transport's lifetime; close
// it after every Endpoint and Store on it has been closed.
func New(t podshare.Transport, opts Options) (*Endpoint, error) {
	if t == nil {
		return nil, errors.New("callreply: transport is nil")
	}
	if opts.SelfID == "" {
		return nil, errors.New("callreply: SelfID is required")
	}
	if opts.RoutingTopic == "" {
		opts.RoutingTopic = "callreply:routing"
	}
	if opts.InboxPrefix == "" {
		opts.InboxPrefix = "callreply:inbox:"
	}
	if opts.CallTimeout == 0 {
		opts.CallTimeout = 5 * time.Second
	}
	if opts.MaxConcurrentHandlers <= 0 {
		opts.MaxConcurrentHandlers = 1024
	}

	routing, err := podshare.New[Route](context.Background(), opts.RoutingTopic, t,
		podshare.WithNodeID[Route](opts.SelfID),
		podshare.WithErrorHandler[Route](opts.OnError),
	)
	if err != nil {
		return nil, fmt.Errorf("callreply: routing store: %w", err)
	}

	inbox := opts.InboxPrefix + opts.SelfID
	sub, err := t.Subscribe(context.Background(), inbox)
	if err != nil {
		_ = routing.Close()
		return nil, fmt.Errorf("callreply: subscribe inbox: %w", err)
	}

	e := &Endpoint{
		transport:   t,
		selfID:      opts.SelfID,
		inboxPrefix: opts.InboxPrefix,
		inbox:       inbox,
		timeout:     opts.CallTimeout,
		onError:     opts.OnError,
		handlerSem:  make(chan struct{}, opts.MaxConcurrentHandlers),
		routing:     routing,
		handlers:    make(map[string]map[string]Handler),
		pending:     make(map[string]chan envelope),
		closeCh:     make(chan struct{}),
	}

	e.wg.Add(1)
	go e.recvLoop(sub)

	return e, nil
}

// Register installs a handler for (target, method) and claims target in
// the routing table. The first call for a target publishes the route;
// later calls reuse it.
func (e *Endpoint) Register(target, method string, h Handler) error {
	if target == "" || method == "" || h == nil {
		return errors.New("callreply: target, method, and handler are required")
	}

	e.handlersMu.Lock()
	first := e.handlers[target] == nil
	if first {
		e.handlers[target] = make(map[string]Handler)
	}
	e.handlers[target][method] = h
	e.handlersMu.Unlock()

	if !first {
		return nil
	}
	return e.routing.Set(context.Background(), target, Route{
		PodID:     e.selfID,
		UpdatedAt: time.Now().UTC(),
	})
}

// Unregister releases target — drops every handler for it and deletes
// the route. In-flight calls already routed to this pod will still try
// to execute against the (now-empty) handler map and return an error.
func (e *Endpoint) Unregister(target string) error {
	e.handlersMu.Lock()
	delete(e.handlers, target)
	e.handlersMu.Unlock()
	return e.routing.Delete(context.Background(), target)
}

// Owner returns the pod ID that currently owns target, or "" if no pod
// claims it. Useful for "is alice online and where" queries.
func (e *Endpoint) Owner(target string) (string, bool) {
	r, ok := e.routing.Get(target)
	if !ok {
		return "", false
	}
	return r.PodID, true
}

// Call invokes (target.method, args) on whichever pod owns target. The
// caller's context controls timeout and cancellation; CallTimeout in
// Options is the upper bound when the context has no deadline. If the
// route resolves to this Endpoint's own SelfID, Call short-circuits and
// invokes the handler in-process.
func (e *Endpoint) Call(ctx context.Context, target, method string, args []byte) ([]byte, error) {
	if e.isClosed() {
		return nil, errors.New("callreply: closed")
	}

	route, ok := e.routing.Get(target)
	if !ok {
		return nil, fmt.Errorf("callreply: no route for %q", target)
	}

	// Apply the same deadline floor that remote calls would see, so
	// self-calls and remote calls behave identically.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && e.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.timeout)
		defer cancel()
	}

	if route.PodID == e.selfID {
		return e.callLocal(ctx, target, method, args)
	}

	id, err := randomID()
	if err != nil {
		return nil, err
	}

	env := envelope{
		Type:   "call",
		ID:     id,
		Reply:  e.inbox,
		Target: target,
		Method: method,
		Args:   args,
	}
	if dl, ok := ctx.Deadline(); ok {
		env.DeadlineNanos = dl.UnixNano()
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	replyCh := make(chan envelope, 1)
	e.pendingMu.Lock()
	e.pending[id] = replyCh
	e.pendingMu.Unlock()
	defer func() {
		e.pendingMu.Lock()
		delete(e.pending, id)
		e.pendingMu.Unlock()
	}()

	targetInbox := e.inboxPrefix + route.PodID
	if err := e.transport.Publish(ctx, targetInbox, raw); err != nil {
		return nil, fmt.Errorf("callreply: publish: %w", err)
	}

	select {
	case reply := <-replyCh:
		if reply.Error != "" {
			return nil, errors.New(reply.Error)
		}
		return reply.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.closeCh:
		return nil, errors.New("callreply: closed")
	}
}

// callLocal services a Call whose target lives on this Endpoint.
// Same semaphore bound and same handler-context shape as a remote call.
func (e *Endpoint) callLocal(ctx context.Context, target, method string, args []byte) ([]byte, error) {
	e.handlersMu.RLock()
	methods := e.handlers[target]
	var h Handler
	if methods != nil {
		h = methods[method]
	}
	e.handlersMu.RUnlock()
	if h == nil {
		return nil, fmt.Errorf("callreply: no handler for %s.%s", target, method)
	}

	select {
	case e.handlerSem <- struct{}{}:
		defer func() { <-e.handlerSem }()
	default:
		return nil, errors.New("callreply: endpoint busy")
	}

	return h(ctx, args)
}

// Close stops the inbox listener and closes the internal routing store.
// The caller-supplied Transport is not closed.
func (e *Endpoint) Close() error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
		e.wg.Wait()
		_ = e.routing.Close()
	})
	return nil
}

func (e *Endpoint) recvLoop(sub <-chan []byte) {
	defer e.wg.Done()
	for {
		select {
		case <-e.closeCh:
			return
		case raw, ok := <-sub:
			if !ok {
				return
			}
			e.handleInbound(raw)
		}
	}
}

func (e *Endpoint) handleInbound(raw []byte) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		e.report(fmt.Errorf("callreply: decode envelope: %w", err))
		return
	}
	switch env.Type {
	case "call":
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.serveCall(env)
		}()
	case "reply":
		e.deliverReply(env)
	default:
		e.report(fmt.Errorf("callreply: unknown envelope type %q", env.Type))
	}
}

func (e *Endpoint) serveCall(c envelope) {
	reply := envelope{Type: "reply", ID: c.ID}

	// Concurrency cap. Open backpressure: tell the caller to back off
	// instead of growing handler goroutines unboundedly.
	select {
	case e.handlerSem <- struct{}{}:
		defer func() { <-e.handlerSem }()
	default:
		reply.Error = "endpoint busy"
		e.sendReply(c.Reply, reply)
		return
	}

	e.handlersMu.RLock()
	methods := e.handlers[c.Target]
	var h Handler
	if methods != nil {
		h = methods[c.Method]
	}
	e.handlersMu.RUnlock()

	if h == nil {
		reply.Error = fmt.Sprintf("no handler for %s.%s", c.Target, c.Method)
		e.sendReply(c.Reply, reply)
		return
	}

	// Honor the caller's deadline when carried on the wire; otherwise
	// fall back to the local CallTimeout so a runaway handler doesn't
	// hang the worker pool forever.
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	switch {
	case c.DeadlineNanos > 0:
		ctx, cancel = context.WithDeadline(context.Background(), time.Unix(0, c.DeadlineNanos))
	case e.timeout > 0:
		ctx, cancel = context.WithTimeout(context.Background(), e.timeout)
	default:
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	result, err := h(ctx, c.Args)
	if err != nil {
		reply.Error = err.Error()
	} else {
		reply.Result = result
	}
	e.sendReply(c.Reply, reply)
}

func (e *Endpoint) sendReply(inbox string, reply envelope) {
	if inbox == "" {
		return
	}
	raw, err := json.Marshal(reply)
	if err != nil {
		e.report(fmt.Errorf("callreply: encode reply: %w", err))
		return
	}
	if err := e.transport.Publish(context.Background(), inbox, raw); err != nil {
		e.report(fmt.Errorf("callreply: publish reply to %s: %w", inbox, err))
	}
}

func (e *Endpoint) deliverReply(r envelope) {
	e.pendingMu.Lock()
	ch, ok := e.pending[r.ID]
	e.pendingMu.Unlock()
	if !ok {
		e.report(fmt.Errorf("callreply: reply for unknown call id %q", r.ID))
		return
	}
	select {
	case ch <- r:
	default:
	}
}

func (e *Endpoint) report(err error) {
	if e.onError != nil {
		e.onError(err)
	}
}

func (e *Endpoint) isClosed() bool {
	select {
	case <-e.closeCh:
		return true
	default:
		return false
	}
}

func randomID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
