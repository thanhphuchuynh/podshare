package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// P2P wire-frame: [u32 totalLen][u8 type][u16 topicLen][topic][payload]
// totalLen = 1 + 2 + len(topic) + len(payload).
const (
	p2pMsgEvent    byte = 1
	p2pMsgSnapReq  byte = 2
	p2pMsgSnapResp byte = 3

	p2pMaxFrame = 64 * 1024 * 1024
)

// P2POptions configures a P2PTransport. Listen is required; Peers should
// list every other pod's listen address. In Kubernetes, populate Peers
// from a headless Service so each pod resolves its siblings via DNS
// (pod-0.svc, pod-1.svc, ...).
//
// The transport has no built-in authentication. For untrusted networks,
// supply a TLSConfig with mTLS enabled (ClientAuth =
// tls.RequireAndVerifyClientCert and a ClientCAs / Certificates pair).
type P2POptions struct {
	Listen string
	Peers  []string

	DialTimeout       time.Duration // default 5s
	ReconnectInterval time.Duration // default 5s
	SnapshotTimeout   time.Duration // default 2s; how long FetchSnapshot waits for a peer reply
	KeepAlivePeriod   time.Duration // default 30s; TCP keepalive interval; 0 disables

	// SendQueueDepth bounds per-peer send queues. A peer that cannot drain
	// fast enough will see Publish drops surface as errors. Default 1024.
	SendQueueDepth int

	// TLSConfig, when non-nil, wraps both inbound (listener) and outbound
	// (dial) connections with TLS. Set ClientAuth and Certificates for mTLS.
	TLSConfig *tls.Config

	// OnError, if set, receives non-fatal transport errors: dial failures,
	// frame decode errors, dropped queue entries. Must be cheap and
	// non-blocking; runs on transport-internal goroutines.
	OnError func(error)

	// OnConnect fires once per accepted or established connection.
	// remote is the peer's network address; inbound is true for accepted
	// connections, false for dialed ones. Use it to trigger a Store
	// Refresh and recover events missed during a disconnect.
	OnConnect func(remote string, inbound bool)

	// OnDisconnect fires once per terminated connection.
	OnDisconnect func(remote string, inbound bool)
}

// P2PTransport is a dependency-free TCP mesh. Snapshots are kept in
// memory and served from any peer that holds them; for persistence across
// pod restarts, wrap the transport with a backing store (etcd, S3, …).
type P2PTransport struct {
	listenAddr string
	peers      []string
	dialT      time.Duration
	reconn     time.Duration
	snapTO     time.Duration
	keepAlive  time.Duration
	sendDepth    int
	tlsConfig    *tls.Config
	onError      func(error)
	onConnect    func(string, bool)
	onDisconnect func(string, bool)

	listener net.Listener

	mu        sync.RWMutex
	subs      map[string]chan []byte
	snapshots map[string][]byte
	conns     map[*p2pConn]struct{}

	snapReqMu       sync.Mutex
	pendingSnapReqs map[string]chan []byte

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type p2pConn struct {
	nc        net.Conn
	sendQ     chan p2pFrame
	cancel    chan struct{}
	closeOnce sync.Once
}

type p2pFrame struct {
	msgType byte
	topic   string
	payload []byte
}

// NewP2PTransport binds the listener immediately and starts dialing every
// peer in the background. Failures to dial are retried indefinitely.
func NewP2PTransport(opts P2POptions) (*P2PTransport, error) {
	if opts.Listen == "" {
		return nil, errors.New("transport: P2P Listen required")
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.ReconnectInterval == 0 {
		opts.ReconnectInterval = 5 * time.Second
	}
	if opts.SnapshotTimeout == 0 {
		opts.SnapshotTimeout = 2 * time.Second
	}
	if opts.KeepAlivePeriod == 0 {
		opts.KeepAlivePeriod = 30 * time.Second
	}
	if opts.SendQueueDepth <= 0 {
		opts.SendQueueDepth = 1024
	}

	rawListener, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", opts.Listen, err)
	}
	listener := rawListener
	if opts.TLSConfig != nil {
		listener = tls.NewListener(rawListener, opts.TLSConfig)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &P2PTransport{
		listenAddr:      opts.Listen,
		peers:           append([]string(nil), opts.Peers...),
		dialT:           opts.DialTimeout,
		reconn:          opts.ReconnectInterval,
		snapTO:          opts.SnapshotTimeout,
		keepAlive:       opts.KeepAlivePeriod,
		sendDepth:       opts.SendQueueDepth,
		tlsConfig:       opts.TLSConfig,
		onError:         opts.OnError,
		onConnect:       opts.OnConnect,
		onDisconnect:    opts.OnDisconnect,
		listener:        listener,
		subs:            make(map[string]chan []byte),
		snapshots:       make(map[string][]byte),
		conns:           make(map[*p2pConn]struct{}),
		pendingSnapReqs: make(map[string]chan []byte),
		ctx:             ctx,
		cancel:          cancel,
	}

	t.wg.Add(1)
	go t.acceptLoop()

	for _, peer := range t.peers {
		t.wg.Add(1)
		go t.dialLoop(peer)
	}

	return t, nil
}

// Addr returns the bound listener address — useful when Listen was ":0"
// and the caller needs to share the port with peers.
func (t *P2PTransport) Addr() string {
	return t.listener.Addr().String()
}

func (t *P2PTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		c, err := t.listener.Accept()
		if err != nil {
			if t.isClosed() {
				return
			}
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		t.tuneTCP(c)
		t.wg.Add(1)
		go func(c net.Conn) {
			defer t.wg.Done()
			t.serveConn(c, true)
		}(c)
	}
}

func (t *P2PTransport) dialLoop(addr string) {
	defer t.wg.Done()
	for {
		if t.isClosed() {
			return
		}

		var (
			c   net.Conn
			err error
		)
		d := net.Dialer{Timeout: t.dialT, KeepAlive: t.keepAlive}
		if t.tlsConfig != nil {
			c, err = tls.DialWithDialer(&d, "tcp", addr, t.tlsConfig)
		} else {
			c, err = d.DialContext(t.ctx, "tcp", addr)
		}
		if err == nil {
			t.tuneTCP(c)
			t.serveConn(c, false)
		} else {
			t.report(fmt.Errorf("transport: dial %s: %w", addr, err))
		}

		select {
		case <-t.ctx.Done():
			return
		case <-time.After(t.reconn):
		}
	}
}

// tuneTCP enables TCP keepalive on the underlying socket. With TLS the
// inner *net.TCPConn is reachable via NetConn(). Keepalive lets the OS
// detect dead peers without our own heartbeat protocol.
func (t *P2PTransport) tuneTCP(c net.Conn) {
	if t.keepAlive <= 0 {
		return
	}
	type tcpish interface {
		SetKeepAlive(bool) error
		SetKeepAlivePeriod(time.Duration) error
	}
	var tc tcpish
	switch v := c.(type) {
	case *net.TCPConn:
		tc = v
	case *tls.Conn:
		if inner, ok := v.NetConn().(*net.TCPConn); ok {
			tc = inner
		}
	}
	if tc == nil {
		return
	}
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(t.keepAlive)
}

func (t *P2PTransport) serveConn(c net.Conn, inbound bool) {
	remote := c.RemoteAddr().String()
	pc := &p2pConn{
		nc:     c,
		sendQ:  make(chan p2pFrame, t.sendDepth),
		cancel: make(chan struct{}),
	}

	t.mu.Lock()
	if t.isClosedLocked() {
		t.mu.Unlock()
		_ = c.Close()
		return
	}
	t.conns[pc] = struct{}{}
	t.mu.Unlock()

	if t.onConnect != nil {
		t.onConnect(remote, inbound)
	}

	t.wg.Add(1)
	go t.senderLoop(pc)

	defer func() {
		pc.close()
		t.mu.Lock()
		delete(t.conns, pc)
		t.mu.Unlock()
		if t.onDisconnect != nil {
			t.onDisconnect(remote, inbound)
		}
	}()

	reader := bufio.NewReader(c)
	for {
		if t.isClosed() {
			return
		}
		msgType, topic, payload, err := readFrame(reader)
		if err != nil {
			if !t.isClosed() && !errors.Is(err, io.EOF) {
				t.report(fmt.Errorf("transport: read frame: %w", err))
			}
			return
		}
		switch msgType {
		case p2pMsgEvent:
			t.deliverEvent(topic, payload)
		case p2pMsgSnapReq:
			t.mu.RLock()
			snap := append([]byte(nil), t.snapshots[topic]...)
			t.mu.RUnlock()
			pc.enqueueOrReport(p2pFrame{msgType: p2pMsgSnapResp, topic: topic, payload: snap}, t.report)
		case p2pMsgSnapResp:
			t.deliverSnapResp(topic, payload)
		}
	}
}

func (t *P2PTransport) senderLoop(pc *p2pConn) {
	defer t.wg.Done()
	for {
		select {
		case <-pc.cancel:
			return
		case f := <-pc.sendQ:
			if err := writeFrame(pc.nc, f.msgType, f.topic, f.payload); err != nil {
				if !t.isClosed() {
					t.report(fmt.Errorf("transport: write frame: %w", err))
				}
				pc.close()
				return
			}
		}
	}
}

func (t *P2PTransport) deliverEvent(topic string, payload []byte) {
	t.mu.RLock()
	ch, ok := t.subs[topic]
	t.mu.RUnlock()
	if !ok {
		return
	}
	buf := append([]byte(nil), payload...)
	select {
	case ch <- buf:
	case <-t.ctx.Done():
	}
}

func (t *P2PTransport) deliverSnapResp(topic string, payload []byte) {
	t.snapReqMu.Lock()
	ch, ok := t.pendingSnapReqs[topic]
	if ok {
		delete(t.pendingSnapReqs, topic)
	}
	t.snapReqMu.Unlock()
	if !ok {
		return
	}
	buf := append([]byte(nil), payload...)
	select {
	case ch <- buf:
	default:
	}
}

func (t *P2PTransport) Publish(_ context.Context, topic string, msg []byte) error {
	if t.isClosed() {
		return errors.New("transport: closed")
	}
	t.mu.RLock()
	conns := make([]*p2pConn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.RUnlock()

	if len(conns) == 0 {
		return nil
	}

	frame := p2pFrame{msgType: p2pMsgEvent, topic: topic, payload: msg}
	dropped := 0
	for _, c := range conns {
		if !c.enqueue(frame) {
			dropped++
		}
	}
	if dropped == len(conns) {
		return fmt.Errorf("transport: all %d peer queue(s) full", len(conns))
	}
	if dropped > 0 {
		t.report(fmt.Errorf("transport: %d/%d peer queue(s) full", dropped, len(conns)))
	}
	return nil
}

func (t *P2PTransport) Subscribe(_ context.Context, topic string) (<-chan []byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isClosedLocked() {
		return nil, errors.New("transport: closed")
	}
	if _, exists := t.subs[topic]; exists {
		return nil, fmt.Errorf("transport: already subscribed to %q", topic)
	}
	ch := make(chan []byte, 256)
	t.subs[topic] = ch
	return ch, nil
}

func (t *P2PTransport) Snapshot(_ context.Context, topic string, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isClosedLocked() {
		return errors.New("transport: closed")
	}
	t.snapshots[topic] = append([]byte(nil), data...)
	return nil
}

// FetchSnapshot polls connected peers for the latest snapshot. Returns
// (nil, nil) if no peer responds within SnapshotTimeout — the caller
// should treat that as a fresh start.
func (t *P2PTransport) FetchSnapshot(ctx context.Context, topic string) ([]byte, error) {
	if t.isClosed() {
		return nil, errors.New("transport: closed")
	}

	respCh := make(chan []byte, 1)
	t.snapReqMu.Lock()
	if _, exists := t.pendingSnapReqs[topic]; exists {
		t.snapReqMu.Unlock()
		return nil, fmt.Errorf("transport: snapshot request for %q already in flight", topic)
	}
	t.pendingSnapReqs[topic] = respCh
	t.snapReqMu.Unlock()
	defer func() {
		t.snapReqMu.Lock()
		delete(t.pendingSnapReqs, topic)
		t.snapReqMu.Unlock()
	}()

	deadline := time.Now().Add(t.snapTO)
	requested := make(map[*p2pConn]struct{})

	for {
		if !time.Now().Before(deadline) {
			return nil, nil
		}

		t.mu.RLock()
		conns := make([]*p2pConn, 0, len(t.conns))
		for c := range t.conns {
			if _, sent := requested[c]; !sent {
				conns = append(conns, c)
			}
		}
		t.mu.RUnlock()

		for _, c := range conns {
			c.enqueueOrReport(p2pFrame{msgType: p2pMsgSnapReq, topic: topic}, t.report)
			requested[c] = struct{}{}
		}

		wait := time.Until(deadline)
		if wait > 100*time.Millisecond {
			wait = 100 * time.Millisecond
		}
		if wait <= 0 {
			return nil, nil
		}

		select {
		case raw := <-respCh:
			if len(raw) == 0 {
				return nil, nil
			}
			return raw, nil
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.ctx.Done():
			return nil, errors.New("transport: closed")
		}
	}
}

func (t *P2PTransport) Close() error {
	t.closeOnce.Do(func() {
		t.cancel()
		_ = t.listener.Close()

		t.mu.Lock()
		conns := make([]*p2pConn, 0, len(t.conns))
		for c := range t.conns {
			conns = append(conns, c)
		}
		subs := t.subs
		t.subs = nil
		t.mu.Unlock()

		for _, c := range conns {
			c.close()
		}

		t.wg.Wait()

		for _, ch := range subs {
			close(ch)
		}
	})
	return nil
}

func (t *P2PTransport) isClosed() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}

// isClosedLocked must be called with t.mu held.
func (t *P2PTransport) isClosedLocked() bool {
	return t.isClosed()
}

func (t *P2PTransport) report(err error) {
	if t.onError != nil {
		t.onError(err)
	}
}

func (c *p2pConn) close() {
	c.closeOnce.Do(func() {
		close(c.cancel)
		_ = c.nc.Close()
	})
}

func (c *p2pConn) enqueue(f p2pFrame) bool {
	select {
	case c.sendQ <- f:
		return true
	case <-c.cancel:
		return false
	default:
		return false
	}
}

func (c *p2pConn) enqueueOrReport(f p2pFrame, report func(error)) {
	if !c.enqueue(f) && report != nil {
		report(fmt.Errorf("transport: send queue full (depth=%d)", cap(c.sendQ)))
	}
}

func writeFrame(w io.Writer, msgType byte, topic string, payload []byte) error {
	if len(topic) > 0xFFFF {
		return errors.New("transport: topic too long")
	}
	totalLen := 1 + 2 + len(topic) + len(payload)
	if totalLen > p2pMaxFrame {
		return fmt.Errorf("transport: frame too large (%d)", totalLen)
	}
	hdr := make([]byte, 4+1+2+len(topic))
	binary.BigEndian.PutUint32(hdr[0:4], uint32(totalLen))
	hdr[4] = msgType
	binary.BigEndian.PutUint16(hdr[5:7], uint16(len(topic)))
	copy(hdr[7:], topic)
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readFrame(r io.Reader) (msgType byte, topic string, payload []byte, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, "", nil, err
	}
	totalLen := binary.BigEndian.Uint32(hdr[:])
	if totalLen < 3 {
		return 0, "", nil, fmt.Errorf("transport: frame length %d too small", totalLen)
	}
	if totalLen > p2pMaxFrame {
		return 0, "", nil, fmt.Errorf("transport: frame too large (%d)", totalLen)
	}
	buf := make([]byte, totalLen)
	if _, err = io.ReadFull(r, buf); err != nil {
		return 0, "", nil, err
	}
	msgType = buf[0]
	tlen := int(binary.BigEndian.Uint16(buf[1:3]))
	if 3+tlen > len(buf) {
		return 0, "", nil, errors.New("transport: invalid topic length")
	}
	topic = string(buf[3 : 3+tlen])
	if 3+tlen < len(buf) {
		payload = buf[3+tlen:]
	}
	return msgType, topic, payload, nil
}
