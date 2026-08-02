// Package sticky provides a session-aware reverse proxy for horizontally
// scaled Engine.IO and Socket.IO servers.
package sticky

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LoadBalancingMethod controls the selection of a backend for a new Engine.IO
// session. Existing sessions are always routed to their owning backend.
type LoadBalancingMethod string

const (
	Random          LoadBalancingMethod = "random"
	RoundRobin      LoadBalancingMethod = "round-robin"
	LeastConnection LoadBalancingMethod = "least-connection"
)

const defaultSessionTTL = 2 * time.Minute

var sidPattern = regexp.MustCompile(`"sid"\s*:\s*"([^"\\]+)"`)

// Options configures a Router.
type Options struct {
	// LoadBalancingMethod defaults to LeastConnection.
	LoadBalancingMethod LoadBalancingMethod
	// SessionTTL removes inactive polling mappings. Active polling clients touch
	// their mapping on every request. A zero value uses two minutes.
	SessionTTL time.Duration
	// SweepInterval controls stale mapping cleanup. A zero value is derived from
	// SessionTTL and capped at 30 seconds.
	SweepInterval time.Duration
	// ErrorHandler optionally handles proxy failures. The default returns 502.
	ErrorHandler func(http.ResponseWriter, *http.Request, error)
	// DebugHeader, when set, receives the selected backend ID. It is intended for
	// tests and diagnostics and is disabled by default.
	DebugHeader string
}

// BackendStats is a point-in-time backend snapshot.
type BackendStats struct {
	ID       string
	Target   string
	Sessions int
}

type backend struct {
	id         string
	target     *url.URL
	proxy      *httputil.ReverseProxy
	transport  *http.Transport
	sessions   int
	webSockets int
}

type session struct {
	backendID string
	lastSeen  time.Time
}

const closePayloadCaptureLimit = 128

type closePayloadCapture struct {
	io.ReadCloser
	prefix    []byte
	truncated bool
}

type upgradeTrackingResponseWriter struct {
	http.ResponseWriter
	hijacked bool
}

func (w *upgradeTrackingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("sticky: response writer does not support hijacking")
	}
	connection, readWriter, err := hijacker.Hijack()
	if err == nil {
		w.hijacked = true
	}
	return connection, readWriter, err
}

func (w *upgradeTrackingResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *upgradeTrackingResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (c *closePayloadCapture) Read(buffer []byte) (int, error) {
	n, err := c.ReadCloser.Read(buffer)
	remaining := closePayloadCaptureLimit - len(c.prefix)
	if remaining > 0 {
		copied := min(n, remaining)
		c.prefix = append(c.prefix, buffer[:copied]...)
		if copied < n {
			c.truncated = true
		}
	} else if n > 0 {
		c.truncated = true
	}
	return n, err
}

// Router routes every request carrying an Engine.IO sid to the backend that
// created that sid. It can be used in front of several Go processes together
// with a cluster adapter (for example the Unix or Redis adapter).
type Router struct {
	opts Options

	mu       sync.RWMutex
	backends map[string]*backend
	order    []string
	sessions map[string]session
	next     uint64

	stop      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

// New creates an empty Router. Backends can be registered with AddBackend.
func New(opts Options) (*Router, error) {
	if opts.LoadBalancingMethod == "" {
		opts.LoadBalancingMethod = LeastConnection
	}
	switch opts.LoadBalancingMethod {
	case Random, RoundRobin, LeastConnection:
	default:
		return nil, fmt.Errorf("sticky: unsupported load balancing method %q", opts.LoadBalancingMethod)
	}
	if opts.SessionTTL < 0 || opts.SweepInterval < 0 {
		return nil, errors.New("sticky: cleanup durations cannot be negative")
	}
	if opts.SessionTTL == 0 {
		opts.SessionTTL = defaultSessionTTL
	}
	if opts.SweepInterval == 0 {
		opts.SweepInterval = min(opts.SessionTTL/2, 30*time.Second)
		if opts.SweepInterval <= 0 {
			opts.SweepInterval = time.Second
		}
	}
	r := &Router{
		opts:     opts,
		backends: make(map[string]*backend),
		sessions: make(map[string]session),
		stop:     make(chan struct{}),
	}
	go r.cleanupLoop()
	return r, nil
}

// AddBackend registers or atomically replaces a backend.
func (r *Router) AddBackend(id string, target *url.URL) error {
	if r.closed.Load() {
		return errors.New("sticky: router is closed")
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("sticky: backend ID cannot be empty")
	}
	if target == nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return errors.New("sticky: backend target must be an absolute HTTP(S) URL")
	}
	targetCopy := *target
	b := &backend{id: id, target: &targetCopy}
	b.proxy = httputil.NewSingleHostReverseProxy(&targetCopy)
	b.transport = http.DefaultTransport.(*http.Transport).Clone()
	b.proxy.Transport = b.transport
	b.proxy.ErrorHandler = r.opts.ErrorHandler
	if b.proxy.ErrorHandler == nil {
		b.proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
		}
	}
	b.proxy.ModifyResponse = func(response *http.Response) error {
		if response.StatusCode/100 != 2 {
			return nil
		}
		requestSID := response.Request.URL.Query().Get("sid")
		if requestSID != "" {
			if response.ContentLength < 0 || response.ContentLength > closePayloadCaptureLimit ||
				response.Header.Get("Content-Encoding") != "" {
				return nil
			}
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(string(body)))
			response.ContentLength = int64(len(body))
			if isEngineClosePayload(body) {
				r.Unbind(requestSID)
			}
			return nil
		}
		query := response.Request.URL.Query()
		if query.Get("EIO") == "" || query.Get("transport") != "polling" {
			return nil
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		response.Body = io.NopCloser(strings.NewReader(string(body)))
		response.ContentLength = int64(len(body))
		if match := sidPattern.FindSubmatch(body); len(match) == 2 {
			r.Bind(string(match[1]), id)
		}
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.backends[id]; existing != nil {
		b.sessions = existing.sessions
		b.webSockets = existing.webSockets
		existing.transport.CloseIdleConnections()
		r.backends[id] = b
		return nil
	}
	r.backends[id] = b
	r.order = append(r.order, id)
	sort.Strings(r.order)
	return nil
}

// RemoveBackend removes a backend and all mappings owned by it.
func (r *Router) RemoveBackend(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.backends[id]; existing != nil {
		existing.transport.CloseIdleConnections()
	}
	delete(r.backends, id)
	for i, candidate := range r.order {
		if candidate == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	for sid, mapped := range r.sessions {
		if mapped.backendID == id {
			delete(r.sessions, sid)
		}
	}
}

// Bind associates sid with backendID. It is safe to call repeatedly.
func (r *Router) Bind(sid, backendID string) bool {
	if sid == "" || backendID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.backends[backendID]
	if b == nil {
		return false
	}
	if previous, ok := r.sessions[sid]; ok {
		if previous.backendID != backendID {
			if old := r.backends[previous.backendID]; old != nil && old.sessions > 0 {
				old.sessions--
			}
			b.sessions++
		}
	} else {
		b.sessions++
	}
	r.sessions[sid] = session{backendID: backendID, lastSeen: time.Now()}
	return true
}

// Unbind removes a session mapping.
func (r *Router) Unbind(sid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unbindLocked(sid)
}

func (r *Router) unbindLocked(sid string) {
	mapped, ok := r.sessions[sid]
	if !ok {
		return
	}
	delete(r.sessions, sid)
	if b := r.backends[mapped.backendID]; b != nil && b.sessions > 0 {
		b.sessions--
	}
}

// BackendForSession returns the current owner of sid.
func (r *Router) BackendForSession(sid string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mapped, ok := r.sessions[sid]
	return mapped.backendID, ok
}

// Stats returns a stable, ID-sorted snapshot.
func (r *Router) Stats() []BackendStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := make([]BackendStats, 0, len(r.order))
	for _, id := range r.order {
		b := r.backends[id]
		if b != nil {
			stats = append(stats, BackendStats{ID: id, Target: b.target.String(), Sessions: b.sessions + b.webSockets})
		}
	}
	return stats
}

// ServeHTTP implements http.Handler, including WebSocket upgrade proxying.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if r.closed.Load() {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	sid := req.URL.Query().Get("sid")
	var closeCapture *closePayloadCapture
	if sid != "" && req.Body != nil && req.Method == http.MethodPost {
		closeCapture = &closePayloadCapture{ReadCloser: req.Body}
		req.Body = closeCapture
	}
	b := r.selectBackend(sid)
	if b == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no Socket.IO backend available", http.StatusServiceUnavailable)
		return
	}
	if r.opts.DebugHeader != "" {
		w.Header().Set(r.opts.DebugHeader, b.id)
	}
	webSocketUpgrade := isWebSocketUpgrade(req)
	if webSocketUpgrade {
		if sid == "" {
			r.changeWebSocketCount(b.id, 1)
			defer r.changeWebSocketCount(b.id, -1)
		} else {
			writer := &upgradeTrackingResponseWriter{ResponseWriter: w}
			b.proxy.ServeHTTP(writer, req)
			if writer.hijacked {
				r.Unbind(sid)
			}
			return
		}
	}
	b.proxy.ServeHTTP(w, req)
	if !webSocketUpgrade && closeCapture != nil && !closeCapture.truncated && isEngineClosePayload(closeCapture.prefix) {
		r.Unbind(sid)
	}
}

func isEngineClosePayload(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	encoded := string(payload)
	for packet := range strings.SplitSeq(encoded, "\x1e") {
		if packet == "1" {
			return true
		}
	}

	// Engine.IO v3 polling payloads use the form `<length>:<packet>`.
	remaining := encoded
	for remaining != "" {
		colon := strings.IndexByte(remaining, ':')
		if colon <= 0 {
			return false
		}
		length, err := strconv.Atoi(remaining[:colon])
		if err != nil || length < 0 || len(remaining[colon+1:]) < length {
			return false
		}
		packet := remaining[colon+1 : colon+1+length]
		if packet == "1" {
			return true
		}
		remaining = remaining[colon+1+length:]
	}
	return false
}

func isWebSocketUpgrade(request *http.Request) bool {
	if !strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket") {
		return false
	}
	for token := range strings.SplitSeq(request.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

func (r *Router) changeWebSocketCount(backendID string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b := r.backends[backendID]; b != nil {
		b.webSockets += delta
		if b.webSockets < 0 {
			b.webSockets = 0
		}
	}
}

func (r *Router) selectBackend(sid string) *backend {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sid != "" {
		if mapped, ok := r.sessions[sid]; ok {
			if b := r.backends[mapped.backendID]; b != nil {
				mapped.lastSeen = time.Now()
				r.sessions[sid] = mapped
				return b
			}
			delete(r.sessions, sid)
		}
	}
	if len(r.order) == 0 {
		return nil
	}
	switch r.opts.LoadBalancingMethod {
	case Random:
		return r.backends[r.order[rand.IntN(len(r.order))]]
	case RoundRobin:
		index := r.next % uint64(len(r.order))
		r.next++
		return r.backends[r.order[index]]
	case LeastConnection:
		var selected *backend
		for _, id := range r.order {
			candidate := r.backends[id]
			if selected == nil || candidate.sessions+candidate.webSockets < selected.sessions+selected.webSockets {
				selected = candidate
			}
		}
		return selected
	default:
		return nil
	}
}

func (r *Router) cleanupLoop() {
	ticker := time.NewTicker(r.opts.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			r.mu.Lock()
			for sid, mapped := range r.sessions {
				if now.Sub(mapped.lastSeen) > r.opts.SessionTTL {
					r.unbindLocked(sid)
				}
			}
			r.mu.Unlock()
		case <-r.stop:
			return
		}
	}
}

// Close stops background cleanup and closes idle backend connections.
func (r *Router) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		close(r.stop)
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, b := range r.backends {
			b.transport.CloseIdleConnections()
			b.sessions = 0
			b.webSockets = 0
		}
		clear(r.sessions)
	})
	return nil
}
