package socketio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	core "github.com/aqcool/socket.io/servers/socket/v4"
)

type Server struct {
	raw *core.Server
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	doneOnce sync.Once

	lifecycleMu sync.Mutex
	serving     *http.Server
	closing     bool
	closed      bool

	namespaceMu       sync.Mutex
	namespaceCreateMu sync.Mutex
	namespaces        map[string]*Namespace

	socketMu sync.Mutex
	sockets  map[*core.Socket]*Socket

	hub *eventHub
}

func New(options ...Option) (*Server, error) {
	cfg := DefaultConfig()
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}

	// v4 starts by reusing the native v4 protocol core. Provider adapters and
	// custom packet codecs move to the native v4 contracts in the adapter/parser
	// migration phases rather than being hidden behind an unsafe method-shape bridge.
	if cfg.Adapter != nil {
		return nil, fmt.Errorf("%w: native v4 adapters are not wired to the native core backend yet", ErrUnsupported)
	}
	if cfg.PacketCodec != nil {
		return nil, fmt.Errorf("%w: native v4 packet codecs are not wired to the native core backend yet", ErrUnsupported)
	}

	coreOptions := core.DefaultServerOptions()
	coreOptions.SetPath(cfg.Path)
	coreOptions.SetServeClient(cfg.ServeClient)
	coreOptions.SetConnectTimeout(cfg.ConnectTimeout)
	coreOptions.SetCleanupEmptyChildNamespaces(cfg.CleanupEmptyChildNamespaces)
	if cfg.Recovery != nil {
		recovery := core.DefaultConnectionStateRecovery()
		recovery.SetMaxDisconnectionDuration(int64(cfg.Recovery.MaxDisconnectionDuration / time.Millisecond))
		recovery.SetSkipMiddlewares(cfg.Recovery.SkipMiddleware)
		recovery.SetSessionCleanupInterval(cfg.Recovery.CleanupInterval)
		coreOptions.SetConnectionStateRecovery(recovery)
	}

	raw, err := core.NewServerWithError(nil, coreOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		raw:        raw,
		cfg:        cfg,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		namespaces: make(map[string]*Namespace),
		sockets:    make(map[*core.Socket]*Socket),
	}
	server.hub = newEventHub(
		func(event string, listener func(...any)) error {
			return raw.On(event, listener)
		},
		func() context.Context { return server.ctx },
		server.transformArgs,
		cfg.Logger,
	)
	server.wrapNamespace(raw.Sockets())
	return server, nil
}

func (s *Server) Config() Config {
	if s == nil {
		return Config{}
	}
	cfg := s.cfg
	if s.cfg.Recovery != nil {
		recovery := *s.cfg.Recovery
		cfg.Recovery = &recovery
	}
	return cfg
}

func (s *Server) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Server) Handler() http.Handler {
	if s == nil || s.raw == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, ErrClosed.Error(), http.StatusServiceUnavailable)
		})
	}
	return s.raw.ServeHandler(nil)
}

func (s *Server) Serve(listener net.Listener) error {
	if s == nil || listener == nil {
		return fmt.Errorf("%w: listener is required", ErrInvalidArgument)
	}

	s.lifecycleMu.Lock()
	if s.closed || s.closing {
		s.lifecycleMu.Unlock()
		return ErrClosed
	}
	if s.serving != nil {
		s.lifecycleMu.Unlock()
		return ErrAlreadyServing
	}
	httpServer := &http.Server{Handler: s.Handler()}
	s.serving = httpServer
	s.lifecycleMu.Unlock()

	err := httpServer.Serve(listener)

	s.lifecycleMu.Lock()
	if s.serving == httpServer {
		s.serving = nil
	}
	wasClosing := s.closing || s.closed
	s.lifecycleMu.Unlock()

	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	if err != nil && !wasClosing {
		s.cancel()
		s.raw.Close(nil)
		s.lifecycleMu.Lock()
		s.closed = true
		s.lifecycleMu.Unlock()
		s.markDone()
	}
	return err
}

func (s *Server) ListenAndServe(addr string) error {
	if s == nil {
		return ErrClosed
	}
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("%w: address is required", ErrInvalidArgument)
	}

	s.lifecycleMu.Lock()
	closed := s.closed || s.closing
	s.lifecycleMu.Unlock()
	if closed {
		return ErrClosed
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.shutdown(ctx, false)
}

func (s *Server) Close() error {
	return s.shutdown(context.Background(), true)
}

func (s *Server) shutdown(ctx context.Context, force bool) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()
		return nil
	}
	if s.closing {
		done := s.done
		s.lifecycleMu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.closing = true
	httpServer := s.serving
	s.lifecycleMu.Unlock()

	// Cancel socket-derived contexts before waiting on transports or user HTTP
	// handlers so downstream DB/RPC work can stop promptly.
	s.cancel()

	var firstErr error
	if httpServer != nil {
		var err error
		if force {
			err = httpServer.Close()
		} else {
			err = httpServer.Shutdown(ctx)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			firstErr = err
		}
	}

	var coreErr error
	s.raw.Close(func(err error) {
		coreErr = err
	})
	if firstErr == nil && coreErr != nil {
		firstErr = coreErr
	}

	s.lifecycleMu.Lock()
	s.serving = nil
	s.closed = true
	s.closing = false
	s.lifecycleMu.Unlock()
	s.markDone()
	return firstErr
}

func (s *Server) Done() <-chan struct{} {
	if s == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return s.done
}

func (s *Server) markDone() {
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *Server) On(event string, listener Listener) Subscription {
	if s == nil {
		return closedSubscription{}
	}
	return s.hub.On(event, listener)
}

func (s *Server) Once(event string, listener Listener) Subscription {
	if s == nil {
		return closedSubscription{}
	}
	return s.hub.Once(event, listener)
}

func (s *Server) RemoveAllListeners(event string) {
	if s != nil {
		s.hub.RemoveAll(event)
	}
}

func (s *Server) OnConnection(fn func(*Socket)) Subscription {
	if fn == nil {
		return closedSubscription{}
	}
	return s.On("connection", func(_ context.Context, args ...any) error {
		if len(args) > 0 {
			if socket, ok := args[0].(*Socket); ok {
				fn(socket)
			}
		}
		return nil
	})
}

func (s *Server) Use(middleware ...Middleware) {
	if s == nil {
		return
	}
	s.Of("/").Use(middleware...)
}

func (s *Server) Emit(event string, args ...any) error {
	if s == nil || s.raw == nil {
		return ErrClosed
	}
	return s.raw.Sockets().Emit(event, args...)
}

func (s *Server) To(rooms ...Room) *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().To(toLegacyRooms(rooms)...), nil)
}

func (s *Server) In(rooms ...Room) *BroadcastOperator {
	return s.To(rooms...)
}

func (s *Server) Except(rooms ...Room) *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().Except(toLegacyRooms(rooms)...), nil)
}

func (s *Server) Volatile() *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().Volatile(), nil)
}

func (s *Server) Local() *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().Local(), nil)
}

func (s *Server) Compress(enabled bool) *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().Compress(enabled), nil)
}

func (s *Server) Timeout(timeout time.Duration) *BroadcastOperator {
	if s == nil {
		return nil
	}
	return newBroadcastOperator(s, s.raw.Sockets().Timeout(timeout), &timeout)
}

func (s *Server) Of(name string) *Namespace {
	if s == nil || s.raw == nil {
		return nil
	}
	name = normalizeNamespace(name)

	s.namespaceMu.Lock()
	if namespace := s.namespaces[name]; namespace != nil {
		s.namespaceMu.Unlock()
		return namespace
	}
	s.namespaceMu.Unlock()

	// The create lock makes concurrent Of("/same") calls converge without
	// holding namespaceMu while the v3 core synchronously emits new_namespace.
	s.namespaceCreateMu.Lock()
	defer s.namespaceCreateMu.Unlock()

	s.namespaceMu.Lock()
	if namespace := s.namespaces[name]; namespace != nil {
		s.namespaceMu.Unlock()
		return namespace
	}
	s.namespaceMu.Unlock()

	raw := s.raw.Of(name, nil)
	return s.wrapNamespace(raw)
}

func (s *Server) Namespace(name string) (*Namespace, bool) {
	if s == nil || s.raw == nil {
		return nil, false
	}
	name = normalizeNamespace(name)

	s.namespaceMu.Lock()
	if namespace := s.namespaces[name]; namespace != nil {
		s.namespaceMu.Unlock()
		return namespace, true
	}
	s.namespaceMu.Unlock()

	for _, raw := range s.raw.Namespaces() {
		if raw != nil && raw.Name() == name {
			return s.wrapNamespace(raw), true
		}
	}
	return nil, false
}

func (s *Server) Namespaces() []*Namespace {
	if s == nil || s.raw == nil {
		return nil
	}
	rawNamespaces := s.raw.Namespaces()
	result := make([]*Namespace, 0, len(rawNamespaces))
	for _, raw := range rawNamespaces {
		if raw != nil {
			result = append(result, s.wrapNamespace(raw))
		}
	}
	return result
}

func (s *Server) OfMatch(matcher NamespaceMatcher) *ParentNamespace {
	if s == nil || s.raw == nil || matcher == nil {
		return nil
	}
	coreMatcher := func(name string, auth map[string]any, next func(error, bool)) {
		ctx := s.Context()
		if err := ctx.Err(); err != nil {
			next(err, false)
			return
		}
		allow, err := matcher(ctx, name, auth)
		next(err, allow)
	}
	key := core.ParentNspNameMatchFn(&coreMatcher)
	raw := s.raw.Of(key, nil)
	parent, ok := raw.(core.ParentNamespace)
	if !ok {
		return nil
	}
	return &ParentNamespace{
		Namespace: s.wrapNamespace(parent),
		raw:       parent,
	}
}

func (s *Server) wrapNamespace(raw core.Namespace) *Namespace {
	if s == nil || raw == nil {
		return nil
	}
	name := normalizeNamespace(raw.Name())
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if namespace := s.namespaces[name]; namespace != nil {
		return namespace
	}
	namespace := newNamespace(s, raw)
	s.namespaces[name] = namespace
	return namespace
}

func (s *Server) transformArgs(_ string, args []any) []any {
	result := append([]any(nil), args...)
	for i, value := range result {
		if raw, ok := value.(*core.Socket); ok {
			result[i] = s.wrapSocket(raw)
		}
	}
	return result
}

func (s *Server) DecodeValue(src any, dst any) error {
	if s == nil || s.cfg.ValueCodec == nil {
		return ErrUnsupported
	}
	return s.cfg.ValueCodec.Decode(src, dst)
}

func normalizeNamespace(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "/"
	}
	if !strings.HasPrefix(name, "/") {
		return "/" + name
	}
	return name
}

func toLegacyRooms(rooms []Room) []core.Room {
	result := make([]core.Room, len(rooms))
	for i, room := range rooms {
		result[i] = core.Room(room)
	}
	return result
}
