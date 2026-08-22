package socket

import (
	"compress/flate"
	"compress/gzip"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/engine/v4"
	"github.com/aqcool/socket.io/v4/pkg/log"
	"github.com/aqcool/socket.io/v4/pkg/slices"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
	"github.com/klauspost/compress/zstd"
)

var ErrAdapterDoesNotSupportConnectionStateRecovery = errors.New("socket.io: adapter does not support connection state recovery")

const (
	// DefaultConnectTimeout is the default time a client has to send its first namespace connection request.
	DefaultConnectTimeout = 45_000 * time.Millisecond

	// DefaultMaxDisconnectionDuration is the default maximum time a session can be disconnected before being discarded.
	DefaultMaxDisconnectionDuration int64 = 2 * 60 * 1000

	// DefaultSessionCleanupInterval is the default interval between two session cleanup sweeps.
	DefaultSessionCleanupInterval = 60_000 * time.Millisecond

	// EmbeddedClientVersion is the official Socket.IO JavaScript client version
	// served by this module when ServeClient is enabled.
	EmbeddedClientVersion = "4.8.3"
)

var (
	dotMapRegex = regexp.MustCompile(`\.map`)
	serverLog   = log.NewLog("socket.io:server")

	//go:embed client-dist/*
	clientDist embed.FS
)

type (
	ParentNspNameMatchFn *func(string, map[string]any, func(error, bool))

	// Represents a Socket.IO server.
	//
	//	import (
	//		"github.com/aqcool/socket.io/v4/pkg/utils"
	//		"github.com/aqcool/socket.io/servers/socket/v4"
	//	)
	//
	//	io := socket.NewServer(nil, nil)
	//
	//	io.On("connection", func(clients ...any) {
	//		socket := clients[0].(*socket.Socket)
	//
	//		utils.Log().Info(`socket %s connected`, socket.Id())
	//
	//		// send an event to the client
	//		socket.Emit("foo", "bar")
	//
	//		socket.On("foobar", func(...any) {
	//			// an event was received from the client
	//		})
	//
	//		// upon disconnection
	//		socket.On("disconnect", func(reason ...any) {
	//			utils.Log().Info(`socket %s disconnected due to %s`, socket.Id(), reason[0])
	//		})
	//	})
	//	io.Listen(3000, nil)
	Server struct {
		*StrictEventEmitter

		// #readonly
		sockets Namespace
		// A reference to the underlying Engine.IO server.
		//
		//	clientsCount := io.Engine().ClientsCount()
		engine     engine.BaseServer
		_parser    parser.Parser
		encoder    parser.Encoder
		_nsps      *types.Map[string, Namespace]
		parentNsps *types.Map[ParentNspNameMatchFn, ParentNamespace]
		//
		// A subset of the {parentNsps} map, only containing {ParentNamespace} which are based on a regular
		// expression.
		parentNamespacesFromRegExp *types.Map[*regexp.Regexp, ParentNamespace]
		_adapter                   AdapterConstructor
		_serveClient               bool
		// #readonly
		opts            ServerOptionsInterface
		eio             engine.Server
		_path           string
		clientPathRegex *regexp.Regexp
		_connectTimeout time.Duration
		httpServer      *types.HttpServer
		_corsMiddleware engine.Middleware
		stateMu         sync.RWMutex

		// dynamicNamespaceMu makes creation of a child namespace atomic when
		// several clients concurrently match the same parent namespace.
		dynamicNamespaceMu sync.Mutex
	}
)

func MakeServer() *Server {
	s := &Server{
		_nsps:                      &types.Map[string, Namespace]{},
		parentNsps:                 &types.Map[ParentNspNameMatchFn, ParentNamespace]{},
		parentNamespacesFromRegExp: &types.Map[*regexp.Regexp, ParentNamespace]{},
	}
	return s
}

func NewServer(srv any, opts ServerOptionsInterface) *Server {
	s, err := NewServerWithError(srv, opts)
	if err != nil {
		panic(err)
	}
	return s
}

// NewServerWithError creates a server and reports invalid startup
// configuration instead of panicking.
func NewServerWithError(srv any, opts ServerOptionsInterface) (*Server, error) {
	s := MakeServer()

	if err := s.construct(srv, opts); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Sockets() Namespace {
	return s.sockets
}

// Namespaces returns a snapshot of all currently registered namespaces.
//
// The returned slice is safe to iterate while namespaces are added or removed.
func (s *Server) Namespaces() []Namespace {
	namespaces := make([]Namespace, 0, s._nsps.Len())
	s._nsps.Range(func(_ string, namespace Namespace) bool {
		namespaces = append(namespaces, namespace)
		return true
	})
	return namespaces
}

func (s *Server) Engine() engine.BaseServer {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.engine
}

func (s *Server) Encoder() parser.Encoder {
	return s.encoder
}

func (s *Server) Construct(srv any, opts ServerOptionsInterface) {
	if err := s.construct(srv, opts); err != nil {
		panic(err)
	}
}

func (s *Server) construct(srv any, opts ServerOptionsInterface) error {
	if opts == nil {
		opts = DefaultServerOptions()
	}

	if recovery := opts.ConnectionStateRecovery(); recovery != nil {
		if adapter := opts.Adapter(); adapter != nil && !CapabilitiesOf(adapter).ConnectionStateRecovery {
			return fmt.Errorf("%w: %T", ErrAdapterDoesNotSupportConnectionStateRecovery, adapter)
		}
	}

	if opts.GetRawPath() != nil {
		s.SetPath(opts.Path())
	} else {
		s.SetPath("/socket.io")
	}
	if opts.GetRawConnectTimeout() != nil {
		s.SetConnectTimeout(opts.ConnectTimeout())
	} else {
		s.SetConnectTimeout(DefaultConnectTimeout)
	}
	if opts.GetRawServeClient() != nil {
		s.SetServeClient(opts.ServeClient())
	} else {
		// Match the official Server default: browser bundles are served unless
		// the application explicitly disables them.
		s.SetServeClient(true)
	}
	if _parser := opts.Parser(); _parser != nil {
		s._parser = _parser
	} else {
		s._parser = parser.NewParser()
	}
	s.encoder = s._parser.NewEncoder()
	s.opts = opts
	if adapter := opts.Adapter(); adapter != nil {
		s.SetAdapter(adapter)
	} else {
		if connectionStateRecovery := opts.ConnectionStateRecovery(); connectionStateRecovery != nil {
			if connectionStateRecovery.GetRawMaxDisconnectionDuration() == nil {
				connectionStateRecovery.SetMaxDisconnectionDuration(DefaultMaxDisconnectionDuration)
			}
			if connectionStateRecovery.GetRawSkipMiddlewares() == nil {
				connectionStateRecovery.SetSkipMiddlewares(true)
			}
			s.SetAdapter(&SessionAwareAdapterBuilder{})
		} else {
			s.SetAdapter(&AdapterBuilder{})
		}
	}
	s.sockets = s.Of("/", nil)

	s.StrictEventEmitter = s.sockets.EventEmitter()

	if srv != nil {
		s.Attach(srv, nil)
	}

	if cors := s.opts.Cors(); cors != nil {
		s._corsMiddleware = types.MiddlewareWrapper(cors)
	}

	return nil
}

func (s *Server) Opts() ServerOptionsInterface {
	return s.opts
}

// SetServeClient sets whether to serve the client code to browsers.
func (s *Server) SetServeClient(v bool) *Server {
	s._serveClient = v
	return s
}

// ServeClient returns whether the server is serving client code.
func (s *Server) ServeClient() bool {
	return s._serveClient
}

// _checkNamespace executes the middleware for an incoming namespace not already created on the server.
// name is the name of the incoming namespace, auth is the auth parameters, fn is the callback.
func (s *Server) _checkNamespace(name string, auth map[string]any, fn func(nsp Namespace)) {
	type parentMatcher struct {
		match  ParentNspNameMatchFn
		parent ParentNamespace
	}
	matchers := make([]parentMatcher, 0, s.parentNsps.Len())
	s.parentNsps.Range(func(match ParentNspNameMatchFn, parent ParentNamespace) bool {
		matchers = append(matchers, parentMatcher{match: match, parent: parent})
		return true
	})

	var run func(int)
	run = func(index int) {
		if index >= len(matchers) {
			fn(nil)
			return
		}

		matcher := matchers[index]
		var callbackOnce sync.Once
		(*matcher.match)(name, auth, func(err error, allow bool) {
			callbackOnce.Do(func() {
				if err != nil || !allow {
					run(index + 1)
					return
				}

				s.dynamicNamespaceMu.Lock()
				namespace, exists := s._nsps.Load(name)
				if !exists {
					namespace = matcher.parent.CreateChild(name)
					serverLog.Debug("dynamic namespace %s was created", name)
				} else {
					serverLog.Debug("dynamic namespace %s already exists", name)
				}
				s.dynamicNamespaceMu.Unlock()
				fn(namespace)
			})
		})
	}

	run(0)
}

// SetPath sets the client serving path.
func (s *Server) SetPath(v string) *Server {
	s._path = strings.TrimRight(v, "/")
	s.clientPathRegex = regexp.MustCompile(`^` + regexp.QuoteMeta(s._path) + `/socket\.io(\.msgpack|\.esm)?(\.min)?\.js(\.map)?(?:\?|$)`)
	return s
}

// Path returns the current client serving path.
func (s *Server) Path() string {
	return s._path
}

// SetConnectTimeout sets the delay after which a client without namespace is closed.
func (s *Server) SetConnectTimeout(v time.Duration) *Server {
	s._connectTimeout = v
	return s
}

// ConnectTimeout returns the current connect timeout duration.
func (s *Server) ConnectTimeout() time.Duration {
	return s._connectTimeout
}

// SetAdapter sets the adapter for rooms.
func (s *Server) SetAdapter(v AdapterConstructor) *Server {
	s._adapter = v
	s._nsps.Range(func(_ string, nsp Namespace) bool {
		nsp.InitAdapter()
		return true
	})
	return s
}

func (s *Server) Adapter() AdapterConstructor {
	return s._adapter
}

// Listen attaches socket.io to a server or port.
// srv is the server or port, opts are options passed to engine.io.
func (s *Server) Listen(srv any, opts *ServerOptions) *Server {
	return s.Attach(srv, opts)
}

// Attach attaches socket.io to a server or port.
// srv is the server or port, opts are options passed to engine.io.
func (s *Server) Attach(srv any, opts *ServerOptions) *Server {
	var server *types.HttpServer
	switch address := srv.(type) {
	case int:
		_address := fmt.Sprintf(":%d", address)
		// handle a port as a int
		serverLog.Debug("creating http server and binding to %s", _address)
		server = types.NewWebServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "404 page not found", http.StatusNotFound)
		}))
		server.Listen(_address, nil)
	case string:
		// handle a port as a string
		serverLog.Debug("creating http server and binding to %s", address)
		server = types.NewWebServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "404 page not found", http.StatusNotFound)
		}))
		server.Listen(address, nil)
	case *types.HttpServer:
		server = address
	default:
		panic(fmt.Errorf("trying to attach socket.io to express request handler %T, please pass a *types.HttpServer instance", address))
	}
	if opts == nil {
		opts = DefaultServerOptions()
	}

	// merge the options passed to the Socket.IO server
	opts.Assign(s.opts)
	// set engine.io path to `/socket.io`
	if opts.GetRawPath() == nil {
		opts.SetPath(s._path)
	}
	s.initEngine(server, opts)

	return s
}

// ServeHandler returns an http.Handler for the server.
func (s *Server) ServeHandler(opts *ServerOptions) http.Handler {
	// If an instance already exists, reuse it.
	s.stateMu.RLock()
	existing := s.eio
	s.stateMu.RUnlock()
	if existing != nil {
		return s.wrapServeHandler(existing)
	}

	if opts == nil {
		opts = DefaultServerOptions()
	}

	// merge the options passed to the Socket.IO server
	opts.Assign(s.opts)
	// set engine.io path to `/socket.io`
	if opts.GetRawPath() == nil {
		opts.SetPath(s._path)
	}

	// initialize engine
	serverLog.Debug("creating http.Handler-based engine with opts %v", opts)
	eio := engine.NewServer(opts)
	created := false
	s.stateMu.Lock()
	if s.eio == nil {
		s.eio = eio
		created = true
	} else {
		eio = s.eio
	}
	s.stateMu.Unlock()
	// bind to engine events
	if created {
		s.Bind(eio)
	}

	return s.wrapServeHandler(eio)
}

func (s *Server) wrapServeHandler(eio engine.Server) http.Handler {
	if !s._serveClient {
		return eio
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.clientPathRegex.MatchString(r.URL.Path) {
			if s._corsMiddleware != nil {
				s._corsMiddleware(types.NewHttpContext(w, r), func(error) {
					s.serve(w, r)
				})
				return
			}
			s.serve(w, r)
			return
		}
		eio.ServeHTTP(w, r)
	})
}

// initEngine initializes the engine.io server and attaches it to the HTTP server.
func (s *Server) initEngine(srv *types.HttpServer, opts ServerOptionsInterface) {
	// initialize engine
	serverLog.Debug("creating engine.io instance with opts %+v", opts)
	eio := engine.Attach(srv, opts)

	// attach static file serving
	if s._serveClient {
		s.attachServe(srv, eio, opts)
	}

	// Export http server
	s.stateMu.Lock()
	s.eio = eio
	s.httpServer = srv
	s.stateMu.Unlock()

	// bind to engine events
	s.Bind(eio)
}

// attachServe attaches the static file serving handler.
func (s *Server) attachServe(srv *types.HttpServer, egs engine.Server, opts ServerOptionsInterface) {
	serverLog.Debug("attaching client serving req handler")
	srv.HandleFunc(s._path+"/", func(w http.ResponseWriter, r *http.Request) {
		if s.clientPathRegex.MatchString(r.URL.Path) {
			if s._corsMiddleware != nil {
				s._corsMiddleware(types.NewHttpContext(w, r), func(error) {
					s.serve(w, r)
				})
			} else {
				s.serve(w, r)
			}
		} else {
			if opts.GetRawAddTrailingSlash() == nil || opts.AddTrailingSlash() {
				egs.ServeHTTP(w, r)
			} else {
				srv.DefaultHandler.ServeHTTP(w, r)
			}
		}
	})
}

// serve handles a request for serving client source and map files.
func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	filename := filepath.Base(r.URL.Path)
	isMap := dotMapRegex.MatchString(filename)
	_type := "source"
	if isMap {
		_type = "map"
	}
	// Per the standard, ETags must be quoted:
	// https://tools.ietf.org/html/rfc7232#section-2.3
	expectedEtag := `"` + EmbeddedClientVersion + `"`
	if s.opts.GetRawClientVersion() != nil {
		expectedEtag = `"` + s.opts.ClientVersion() + `"`
	}
	weakEtag := "W/" + expectedEtag
	w.Header().Set("Cache-Control", "public, max-age=0")
	w.Header().Set("ETag", expectedEtag)

	if etag := r.Header.Get("If-None-Match"); etag != "" {
		if expectedEtag == etag || weakEtag == etag {
			serverLog.Debug("serve client %s 304", _type)
			w.WriteHeader(http.StatusNotModified)
			_, _ = w.Write(nil)
			return
		}
	}

	serverLog.Debug("serve client %s", _type)
	if isMap {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	}
	s.sendFile(filename, w, r)
}

// sendFile sends a static file to the client.
func (*Server) sendFile(filename string, w http.ResponseWriter, r *http.Request) {
	file, err := clientDist.Open("client-dist/" + filename)
	if err != nil {
		serverLog.Debug("File read failed: %v", err)
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer func() { _ = file.Close() }()

	// Get file size for Content-Length in uncompressed responses
	fi, statErr := file.Stat()
	if statErr != nil {
		serverLog.Debug("File stat failed: %v", statErr)
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	encoding := utils.Contains(r.Header.Get("Accept-Encoding"), []string{"gzip", "deflate", "br", "zstd"})
	if encoding != "" {
		w.Header().Add("Vary", "Accept-Encoding")
	}

	switch encoding {
	case "br":
		br := brotli.NewWriterLevel(w, brotli.DefaultCompression)
		defer func() { _ = br.Close() }()
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(br, file)
	case "gzip":
		gz, err := gzip.NewWriterLevel(w, gzip.DefaultCompression)
		if err != nil {
			serverLog.Debug("Failed to compress data: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = gz.Close() }()
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(gz, file)
	case "deflate":
		fl, err := flate.NewWriter(w, flate.DefaultCompression)
		if err != nil {
			serverLog.Debug("Failed to compress data: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = fl.Close() }()
		w.Header().Set("Content-Encoding", "deflate")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(fl, file)
	case "zstd":
		zd, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			serverLog.Debug("Failed to compress data: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		defer func() { _ = zd.Close() }()
		w.Header().Set("Content-Encoding", "zstd")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(zd, file)
	default:
		w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, file)
	}
}

// Bind binds socket.io to an engine.io instance.
// egs is the engine.io (or compatible) server.
func (s *Server) Bind(egs engine.BaseServer) *Server {
	s.stateMu.Lock()
	s.engine = egs
	s.stateMu.Unlock()
	_ = egs.On("connection", s.onconnection)
	s.EmitReserved("engine_initialized", egs)
	return s
}

// onconnection is called with each incoming transport connection.
func (s *Server) onconnection(conns ...any) {
	conn := slices.TryGetAny[engine.Socket](conns, 0)
	serverLog.Debug("incoming connection with id %s", conn.Id())
	client := NewClient(s, conn)
	if conn.Protocol() == 3 {
		client.connect("/", nil)
	}
}

// Of looks up a namespace by name or pattern and optionally registers a connection event handler.
// name can be a string, regexp, or ParentNspNameMatchFn; fn is the connection event handler.
func (s *Server) Of(name any, fn types.EventListener) Namespace {
	switch n := name.(type) {
	case ParentNspNameMatchFn:
		parentNsp := NewParentNamespace(s)
		serverLog.Debug("initializing parent namespace %s", parentNsp.Name())

		s.parentNsps.Store(n, parentNsp)

		if fn != nil {
			_ = parentNsp.On("connect", fn)
		}
		return parentNsp
	case *regexp.Regexp:
		parentNsp := NewParentNamespace(s)
		serverLog.Debug("initializing parent namespace %s", parentNsp.Name())

		s.parentNsps.Store(ParentNspNameMatchFn(utils.Ptr(func(nsp string, _ map[string]any, next func(error, bool)) {
			next(nil, n.MatchString(nsp))
		})), parentNsp)
		s.parentNamespacesFromRegExp.Store(n, parentNsp)

		if fn != nil {
			_ = parentNsp.On("connect", fn)
		}
		return parentNsp
	}

	n, ok := name.(string)
	if ok {
		if len(n) > 0 {
			if n[0] != '/' {
				n = "/" + n
			}
		} else {
			n = "/"
		}
	} else {
		n = "/"
	}

	var namespace Namespace

	if nsp, ok := s._nsps.Load(n); ok {
		namespace = nsp
	} else {
		s.parentNamespacesFromRegExp.Range(func(regex *regexp.Regexp, parentNamespace ParentNamespace) bool {
			if regex.MatchString(n) {
				serverLog.Debug("attaching namespace %s to parent namespace %s", n, regex.String())
				namespace = parentNamespace.CreateChild(n)
				return false
			}
			return true
		})

		if namespace != nil {
			return namespace
		}

		serverLog.Debug("initializing namespace %s", n)
		namespace = NewNamespace(s, n)
		s._nsps.Store(n, namespace)
		if n != "/" {
			s.sockets.EmitReserved("new_namespace", namespace)
		}
	}

	if fn != nil {
		_ = namespace.On("connect", fn)
	}
	return namespace
}

// Close closes the server and all client connections. If fn is provided, it is called on error or when all connections are closed.
func (s *Server) Close(fn func(error)) {
	s._nsps.Range(func(_ string, nsp Namespace) bool {
		nsp.Sockets().Range(func(_ SocketId, socket *Socket) bool {
			socket._onclose("server shutting down")
			return true
		})
		nsp.Adapter().Close()
		return true
	})

	// The official server always closes Engine.IO before closing the HTTP
	// listener. This also clears clients and connection timers when the HTTP
	// server was never started and its Close operation returns an error.
	s.stateMu.RLock()
	engineServer := s.engine
	httpServer := s.httpServer
	s.stateMu.RUnlock()
	if engineServer != nil {
		engineServer.Close()
	}

	if httpServer != nil {
		_ = httpServer.Close(fn)
		return
	}

	if fn != nil {
		fn(nil)
	}
}

// Use registers a middleware function that is executed for every incoming Socket.
func (s *Server) Use(fn NamespaceMiddleware) *Server {
	s.sockets.Use(fn)
	return s
}

// To targets a room when emitting events. Returns a new BroadcastOperator for chaining.
func (s *Server) To(room ...Room) *BroadcastOperator {
	return s.sockets.To(room...)
}

// In targets a room when emitting events. Returns a new BroadcastOperator for chaining.
func (s *Server) In(room ...Room) *BroadcastOperator {
	return s.sockets.In(room...)
}

// Except excludes a room when emitting events. Returns a new BroadcastOperator for chaining.
func (s *Server) Except(room ...Room) *BroadcastOperator {
	return s.sockets.Except(room...)
}

// Emit broadcasts an event to all connected clients.
func (s *Server) Emit(ev string, args ...any) *Server {
	if err := s.sockets.Emit(ev, args...); err != nil {
		// Match the official Server API: emitting a reserved event is a
		// programmer error and must not be silently ignored.
		panic(err)
	}
	return s
}

// Send sends a "message" event to all clients. This mimics the WebSocket.send() method.
func (s *Server) Send(args ...any) *Server {
	// This type-cast is needed because EmitEvents likely doesn't have `message` as a key.
	// if you specify the EmitEvents, the type of args will be never.
	_ = s.sockets.Emit("message", args...)
	return s
}

// Write sends a "message" event to all clients. Alias of Send.
func (s *Server) Write(args ...any) *Server {
	// This type-cast is needed because EmitEvents likely doesn't have `message` as a key.
	// if you specify the EmitEvents, the type of args will be never.
	_ = s.sockets.Emit("message", args...)
	return s
}

// ServerSideEmit sends a message to other Socket.IO servers in the cluster.
// ev is the event name, args are the arguments (may include an acknowledgement callback).
func (s *Server) ServerSideEmit(ev string, args ...any) error {
	return s.sockets.ServerSideEmit(ev, args...)
}

// ServerSideEmitWithAck sends a message and expects an acknowledgement from other Socket.IO servers in the cluster.
// Returns a function that will be fulfilled when all servers have acknowledged the event.
func (s *Server) ServerSideEmitWithAck(ev string, args ...any) func(Ack) error {
	return s.sockets.ServerSideEmitWithAck(ev, args...)
}

// Compress sets the compress flag for subsequent event emissions.
func (s *Server) Compress(compress bool) *BroadcastOperator {
	return s.sockets.Compress(compress)
}

// Volatile sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to receive messages.
func (s *Server) Volatile() *BroadcastOperator {
	return s.sockets.Volatile()
}

// Local sets a modifier for a subsequent event emission that the event data will only be broadcast to the current node.
func (s *Server) Local() *BroadcastOperator {
	return s.sockets.Local()
}

// Timeout adds a timeout for the next operation.
func (s *Server) Timeout(timeout time.Duration) *BroadcastOperator {
	return s.sockets.Timeout(timeout)
}

// FetchSockets returns a function to fetch the matching socket instances.
func (s *Server) FetchSockets() func(func([]*RemoteSocket, error)) {
	return s.sockets.FetchSockets()
}

// AllSockets returns the IDs of sockets in the default namespace.
//
// Deprecated: use FetchSockets instead.
func (s *Server) AllSockets() func(func(*types.Set[SocketId], error)) {
	return s.sockets.AllSockets()
}

// CountSockets returns the number of sockets in the default namespace.
func (s *Server) CountSockets() func(func(uint64, error)) {
	return s.sockets.CountSockets()
}

// ListRooms returns room names and socket counts in the default namespace.
func (s *Server) ListRooms() func(func(map[Room]uint64, error)) {
	return s.sockets.ListRooms()
}

// SocketsJoin makes the matching socket instances join the specified rooms.
func (s *Server) SocketsJoin(room ...Room) {
	s.sockets.SocketsJoin(room...)
}

// SocketsLeave makes the matching socket instances leave the specified rooms.
func (s *Server) SocketsLeave(room ...Room) {
	s.sockets.SocketsLeave(room...)
}

// DisconnectSockets makes the matching socket instances disconnect. If status is true, closes the underlying connection.
func (s *Server) DisconnectSockets(status bool) {
	s.sockets.DisconnectSockets(status)
}
