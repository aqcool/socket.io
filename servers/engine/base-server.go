// Package engine provides the core Engine.IO server implementation, including base server logic, protocol error handling, and middleware management.
package engine

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/servers/engine/v3/errors"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

var (
	serverLog = log.NewLog("engine")

	// Protocol errors mappings.
	UNKNOWN_TRANSPORT            = &types.CodeMessage{Code: 0, Message: `Transport unknown`}
	UNKNOWN_SID                  = &types.CodeMessage{Code: 1, Message: `Session ID unknown`}
	BAD_HANDSHAKE_METHOD         = &types.CodeMessage{Code: 2, Message: `Bad handshake method`}
	BAD_REQUEST                  = &types.CodeMessage{Code: 3, Message: `Bad request`}
	FORBIDDEN                    = &types.CodeMessage{Code: 4, Message: `Forbidden`}
	UNSUPPORTED_PROTOCOL_VERSION = &types.CodeMessage{Code: 5, Message: `Unsupported protocol version`}
)

type baseServer struct {
	types.EventEmitter

	// Prototype interface, used to implement interface method rewriting
	_proto_ BaseServer

	opts config.ServerOptionsInterface

	transports        *types.Set[string]       // Available transport types
	_transportsByName map[string]TransportCtor // Transport constructors by name

	clients      *types.Map[string, Socket]
	clientsCount atomic.Uint64
	middlewares  []Middleware
	middlewareMu sync.RWMutex
}

type transportStarter interface {
	Start()
}

func startTransport(transport transports.Transport) {
	if starter, ok := transport.(transportStarter); ok {
		starter.Start()
	}
}

func MakeBaseServer() BaseServer {
	baseServer := &baseServer{
		EventEmitter: types.NewEventEmitter(),

		clients: &types.Map[string, Socket]{},
	}

	baseServer.Prototype(baseServer)

	return baseServer
}

func (bs *baseServer) Prototype(server BaseServer) {
	bs._proto_ = server
}

func (bs *baseServer) Proto() BaseServer {
	return bs._proto_
}

func (bs *baseServer) Opts() config.ServerOptionsInterface {
	return bs.opts
}

func (bs *baseServer) Clients() *types.Map[string, Socket] {
	return bs.clients
}

func (bs *baseServer) ClientsCount() uint64 {
	return bs.clientsCount.Load()
}

func (bs *baseServer) Middlewares() []Middleware {
	bs.middlewareMu.RLock()
	defer bs.middlewareMu.RUnlock()
	middlewares := make([]Middleware, len(bs.middlewares))
	copy(middlewares, bs.middlewares)
	return middlewares
}

func (bs *baseServer) Transports() *types.Set[string] {
	return bs.transports
}

func (bs *baseServer) TransportsByName() map[string]transports.TransportCtor {
	return bs._transportsByName
}

// BaseServer build.
func (bs *baseServer) Construct(opt any) {
	opts, _ := opt.(config.ServerOptionsInterface)

	options := config.DefaultServerOptions()
	options.SetPingTimeout(20_000 * time.Millisecond)
	options.SetPingInterval(25_000 * time.Millisecond)
	options.SetUpgradeTimeout(10_000 * time.Millisecond)
	options.SetMaxHttpBufferSize(1e6)
	options.SetIdleTimeout(120 * time.Second)
	options.SetTransports(types.NewSet(Polling, WebSocket))
	options.SetAllowUpgrades(true)
	options.SetHttpCompression(&types.HttpCompression{Threshold: 1024})
	options.SetCors(nil)
	options.SetAllowEIO3(false)

	bs.opts = options.Assign(opts)
	bs.snapshotInitialPacket()

	bs.transports = types.NewSet[string]()
	bs._transportsByName = map[string]TransportCtor{}
	if transports := bs.opts.Transports(); transports != nil {
		for _, transport := range transports.Keys() {
			transportName := transport.Name()
			bs.transports.Add(transportName)
			bs._transportsByName[transportName] = transport
		}
	}

	if opts != nil {
		if cookie := opts.Cookie(); cookie != nil {
			cookie = cloneCookie(cookie)
			if len(cookie.Name) == 0 {
				cookie.Name = "io"
			}
			cookie.Path = "/"
			cookie.HttpOnly = true
			if cookieOptions, ok := bs.opts.(interface {
				GetRawCookiePath() types.Optional[string]
				CookiePath() string
				GetRawCookieHttpOnly() types.Optional[bool]
				CookieHttpOnly() bool
			}); ok {
				if cookieOptions.GetRawCookiePath() != nil {
					cookie.Path = cookieOptions.CookiePath()
				} else if opts.Cookie().Path != "" {
					cookie.Path = opts.Cookie().Path
				}
				if cookieOptions.GetRawCookieHttpOnly() != nil {
					cookie.HttpOnly = cookieOptions.CookieHttpOnly()
				}
			} else if opts.Cookie().Path != "" {
				cookie.Path = opts.Cookie().Path
			}
			if cookie.SameSite == 0 || cookie.SameSite == http.SameSiteDefaultMode {
				cookie.SameSite = http.SameSiteLaxMode
			}
			bs.opts.SetCookie(cookie)
		}
	}

	if cors := bs.opts.Cors(); cors != nil {
		bs.Use(types.MiddlewareWrapper(cors))
	}

	bs._proto_.Init()
}

// snapshotInitialPacket turns the caller-owned, potentially one-shot reader
// into an immutable template. Each connection clones this template before
// encoding it, so concurrent and subsequent handshakes receive the same
// initial packet.
func (bs *baseServer) snapshotInitialPacket() {
	initialPacket := bs.opts.InitialPacket()
	if initialPacket == nil {
		return
	}

	switch value := initialPacket.(type) {
	case types.BufferInterface:
		bs.opts.SetInitialPacket(value.Clone())
	case *strings.Reader:
		reader := *value
		data, err := io.ReadAll(&reader)
		if err != nil {
			serverLog.Debug("reading initial packet: %s", err)
		}
		bs.opts.SetInitialPacket(types.NewStringBuffer(data))
	case *bytes.Reader:
		reader := *value
		data, err := io.ReadAll(&reader)
		if err != nil {
			serverLog.Debug("reading initial packet: %s", err)
		}
		bs.opts.SetInitialPacket(types.NewBytesBuffer(data))
	case *bytes.Buffer:
		bs.opts.SetInitialPacket(types.NewBytesBuffer(append([]byte(nil), value.Bytes()...)))
	default:
		data, err := io.ReadAll(initialPacket)
		if err != nil {
			serverLog.Debug("reading initial packet: %s", err)
		}
		bs.opts.SetInitialPacket(types.NewBytesBuffer(data))
	}
}

// abstract
func (bs *baseServer) Init() {
}

// Compute the pathname of the requests that are handled by the server
func (bs *baseServer) ComputePath(options config.AttachOptionsInterface) string {
	path := "/engine.io"

	if options != nil {
		if options.GetRawPath() != nil {
			path = strings.TrimRight(options.Path(), "/")
		}
	}
	if options == nil || options.GetRawAddTrailingSlash() == nil || options.AddTrailingSlash() {
		// The official default is addTrailingSlash=true. Keeping the slash on
		// the registered route also makes the Go mux treat it as a prefix, so
		// paths such as /engine.io/default/ reach Engine.IO as they do in Node.
		path += "/"
	}

	return path
}

// Returns a list of available transports for upgrade given a certain transport.
func (bs *baseServer) Upgrades(transport string) []string {
	if !bs.opts.AllowUpgrades() {
		return nil
	}
	ctor, ok := bs._transportsByName[transport]
	if !ok {
		return nil
	}
	return ctor.UpgradesTo()
}

// protocolVersion returns the explicitly requested Engine.IO protocol version.
// Engine.IO clients must send EIO on the handshake and every follow-up request.
func (bs *baseServer) protocolVersion(ctx *types.HttpContext) (int, bool) {
	values, exists := ctx.Query().All()["EIO"]
	if !exists || len(values) != 1 {
		return 0, false
	}
	switch values[0] {
	case "4":
		return 4, true
	case "3":
		return 3, bs.opts.AllowEIO3()
	default:
		return 0, false
	}
}

func unsupportedProtocolContext(ctx *types.HttpContext) map[string]any {
	value := ctx.Query().Peek("EIO")
	if protocol, err := strconv.Atoi(value); err == nil {
		return map[string]any{"protocol": protocol}
	}
	return map[string]any{"protocol": value}
}

// Verifies a request.
func (bs *baseServer) Verify(ctx *types.HttpContext, upgrade bool) (*types.CodeMessage, map[string]any) {
	// transport check
	transport := ctx.Query().Peek("transport")
	if !bs.transports.Has(transport) || transport == transports.WEBTRANSPORT {
		serverLog.Debug(`unknown transport "%s"`, transport)
		return UNKNOWN_TRANSPORT, map[string]any{"transport": transport}
	}

	// 'Origin' header check
	if origin := ctx.Headers().Peek("Origin"); utils.CheckInvalidHeaderChar(origin) {
		ctx.Headers().Remove("Origin")
		serverLog.Debug("origin header invalid")
		return BAD_REQUEST, map[string]any{"name": "INVALID_ORIGIN", "origin": origin}
	}

	protocol, supported := bs.protocolVersion(ctx)
	if !supported {
		serverLog.Debug(`unsupported protocol version "%s"`, ctx.Query().Peek("EIO"))
		return UNSUPPORTED_PROTOCOL_VERSION, unsupportedProtocolContext(ctx)
	}

	// sid check
	sid := ctx.Query().Peek("sid")
	if len(sid) > 0 {
		// Validate SID format to prevent abuse (e.g. excessively long values)
		if !utils.IsValidSid(sid) {
			serverLog.Debug(`invalid sid format "%s"`, sid)
			return BAD_REQUEST, map[string]any{"name": "INVALID_SID", "sid": sid}
		}
		socket, ok := bs.clients.Load(sid)
		if !ok {
			serverLog.Debug(`unknown sid "%s"`, sid)
			return UNKNOWN_SID, map[string]any{"sid": sid}
		}
		if socket.Protocol() != protocol {
			serverLog.Debug(`protocol mismatch for sid "%s": got %d, expected %d`, sid, protocol, socket.Protocol())
			return UNSUPPORTED_PROTOCOL_VERSION, map[string]any{
				"name":     "PROTOCOL_MISMATCH",
				"protocol": protocol,
				"expected": socket.Protocol(),
			}
		}
		if previousTransport := socket.Transport().Name(); !upgrade && previousTransport != transport {
			serverLog.Debug("bad request: unexpected transport without upgrade")
			return BAD_REQUEST, map[string]any{"name": "TRANSPORT_MISMATCH", "transport": transport, "previousTransport": previousTransport}
		}
	} else {
		// handshake is GET only
		if method := ctx.Method(); method != http.MethodGet {
			return BAD_HANDSHAKE_METHOD, map[string]any{"method": method}
		}

		if transport == transports.WEBSOCKET && !upgrade {
			serverLog.Debug("invalid transport upgrade")
			return BAD_REQUEST, map[string]any{"name": "TRANSPORT_HANDSHAKE_ERROR"}
		}

		if allowRequest := bs.opts.AllowRequest(); allowRequest != nil {
			if err := allowRequest(ctx); err != nil {
				return FORBIDDEN, map[string]any{"message": err.Error()}
			}
		}
	}

	return nil, nil
}

// Adds a new middleware.
func (bs *baseServer) Use(fn Middleware) {
	bs.middlewareMu.Lock()
	defer bs.middlewareMu.Unlock()
	bs.middlewares = append(bs.middlewares, fn)
}

// Apply the middlewares to the request.
func (bs *baseServer) ApplyMiddlewares(ctx *types.HttpContext, callback func(error)) {
	bs.middlewareMu.RLock()
	middlewares := make([]Middleware, len(bs.middlewares))
	copy(middlewares, bs.middlewares)
	bs.middlewareMu.RUnlock()

	if len(middlewares) == 0 {
		serverLog.Debug("no middleware to apply, skipping")
		callback(nil)
		return
	}
	var apply func(int)
	apply = func(i int) {
		serverLog.Debug("applying middleware n°%d", i+1)
		middlewares[i](ctx, func(err error) {
			if err != nil {
				callback(err)
				return
			}
			if i+1 < len(middlewares) {
				apply(i + 1)
			} else {
				callback(nil)
			}
		})
	}

	apply(0)
}

// Closes all clients.
func (bs *baseServer) Close() BaseServer {
	serverLog.Debug("closing all open clients")
	bs.clients.Range(func(_ string, client Socket) bool {
		client.Close(true)
		return true
	})

	bs._proto_.Cleanup()

	return bs
}

func (bs *baseServer) Cleanup() {
}

// generate a socket id.
// Overwrite this method to generate your custom socket id
func (bs *baseServer) GenerateId(*types.HttpContext) string {
	return utils.Base64Id().GenerateId()
}

// Handshakes a new client.
func (bs *baseServer) Handshake(transportName string, ctx *types.HttpContext) (*types.CodeMessage, transports.Transport) {
	id, err := bs.generateId(ctx)
	if err != nil {
		context := map[string]any{"name": "ID_GENERATION_ERROR", "error": err}
		bs.Emit("connection_error", &types.ErrorMessage{
			CodeMessage: BAD_REQUEST,
			Req:         ctx,
			Context:     context,
		})
		return BAD_REQUEST, nil
	}
	return bs.handshakeWithID(transportName, ctx, id)
}

func (bs *baseServer) generateId(ctx *types.HttpContext) (string, error) {
	if options, ok := bs.opts.(interface {
		GetRawGenerateId() types.Optional[config.IdGenerator]
		GenerateId() config.IdGenerator
	}); ok && options.GetRawGenerateId() != nil {
		return options.GenerateId()(ctx)
	}
	return bs._proto_.GenerateId(ctx), nil
}

func (bs *baseServer) handshakeWithID(transportName string, ctx *types.HttpContext, id string) (*types.CodeMessage, transports.Transport) {
	protocol, supported := bs.protocolVersion(ctx)
	if !supported {
		serverLog.Debug(`unsupported protocol version "%s"`, ctx.Query().Peek("EIO"))
		bs.Emit("connection_error", &types.ErrorMessage{
			CodeMessage: UNSUPPORTED_PROTOCOL_VERSION,
			Req:         ctx,
			Context:     unsupportedProtocolContext(ctx),
		})
		return UNSUPPORTED_PROTOCOL_VERSION, nil
	}

	serverLog.Debug(`handshaking client "%s" (%s)`, id, transportName)

	ctx.IdleTimeout = bs.opts.IdleTimeout()
	transport, err := bs._proto_.CreateTransport(transportName, ctx)
	if err != nil {
		serverLog.Debug(`handshaking client "%s" (%s)`, id, transportName)
		bs.Emit("connection_error", &types.ErrorMessage{
			CodeMessage: BAD_REQUEST,
			Req:         ctx,
			Context: map[string]any{
				"name":  "TRANSPORT_HANDSHAKE_ERROR",
				"error": err,
			},
		})
		return BAD_REQUEST, nil
	}

	if transports.POLLING == transportName {
		transport.SetMaxHttpBufferSize(bs.opts.MaxHttpBufferSize())
		transport.SetHttpCompression(bs.opts.HttpCompression())
	} else if transports.WEBSOCKET == transportName {
		transport.SetPerMessageDeflate(bs.opts.PerMessageDeflate())
	}

	_ = transport.On("headers", func(args ...any) {
		headers, req := slices.TryGetAny[*types.ParameterBag](args, 0), slices.TryGetAny[*types.HttpContext](args, 1)
		if req != nil && !req.Query().Has("sid") {
			addSessionCookie(headers, bs.opts.Cookie(), id)
			bs.Emit("initial_headers", headers, req)
		}
		bs.Emit("headers", headers, req)
	})

	transport.OnRequest(ctx)

	engineSocket := newSocketForHandshake(id, bs, transport, ctx, protocol)
	// Register the SID before queuing the OPEN packet. WebSocket writes run on
	// an asynchronous queue in Go, so the peer can otherwise observe its SID
	// before Clients() does, unlike the atomic ordering of the official event
	// loop implementation.
	bs.clients.Store(id, engineSocket)
	bs.clientsCount.Add(1)

	var registered atomic.Bool
	registered.Store(true)
	removeClient := func() {
		if registered.CompareAndSwap(true, false) {
			bs.clients.Delete(id)
			bs.clientsCount.Add(^uint64(0))
		}
	}
	_ = engineSocket.Once("close", func(...any) {
		removeClient()
	})

	if !engineSocket.(*socket).onOpen() {
		removeClient()
		engineSocket.(*socket).markConnectionReady()
		transport.Discard()
		transport.Close()
		return BAD_REQUEST, nil
	}
	// The transport constructor must not dispatch packets before Socket has
	// installed its listeners. This ordering is implicit in Node.js but must be
	// explicit with Go transport reader goroutines.
	startTransport(transport)
	concreteSocket := engineSocket.(*socket)
	if emitter, ok := bs._proto_.(interface{ emitConnection(Socket) }); ok {
		emitter.emitConnection(engineSocket)
	} else {
		concreteSocket.beginConnectionAnnouncement()
		if engineSocket.ReadyState() != "open" {
			concreteSocket.finishConnectionAnnouncement()
			concreteSocket.markConnectionReady()
			return BAD_REQUEST, nil
		}
		bs.Emit("connection", engineSocket)
		concreteSocket.finishConnectionAnnouncement()
	}
	concreteSocket.markConnectionReady()

	return nil, transport
}

func cloneCookie(cookie *http.Cookie) *http.Cookie {
	if cookie == nil {
		return nil
	}
	cloned := *cookie
	return &cloned
}

func addSessionCookie(headers *types.ParameterBag, cookie *http.Cookie, id string) {
	if headers == nil || cookie == nil {
		return
	}
	sessionCookie := cloneCookie(cookie)
	sessionCookie.Value = id
	headers.Add("Set-Cookie", sessionCookie.String())
}

// abstract
func (*baseServer) CreateTransport(string, *types.HttpContext) (transports.Transport, error) {
	return nil, errors.ErrTransportNotImplemented
}
