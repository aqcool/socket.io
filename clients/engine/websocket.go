package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/parsers/engine/v3/parser"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/queue"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	ws "github.com/gorilla/websocket"
)

// WebSocket implements the WebSocket transport for Engine.IO.
// It provides full-duplex communication over a single TCP connection using
// the WebSocket protocol. This transport supports both text and binary
// data transmission, with optional compression.
//
// Features:
//   - Full-duplex communication
//   - Binary data support
//   - Message compression (optional)
//   - Custom protocols support
type websocket struct {
	Transport

	// dialer is the WebSocket dialer used to establish connections.
	// It handles connection establishment with custom options like
	// proxy settings, TLS configuration, and protocol selection.
	dialer WebSocketDialer

	// socket is the active WebSocket connection instance.
	// It provides the actual communication channel with the server.
	socket *types.WebSocketConn

	// mu protects concurrent access to the WebSocket connection.
	// This ensures thread-safe operations on the connection.
	mu sync.Mutex

	writeQueue  *queue.Queue
	dialContext context.Context
	cancelDial  context.CancelFunc
	closing     atomic.Bool
}

// Name returns the identifier for the WebSocket transport.
// This identifier is used in transport selection and upgrade processes.
//
// Returns:
//   - string: The transport name ("websocket")
func (w *websocket) Name() string {
	return transports.WEBSOCKET
}

// MakeWebSocket creates a new WebSocket transport instance with default settings.
// This is the factory function for creating a new WebSocket transport.
//
// Returns:
//   - WebSocket: A new WebSocket transport instance initialized with default settings
func MakeWebSocket() WebSocket {
	s := &websocket{
		Transport: MakeTransport(),
	}

	s.Prototype(s)

	return s
}

// NewWebSocket creates a new WebSocket transport instance with the specified
// socket and options.
//
// Parameters:
//   - socket: The parent socket instance
//   - opts: The socket options configuration
//
// Returns:
//   - WebSocket: A new WebSocket transport instance configured with the specified options
func NewWebSocket(socket Socket, opts SocketOptionsInterface) WebSocket {
	s := MakeWebSocket()

	s.Construct(socket, opts)

	return s
}

// Construct initializes the WebSocket transport with the given socket and options.
// This sets up the WebSocket dialer with appropriate configuration for the connection.
//
// Parameters:
//   - socket: The parent socket instance
//   - opts: The socket options configuration
func (w *websocket) Construct(socket Socket, opts SocketOptionsInterface) {
	w.Transport.Construct(socket, opts)

	w.writeQueue = queue.New()
	w.dialContext, w.cancelDial = context.WithCancel(context.Background())

	dialer := &ws.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		TLSClientConfig:   w.Opts().TLSClientConfig(),
		Subprotocols:      w.Opts().Protocols(),
		EnableCompression: w.Opts().PerMessageDeflate() != nil,
		Jar:               w.Socket().CookieJar(),
	}
	if proxy := w.Opts().ProxyURL(); proxy != nil {
		dialer.Proxy = http.ProxyURL(proxy)
	}
	w.dialer = dialer.DialContext
	if custom := w.Opts().WebSocketDialer(); custom != nil {
		w.dialer = custom
	}
}

// DoOpen initiates the WebSocket connection.
// This method establishes the WebSocket connection and sets up event listeners.
// It handles the initial handshake and connection setup process.
func (w *websocket) DoOpen() {
	// Dialing must not block the constructor. Besides matching browser/Node
	// transports, this guarantees callers can attach open/error listeners after
	// NewSocket returns without racing a fast local server.
	go w.open()
}

func (w *websocket) open() {
	headers := http.Header{}
	for k, vs := range w.Opts().ExtraHeaders() {
		for _, v := range vs {
			headers.Add(k, v)
		}
	}
	uri := w.uri().String()
	startedAt := time.Now()
	socket, response, err := w.dialer(w.dialContext, uri, headers)
	notifyNetwork(w.Opts(), &NetworkEvent{
		Operation: "dial", Transport: w.Name(), URL: uri,
		Duration: time.Since(startedAt), Success: err == nil, Err: err,
	})
	if err != nil {
		if w.closing.Load() && errors.Is(err, context.Canceled) {
			return
		}
		var requestContext context.Context
		if response != nil && response.Request != nil {
			requestContext = response.Request.Context()
		}
		// Preserve the transport's asynchronous event contract even when a
		// custom dialer fails immediately in its goroutine. The short next-tick
		// delay gives NewSocket's caller a deterministic listener-registration
		// window, just like the official client.
		time.AfterFunc(time.Millisecond, func() {
			if !w.closing.Load() {
				w.OnError("websocket error", err, requestContext)
			}
		})
		return
	}
	connection := &types.WebSocketConn{EventEmitter: types.NewEventEmitter(), WebSocketConnection: socket}
	w.mu.Lock()
	w.socket = connection
	w.mu.Unlock()
	if w.closing.Load() {
		_ = connection.Close()
		return
	}

	w.addEventListeners()
}

func (w *websocket) _error(err error) {
	if ws.IsUnexpectedCloseError(err) || errors.Is(err, net.ErrClosed) {
		w.socket.Emit("close", err)
	} else {
		w.socket.Emit("error", err)
	}
}

// message handles the WebSocket message reading loop.
// This method processes incoming WebSocket messages and handles different message types.
// It runs in a separate goroutine to continuously read messages from the connection.
func (w *websocket) message() {
	defer func() {
		if !w.writeQueue.IsShuttingDown() {
			w.socket.Emit("close")
		}
	}()

	for {
		if w.Opts().IdleTimeout() > 0 {
			_ = w.socket.SetReadDeadline(time.Now().Add(w.Opts().IdleTimeout()))
		}
		mt, message, err := w.socket.NextReader()
		if err != nil {
			w._error(err)
			return
		}

		switch mt {
		case ws.BinaryMessage:
			read := types.NewBytesBuffer(nil)
			if _, err := read.ReadFrom(message); err != nil {
				w._error(err)
			} else {
				w.OnData(read)
			}
		case ws.TextMessage:
			read := types.NewStringBuffer(nil)
			if _, err := read.ReadFrom(message); err != nil {
				w._error(err)
			} else {
				w.OnData(read)
			}
		case ws.CloseMessage:
			w.socket.Emit("close")
			if c, ok := message.(io.Closer); ok {
				_ = c.Close()
			}
			return
		case ws.PingMessage:
		case ws.PongMessage:
		}
		if c, ok := message.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

// addEventListeners sets up event handlers for the WebSocket connection.
// This method configures error and close event handlers and starts the
// message reading loop.
func (w *websocket) addEventListeners() {
	_ = w.socket.On("error", func(errs ...any) {
		w.OnError("websocket error", slices.TryGetAny[error](errs, 0), nil)
	})
	_ = w.socket.Once("close", func(details ...any) {
		w.OnClose(NewTransportError("websocket connection closed", slices.TryGetAny[error](details, 0), nil).Err())
	})

	// This goroutine is invoked only once.
	go w.message()

	w.OnOpen()
}

// Write sends packets over the WebSocket connection.
// This method handles packet encoding and WebSocket message framing.
//
// Parameters:
//   - packets: Array of packets to be sent
func (w *websocket) Write(packets []*packet.Packet) {
	w.SetWritable(false)

	w.writeQueue.Enqueue(func() { w.write(packets) })
}

// write performs the actual packet writing operation.
// This method runs in a separate goroutine to handle asynchronous writes.
//
// Parameters:
//   - packets: Array of packets to be sent
func (w *websocket) write(packets []*packet.Packet) {
	// fake drain
	// defer to next tick to allow Socket to clear writeBuffer
	defer func() {
		w.SetWritable(true)
		w.Emit("drain")
	}()

	var writeErr error

	w.mu.Lock()
	// encodePacket efficient as it uses websocket framing
	// no need for encodePayload
	for _, packet := range packets {
		// always creates a new object since ws modifies it
		compress := true
		if packet.Options != nil {
			if packet.Options.Compress != nil && !*packet.Options.Compress {
				compress = false
			}

			if w.Opts().PerMessageDeflate() == nil && packet.Options.WsPreEncodedFrame != nil {
				mt := ws.BinaryMessage
				if _, ok := packet.Options.WsPreEncodedFrame.(*types.StringBuffer); ok {
					mt = ws.TextMessage
				}
				if err := w.socket.WriteMessage(mt, packet.Options.WsPreEncodedFrame.Bytes()); err != nil {
					clientWebsocketLog.Debug(`Send Error "%s"`, err.Error())
					writeErr = err
					break
				}
				continue
			}
		}

		data, err := parser.Parserv4().EncodePacket(packet, w.SupportsBinary())
		if err != nil {
			clientWebsocketLog.Debug(`Send Error "%s"`, err.Error())
			writeErr = err
			break
		}
		w.doWrite(data, compress, &writeErr)
		if writeErr != nil {
			break
		}
	}
	w.mu.Unlock()

	// Report errors outside of the lock to prevent potential deadlocks
	// from event handlers that may try to write.
	if writeErr != nil {
		w._error(writeErr)
	}
}

// doWrite performs the actual WebSocket write operation.
// This method handles message compression and WebSocket message framing.
// When called from write() under lock, errors are stored in writeErr instead of
// calling _error() directly, to prevent deadlocks from event handlers.
//
// Parameters:
//   - data: The data to be written
//   - compress: Whether to compress the message
//   - writeErr: Optional error pointer to store errors (when called under lock)
func (w *websocket) doWrite(data types.BufferInterface, compress bool, writeErr ...*error) {
	reportErr := func(err error) {
		if len(writeErr) > 0 && writeErr[0] != nil {
			*writeErr[0] = err
		} else {
			w._error(err)
		}
	}

	if perMessageDeflate := w.Opts().PerMessageDeflate(); perMessageDeflate != nil {
		if data.Len() < perMessageDeflate.Threshold {
			compress = false
		}
	}
	clientWebsocketLog.Debug(`writing %#v`, data)

	w.socket.EnableWriteCompression(compress)
	mt := ws.BinaryMessage
	if _, ok := data.(*types.StringBuffer); ok {
		mt = ws.TextMessage
	}
	write, err := w.socket.NextWriter(mt)
	if err != nil {
		reportErr(err)
		return
	}
	defer func() {
		if err := write.Close(); err != nil {
			reportErr(err)
			return
		}
	}()
	if _, err := io.Copy(write, data); err != nil {
		reportErr(err)
		return
	}
}

// DoClose gracefully closes the WebSocket connection.
// This method ensures proper cleanup of the WebSocket connection.
func (w *websocket) DoClose() {
	w.closing.Store(true)
	if w.cancelDial != nil {
		w.cancelDial()
	}
	w.writeQueue.TryClose()
	w.mu.Lock()
	connection := w.socket
	w.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
}

// uri generates the URI for the WebSocket connection.
// This method constructs the appropriate WebSocket URL with query parameters.
//
// Returns:
//   - *url.URL: The constructed WebSocket URL
func (w *websocket) uri() *url.URL {
	schema := "ws"
	if w.Opts().Secure() {
		schema = "wss"
	}

	query := url.Values{}
	for k, vs := range w.Query() {
		for _, v := range vs {
			query.Add(k, v)
		}
	}

	if w.Opts().TimestampRequests() {
		query.Set(w.Opts().TimestampParam(), randomString())
	}

	if !w.SupportsBinary() {
		query.Set("b64", "1")
	}

	return w.CreateUri(schema, query)
}
