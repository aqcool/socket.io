package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/parsers/engine/v4/parser"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/events"
	"github.com/aqcool/socket.io/v4/pkg/queue"
	"github.com/aqcool/socket.io/v4/pkg/slices"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

// SocketWithoutUpgrade provides a WebSocket-like interface to connect to an Engine.IO server.
// This implementation maintains a single transport throughout the connection lifecycle without
// attempting to upgrade to a different transport type.
//
// Key features:
//   - Maintains a persistent connection using the initial transport
//   - Supports various transport types (HTTP long-polling, WebSocket, WebTransport)
//   - Handles packet buffering and flushing
//   - Manages connection state and heartbeat
//   - Provides event-based communication
//
// Example usage:
//
//	import (
//		"strings"
//
//		"github.com/aqcool/socket.io/clients/engine/v4"
//	)
//
//	func main() {
//		opts := engine.DefaultSocketOptions()
//		opts.SetTransportList([]engine.TransportCtor{&engine.PollingBuilder{}})
//		socket := engine.NewSocketWithoutUpgrade("http://localhost:8080", opts)
//		socket.On("open", func(...any) {
//			socket.Send(strings.NewReader("hello"), nil, nil)
//		})
//	}
//
// See: [SocketWithUpgrade]
// See: [Socket]
type socketWithoutUpgrade struct {
	types.EventEmitter

	// _proto_ is used for interface method rewriting and prototype pattern implementation
	_proto_ SocketWithoutUpgrade

	// Public fields
	id          types.Atomic[string]         // Unique session identifier
	transport   atomic.Pointer[Transport]    // Current transport instance
	readyState  types.Atomic[SocketState]    // Current connection state
	writeBuffer *types.Slice[*packet.Packet] // Buffer for outgoing packets
	// Number of packets handed to the current transport write. They stay in
	// writeBuffer until the transport emits drain, matching engine.io-client.
	prevBufferLen int

	// Protected fields (read-only after initialization)
	opts       *SocketOptions       // Connection options
	transports *types.Slice[string] // Available transport types
	upgrading  atomic.Bool          // Upgrade status flag

	// Private fields
	_pingInterval              atomic.Int64                // Interval between ping messages
	_pingTimeout               atomic.Int64                // Timeout for ping responses
	_maxPayload                atomic.Int64                // Maximum payload size allowed
	_pingTimeoutTimer          atomic.Pointer[utils.Timer] // Timer for ping timeout
	_pingTimeoutTime           types.Atomic[float64]       // Timestamp for ping timeout expiration
	_beforeunloadEventListener types.EventListener         // Event listener for page unload
	_offlineEventListener      types.EventListener         // Event listener for offline status

	// Connection configuration (read-only)
	secure            bool                       // Whether to use secure connection
	hostname          string                     // Server hostname
	port              string                     // Server port
	_transportsByName map[string][]TransportCtor // Ordered transport constructors grouped by name
	_cookieJar        http.CookieJar             // Cookie storage for HTTP requests

	// Static fields
	protocol int // Engine.IO protocol version

	flushMu   sync.Mutex
	taskQueue *queue.Queue
}

// Prototype sets the prototype for method rewriting.
func (s *socketWithoutUpgrade) Prototype(_proto_ SocketWithoutUpgrade) {
	s._proto_ = _proto_
}

// Proto returns the current prototype instance.
func (s *socketWithoutUpgrade) Proto() SocketWithoutUpgrade {
	return s._proto_
}

// Id returns the unique session identifier.
func (s *socketWithoutUpgrade) Id() string {
	return s.id.Load()
}

// Transport returns the current transport instance.
func (s *socketWithoutUpgrade) Transport() Transport {
	if transport := s.transport.Load(); transport != nil {
		return *transport
	}
	return nil
}

// ReadyState returns the current connection state.
func (s *socketWithoutUpgrade) ReadyState() SocketState {
	return s.readyState.Load()
}

// WriteBuffer returns the buffer for outgoing packets.
func (s *socketWithoutUpgrade) WriteBuffer() *types.Slice[*packet.Packet] {
	return s.writeBuffer
}

// Opts returns the socket options interface.
func (s *socketWithoutUpgrade) Opts() SocketOptionsInterface {
	return s.opts
}

// Transports returns the set of available transport types.
func (s *socketWithoutUpgrade) Transports() *types.Slice[string] {
	return s.transports
}

// SetUpgrading sets the upgrading status flag.
func (s *socketWithoutUpgrade) SetUpgrading(upgrading bool) {
	s.upgrading.Store(upgrading)
}

// Upgrading returns the current upgrading status.
func (s *socketWithoutUpgrade) Upgrading() bool {
	return s.upgrading.Load()
}

// CookieJar returns the HTTP cookie jar for the socket.
func (s *socketWithoutUpgrade) CookieJar() http.CookieJar {
	return s._cookieJar
}

// SetPriorWebsocketSuccess sets the flag indicating previous WebSocket connection success.
func (s *socketWithoutUpgrade) SetPriorWebsocketSuccess(priorWebsocketSuccess bool) {
	sharedPriorWebsocketSuccess.Store(priorWebsocketSuccess)
}

// PriorWebsocketSuccess returns whether previous WebSocket connection was successful.
func (s *socketWithoutUpgrade) PriorWebsocketSuccess() bool {
	return sharedPriorWebsocketSuccess.Load()
}

// Protocol returns the Engine.IO protocol version.
func (s *socketWithoutUpgrade) Protocol() int {
	return parser.Protocol
}

// MakeSocketWithoutUpgrade creates a new socketWithoutUpgrade instance with default settings.
// It initializes the basic socket structure without establishing a connection.
func MakeSocketWithoutUpgrade() SocketWithoutUpgrade {
	s := &socketWithoutUpgrade{
		EventEmitter: types.NewEventEmitter(),
		writeBuffer:  &types.Slice[*packet.Packet]{},

		taskQueue: queue.New(),
	}

	s._pingInterval.Store(-1)
	s._pingTimeout.Store(-1)
	s._maxPayload.Store(-1)
	s._pingTimeoutTime.Store(math.Inf(1))
	s.protocol = parser.Protocol
	s.Prototype(s)

	return s
}

// NewSocketWithoutUpgrade creates and initializes a new socket connection.
// It takes a URI string and optional socket options to configure the connection.
func NewSocketWithoutUpgrade(uri string, opts SocketOptionsInterface) SocketWithoutUpgrade {
	s := MakeSocketWithoutUpgrade()
	s.Construct(uri, opts)
	return s
}

// Construct initializes the socket with the given URI and options.
// It parses the URI, sets up transport options, and establishes the initial connection.
func (s *socketWithoutUpgrade) Construct(uri string, opts SocketOptionsInterface) {
	if opts == nil {
		opts = DefaultSocketOptions()
	}
	if uri != "" {
		if parsedURI, err := parseSocketURL(uri); err == nil {
			opts.SetHostname(parsedURI.Hostname())
			secure := parsedURI.Scheme == "https" || parsedURI.Scheme == "wss"
			opts.SetSecure(secure)
			if port := parsedURI.Port(); port != "" {
				opts.SetPort(port)
			} else if secure {
				opts.SetPort("443")
			} else {
				opts.SetPort("80")
			}
			if parsedURI.RawQuery != "" {
				opts.SetQuery(parsedURI.Query())
			}
		} else {
			clientSocketLog.Error("Invalid URL address: %v", err)
		}
	} else if opts.GetRawHost() != nil {
		if parsedURI, err := parseSocketURL(opts.Host()); err == nil {
			opts.SetHostname(parsedURI.Hostname())
		}
	}

	s.secure = opts.Secure()

	if opts.GetRawHostname() != nil && opts.GetRawPort() == nil {
		// if no port is specified manually, use the protocol default
		if s.secure {
			opts.SetPort("443")
		} else {
			opts.SetPort("80")
		}
	}

	if opts.GetRawHostname() != nil {
		s.hostname = opts.Hostname()
	} else {
		s.hostname = "localhost"
	}

	if opts.GetRawPort() != nil {
		s.port = opts.Port()
	} else {
		if s.secure {
			s.port = "443"
		} else {
			s.port = "80"
		}
	}

	s.transports = types.NewSlice[string]()
	s._transportsByName = map[string][]TransportCtor{}
	var transportConstructors []TransportCtor
	if ordered, ok := opts.(interface {
		GetRawTransportList() types.Optional[[]TransportCtor]
		TransportList() []TransportCtor
	}); ok && ordered.GetRawTransportList() != nil {
		transportConstructors = ordered.TransportList()
	} else if configured := opts.Transports(); configured != nil {
		transportConstructors = configured.Keys()
		// Legacy SetTransports values have no insertion order. Keep their
		// behavior deterministic and prefer the official built-in order.
		priority := map[string]int{
			transports.POLLING:      0,
			transports.WEBSOCKET:    1,
			transports.WEBTRANSPORT: 2,
		}
		sort.Slice(transportConstructors, func(i, j int) bool {
			left, leftKnown := priority[transportConstructors[i].Name()]
			right, rightKnown := priority[transportConstructors[j].Name()]
			if leftKnown && rightKnown {
				return left < right
			}
			if leftKnown != rightKnown {
				return leftKnown
			}
			return transportConstructors[i].Name() < transportConstructors[j].Name()
		})
	}
	for _, transport := range transportConstructors {
		if transport != nil {
			transportName := transport.Name()
			s.transports.Push(transportName)
			s._transportsByName[transportName] = append(s._transportsByName[transportName], transport)
		}
	}

	s.opts = DefaultSocketOptions()
	s.opts.SetPath("/engine.io")
	s.opts.SetAgent("")
	s.opts.SetWithCredentials(false)
	s.opts.SetUpgrade(true)
	s.opts.SetTimestampParam("t")
	s.opts.SetRememberUpgrade(false)
	s.opts.SetAddTrailingSlash(true)
	s.opts.SetIdleTimeout(120 * time.Second)
	s.opts.SetPerMessageDeflate(&types.PerMessageDeflate{
		Threshold: 1024,
	})
	s.opts.SetTransportOptions(map[string]SocketOptionsInterface{})
	s.opts.SetCloseOnBeforeunload(false)

	s.opts.Assign(opts)

	path := strings.TrimRight(s.opts.Path(), "/")
	if s.opts.AddTrailingSlash() {
		path += "/"
	}

	s.opts.SetPath(path)

	if s.opts.CloseOnBeforeunload() {
		s._beforeunloadEventListener = func(...any) {
			if transport := s.Transport(); transport != nil {
				transport.Clear()
				transport.Close()
			}
		}

		_ = events.Once(EventBeforeUnload, s._beforeunloadEventListener)

		if s.hostname != "localhost" {
			clientSocketLog.Debug("adding listener for the 'offline' event")
			s._offlineEventListener = func(...any) {
				s._onClose("transport close", errors.New("network connection lost"))
			}
			_ = events.Once(EventOffline, s._offlineEventListener)
		}
	}

	if s.opts.WithCredentials() {
		if jar, err := cookiejar.New(nil); err == nil {
			s._cookieJar = jar
		}
	}

	s._open()
}

var sharedPriorWebsocketSuccess atomic.Bool

type noTransportsAvailableError struct{}

func (noTransportsAvailableError) Error() string { return "No transports available" }

// parseSocketURL accepts the scheme-less host forms supported by the official
// client in addition to the URLs understood by net/url. In particular,
// url.Parse("localhost:3000") treats "localhost" as a scheme and url.Parse
// does not accept an unbracketed IPv6 literal as a URL host.
func parseSocketURL(raw string) (*url.URL, error) {
	if strings.Contains(raw, "://") {
		return url.Parse(raw)
	}

	if host := strings.Trim(raw, "[]"); net.ParseIP(host) != nil {
		return &url.URL{Host: "[" + host + "]"}, nil
	}

	return url.Parse("http://" + raw)
}

// CreateTransport initializes a new transport instance with the specified name.
// It sets up the necessary query parameters and configuration for the transport.
func (s *socketWithoutUpgrade) CreateTransport(name string) Transport {
	clientSocketLog.Debug(`creating transport "%s"`, name)

	query := url.Values{}

	for k, vs := range s.opts.Query() {
		for _, v := range vs {
			query.Add(k, v)
		}
	}

	// append engine.io protocol identifier
	query.Set("EIO", strconv.FormatInt(int64(s.protocol), 10))

	// transport name
	query.Set("transport", name)

	// session id if we already have one
	if id := s.Id(); id != "" {
		query.Set("sid", id)
	}

	opts := DefaultSocketOptions()
	opts.Assign(s.opts)
	opts.SetQuery(query)
	opts.SetHostname(s.hostname)
	opts.SetSecure(s.secure)
	opts.SetPort(s.port)
	if transportOptions := s.opts.TransportOptions(); transportOptions != nil {
		if topts, ok := transportOptions[name]; ok {
			opts.Assign(topts)
		}
	}

	clientSocketLog.Debug(`options "%v"`, opts)

	constructors := s._transportsByName[name]
	return constructors[0].New(s._proto_, opts)
}

// _open initializes the connection by selecting and creating the appropriate transport.
// It handles transport selection based on configuration and previous connection history.
func (s *socketWithoutUpgrade) _open() {
	if s.transports.Len() == 0 {
		// Emit error on next tick so it can be listened to
		time.AfterFunc(time.Millisecond, func() { s.Emit("error", noTransportsAvailableError{}) })
		return
	}
	transportName, err := s.transports.Get(0)
	if err != nil {
		time.AfterFunc(time.Millisecond, func() { s.Emit("error", err) })
		return
	}
	if s.opts.RememberUpgrade() && s.PriorWebsocketSuccess() && s.transports.FindIndex(func(s string) bool {
		return s == transports.WEBSOCKET
	}) != -1 {
		transportName = transports.WEBSOCKET
	}
	s.readyState.Store(SocketStateOpening)

	transport := s._proto_.CreateTransport(transportName)
	s._proto_.SetTransport(transport)

	transport.Open()
}

// SetTransport configures the current transport and sets up event listeners.
// It ensures proper cleanup of any existing transport before setting up the new one.
func (s *socketWithoutUpgrade) SetTransport(transport Transport) {
	clientSocketLog.Debug("setting transport %s", transport.Name())

	if oldTransport := s.Transport(); oldTransport != nil {
		clientSocketLog.Debug("clearing existing transport %s", oldTransport.Name())
		oldTransport.Clear()
	}

	// set up transport
	s.transport.Store(&transport)

	// set up transport listeners
	_ = transport.On("drain", func(...any) { s._onDrain() })
	_ = transport.On("packet", func(packets ...any) {
		s._onPacket(slices.TryGetAny[*packet.Packet](packets, 0))
	})
	_ = transport.On("error", func(err ...any) { s._onError(slices.TryGetAny[error](err, 0)) })
	_ = transport.On("close", func(reason ...any) { s._onClose("transport close", slices.TryGetAny[error](reason, 0)) })
}

// OnOpen is called when the connection is successfully established.
// It updates the connection state and triggers necessary initialization.
func (s *socketWithoutUpgrade) OnOpen() {
	clientSocketLog.Debug("socket open")
	s.readyState.Store(SocketStateOpen)
	s.SetPriorWebsocketSuccess(transports.WEBSOCKET == s.Transport().Name())
	s.Emit("open")
	s._proto_.Flush()
}

// _onPacket handles incoming packets from the transport.
// It processes different packet types and triggers appropriate events.
func (s *socketWithoutUpgrade) _onPacket(data *packet.Packet) {
	if readyState := s.ReadyState(); data != nil && (SocketStateOpening == readyState || SocketStateOpen == readyState || SocketStateClosing == readyState) {
		clientSocketLog.Debug(`socket receive: type "%s", data "%v"`, data.Type, data.Data)

		s.Emit("packet", data)

		// Socket is live - any packet counts
		s.Emit("heartbeat")

		switch data.Type {
		case packet.OPEN:
			if data.Data == nil {
				s._onError(errors.New("data must not be nil"))
				return
			}
			var handshake *HandshakeData
			if err := json.NewDecoder(data.Data).Decode(&handshake); err != nil {
				s._onError(err)
				return
			}
			if handshake == nil {
				s._onError(errors.New("decode error"))
				return
			}
			s._proto_.OnHandshake(handshake)
		case packet.PING:
			s._sendPacket(packet.PONG, nil, nil, nil)
			s.Emit("ping")
			s.Emit("pong")
			s._resetPingTimeout()
		case packet.ERROR:
			s._onError(fmt.Errorf("server error: %v", data.Data))
		case packet.MESSAGE:
			s.Emit("data", data.Data)
			s.Emit("message", data.Data)
		}
	} else {
		clientSocketLog.Debug(`packet received with socket readyState "%s"`, readyState)
	}
}

// OnHandshake processes the handshake data received from the server.
// It initializes connection parameters and starts the heartbeat mechanism.
func (s *socketWithoutUpgrade) OnHandshake(data *HandshakeData) {
	s.Emit("handshake", data)
	s.id.Store(data.Sid)
	s._pingInterval.Store(data.PingInterval)
	s._pingTimeout.Store(data.PingTimeout)
	s._maxPayload.Store(data.MaxPayload)
	s._proto_.OnOpen()
	// In case open handler closes socket
	if SocketStateClosed == s.ReadyState() {
		return
	}
	s._resetPingTimeout()
}

// _resetPingTimeout manages the ping timeout timer to ensure connection health.
// It handles timer cleanup and sets up new timeout periods based on server configuration.
func (s *socketWithoutUpgrade) _resetPingTimeout() {
	utils.ClearTimeout(s._pingTimeoutTimer.Load())
	delay := s._pingInterval.Load() + s._pingTimeout.Load()
	s._pingTimeoutTime.Store(float64(time.Now().UnixMilli() + delay))
	s._pingTimeoutTimer.Store(utils.SetTimeout(func() {
		s._onClose("ping timeout", nil)
	}, time.Duration(delay)*time.Millisecond))
	if s.opts.AutoUnref() {
		s._pingTimeoutTimer.Load().Unref()
	}
}

// _onDrain handles the drain event from the transport.
// It manages the write buffer and triggers appropriate events when the buffer is cleared.
func (s *socketWithoutUpgrade) _onDrain() {
	s.flushMu.Lock()
	if s.prevBufferLen > 0 {
		_, _ = s.writeBuffer.Splice(0, s.prevBufferLen)
		s.prevBufferLen = 0
	}
	empty := s.writeBuffer.Len() == 0
	s.flushMu.Unlock()

	if empty {
		s.Emit("drain")
	} else {
		s._proto_.Flush()
	}
}

// Flush sends buffered packets to the transport.
// It ensures packets are sent within payload size limits and handles transport state.
func (s *socketWithoutUpgrade) Flush() {
	s.flushMu.Lock()

	shouldEmitFlush := false
	// A transport marks itself writable immediately before emitting drain. In
	// Go, another goroutine can observe that flag in the tiny window before
	// _onDrain removes the batch that was just sent. Do not hand the same packet
	// objects to the transport twice: prevBufferLen is the authoritative
	// in-flight marker until the drain handler clears it.
	if SocketStateClosed != s.ReadyState() && s.Transport().Writable() && !s.Upgrading() && s.prevBufferLen == 0 {
		if packets := s._getWritablePackets(); len(packets) > 0 {
			clientSocketLog.Debug("flushing %d packets in socket", len(packets))
			s.prevBufferLen = len(packets)
			s.Transport().Send(packets)
			shouldEmitFlush = true
		}
	}

	s.flushMu.Unlock()

	if shouldEmitFlush {
		s.Emit("flush")
	}
}

// _getWritablePackets prepares packets for sending while respecting payload size limits.
// It handles packet encoding and size calculation for different transport types.
func (s *socketWithoutUpgrade) _getWritablePackets() (res []*packet.Packet) {
	packets := s.writeBuffer.All()
	maxPayload := s._maxPayload.Load()
	if maxPayload == 0 || s.Transport().Name() != transports.POLLING || len(packets) <= 1 {
		return packets
	}

	payloadSize := int64(1) // first packet type
	for i, outgoingPacket := range packets {
		if outgoingPacket.Data != nil {
			switch v := outgoingPacket.Data.(type) {
			case *types.StringBuffer:
				payloadSize += int64(v.Len())
			case *strings.Reader:
				payloadSize += int64(v.Len())
			case interface{ Len() int }:
				payloadSize += int64(math.Ceil(float64(v.Len()) * BASE64_OVERHEAD))
			default:
				snapshot, _ := types.NewBytesBufferReader(v)
				payloadSize += int64(math.Ceil(float64(snapshot.Len()) * BASE64_OVERHEAD))
				outgoingPacket.Data = snapshot
			}
			if i > 0 && payloadSize > maxPayload {
				clientSocketLog.Debug("only send %d out of %d packets", i, payloadSize)
				return packets[:i]
			}
			payloadSize += 2 // separator + packet type
		}
	}

	clientSocketLog.Debug("payload size is %d (max: %d)", payloadSize, maxPayload)
	return packets
}

// HasPingExpired checks if the connection has timed out due to missed heartbeats.
// It handles timer throttling and connection cleanup for timeout scenarios.
func (s *socketWithoutUpgrade) HasPingExpired() bool {
	if s._pingTimeoutTime.Load() == 0 {
		return true
	}
	hasExpired := float64(time.Now().UnixMilli()) > s._pingTimeoutTime.Load()
	if hasExpired {
		clientSocketLog.Debug("throttled timer detected, scheduling connection close")
		s._pingTimeoutTime.Store(0)

		s.taskQueue.Enqueue(func() { s._onClose("ping timeout", nil) })
	}

	return hasExpired
}

// Write sends a message through the socket.
// It buffers the message and triggers a flush operation.
func (s *socketWithoutUpgrade) Write(msg io.Reader, options *packet.Options, fn func()) SocketWithoutUpgrade {
	s._sendPacket(packet.MESSAGE, msg, options, fn)
	return s
}

// Send is an alias for Write, providing the same functionality.
func (s *socketWithoutUpgrade) Send(msg io.Reader, options *packet.Options, fn func()) SocketWithoutUpgrade {
	s._sendPacket(packet.MESSAGE, msg, options, fn)
	return s
}

// _sendPacket handles the internal packet sending logic.
// It manages packet buffering and callback handling.
func (s *socketWithoutUpgrade) _sendPacket(_type packet.Type, data io.Reader, options *packet.Options, fn func()) {
	if readyState := s.ReadyState(); SocketStateClosing == readyState || SocketStateClosed == readyState {
		return
	}
	packet := &packet.Packet{
		Type:    _type,
		Data:    data,
		Options: options,
	}
	s.Emit("packetCreate", packet)

	s.writeBuffer.Push(packet)

	if fn != nil {
		_ = s.Once("flush", func(...any) {
			fn()
		})
	}
	s._proto_.Flush()
}

// Close gracefully closes the socket connection.
// It handles cleanup of resources and ensures proper transport closure.
func (s *socketWithoutUpgrade) Close() SocketWithoutUpgrade {
	close := func() {
		s._onClose("forced close", nil)
		clientSocketLog.Debug("socket closing - telling transport to close")
		s.Transport().Close()
	}

	var cleanupAndClose types.EventListener
	cleanupAndClose = func(...any) {
		s.RemoveListener("upgrade", cleanupAndClose)
		s.RemoveListener("upgradeError", cleanupAndClose)
		close()
	}

	waitForUpgrade := func() {
		// wait for upgrade to finish since we can't send packets while pausing a transport
		_ = s.Once("upgrade", cleanupAndClose)
		_ = s.Once("upgradeError", cleanupAndClose)
	}

	if readyState := s.ReadyState(); SocketStateOpening == readyState || SocketStateOpen == readyState {
		s.readyState.Store(SocketStateClosing)
		if s.writeBuffer.Len() > 0 {
			_ = s.Once("drain", func(...any) {
				if s.Upgrading() {
					waitForUpgrade()
				} else {
					close()
				}
			})
		} else if s.Upgrading() {
			waitForUpgrade()
		} else {
			close()
		}
	}

	return s
}

// _onError handles transport errors and connection failures.
// It manages transport fallback and error reporting.
func (s *socketWithoutUpgrade) _onError(err error) {
	clientSocketLog.Debug("socket error %v", err)
	s.SetPriorWebsocketSuccess(false)

	if s.opts.TryAllTransports() && s.transports.Len() > 1 && s.ReadyState() == SocketStateOpening {
		clientSocketLog.Debug("trying next transport")
		failedName, _ := s.transports.Shift()
		if constructors := s._transportsByName[failedName]; len(constructors) > 0 {
			s._transportsByName[failedName] = constructors[1:]
		}
		s._open()
		return
	}

	s.Emit("error", err)
	s._onClose("transport error", err)
}

// _onClose handles the connection closure process.
// It performs cleanup operations and notifies listeners of the closure.
func (s *socketWithoutUpgrade) _onClose(reason string, description error) {
	if readyState := s.ReadyState(); SocketStateOpening == readyState || SocketStateOpen == readyState || SocketStateClosing == readyState {
		clientSocketLog.Debug(`socket close with reason: "%s"`, reason)

		// clear timers
		utils.ClearTimeout(s._pingTimeoutTimer.Load())

		if transport := s.Transport(); transport != nil {
			// stop event from firing again for transport
			transport.RemoveAllListeners("close")

			// ensure transport won't stay open
			transport.Close()

			// ignore further transport communication
			transport.Clear()
		}

		if s._beforeunloadEventListener != nil {
			events.RemoveListener(EventBeforeUnload, s._beforeunloadEventListener)
		}

		if s._offlineEventListener != nil {
			events.RemoveListener(EventOffline, s._offlineEventListener)
		}

		// set ready state
		s.readyState.Store(SocketStateClosed)

		// clear session id
		s.id.Store("")

		// emit close event
		s.Emit("close", reason, description)

		// clean buffers after, so users can still
		// grab the buffers on `close` event
		s.flushMu.Lock()
		s.writeBuffer.Clear()
		s.prevBufferLen = 0
		s.flushMu.Unlock()

		s.taskQueue.TryClose()
	}
}
