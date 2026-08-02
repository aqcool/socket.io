// Package engine implements the Engine.IO socket, which manages client connections, transport upgrades, and protocol state.
package engine

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/servers/engine/v3/errors"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

var socketLog = log.NewLog("engine:socket")

const (
	// DefaultUpgradeCheckInterval is the interval between upgrade readiness checks during transport upgrade.
	DefaultUpgradeCheckInterval = 100 * time.Millisecond
)

type socketAnnouncement struct {
	done chan struct{}
	once sync.Once
}

func newSocketAnnouncement() *socketAnnouncement {
	return &socketAnnouncement{done: make(chan struct{})}
}

func (a *socketAnnouncement) finish() {
	if a != nil {
		a.once.Do(func() { close(a.done) })
	}
}

type socket struct {
	types.EventEmitter

	// The revision of the protocol:
	//
	// - 3rd is used in Engine.IO v3 / Socket.IO v2
	// - 4th is used in Engine.IO v4 and above / Socket.IO v3 and above
	//
	// It is found in the `EIO` query parameters of the HTTP requests.
	//
	// See: https://github.com/socketio/engine.io-protocol
	protocol int
	// A reference to the first HTTP request of the session
	//
	// TODO for the next major release: remove it
	request *types.HttpContext
	// The IP address of the client.
	remoteAddress string

	// The current state of the socket.
	readyState types.Atomic[string]
	// The current low-level transport.
	transport atomic.Pointer[transports.Transport]

	// This is the session identifier that the client will use in the subsequent HTTP requests. It must not be shared with
	// others parties, as it might lead to session hijacking.
	id                string
	server            BaseServer
	upgrading         atomic.Bool
	upgraded          atomic.Bool
	upgradeStateMu    sync.Mutex
	upgradeGeneration uint64
	activeUpgrade     uint64
	writeBuffer       *types.Slice[*packet.Packet]
	packetsFn         *types.Slice[SendCallback]
	sentCallbackFn    *types.Slice[[]SendCallback]
	cleanupFn         *types.Slice[types.Callable]
	pingTimeoutTimer  atomic.Pointer[utils.Timer]
	pingIntervalTimer atomic.Pointer[utils.Timer]

	flushMu                sync.Mutex
	drainCallbackMu        sync.Mutex
	requestMu              sync.Mutex
	packetMu               sync.Mutex
	connectionReady        chan struct{}
	connectionReadyOnce    sync.Once
	openEventStarted       atomic.Bool
	openEventDone          chan struct{}
	openEventDoneOnce      sync.Once
	connectionAnnouncing   atomic.Bool
	connectionAnnounced    chan struct{}
	connectionAnnounceOnce sync.Once
	upgradeAnnouncement    atomic.Pointer[socketAnnouncement]
}

func (s *socket) onRequest(ctx *types.HttpContext) {
	if ctx.Method() == http.MethodGet {
		var waitFinished *forwardingTransport
		for {
			s.requestMu.Lock()
			transport := s.Transport()
			forwarder, forwarding := transport.(*forwardingTransport)
			if !forwarding || forwarder == waitFinished || !forwarder.oneShotReleasePending() {
				if transport != nil {
					transport.OnRequest(ctx)
				}
				s.requestMu.Unlock()
				return
			}
			s.requestMu.Unlock()

			// Socket.server is the base Engine.IO prototype, while the forwarding
			// transport retains the concrete cluster server which owns this wait.
			responseTimeout := forwarder.server.options.ResponseTimeout
			serverClosed := forwarder.server.closed
			timer := time.NewTimer(responseTimeout)
			select {
			case <-forwarder.done:
				timer.Stop()
				// A normal one-shot has already restored the physical transport.
				// If a terminal path intentionally kept this inactive slot, bypass
				// the wait once and let forwardingTransport return its explicit 400.
				waitFinished = forwarder
				continue
			case <-ctx.Context().Done():
				timer.Stop()
				return
			case <-serverClosed:
				timer.Stop()
				// This GET has not yet been attached to the physical polling
				// transport, so shutdown has no transport callback that could finish
				// it. Complete it explicitly instead of leaving ServeHTTP waiting.
				_ = ctx.SetStatusCode(http.StatusBadRequest)
				_, _ = ctx.Write(nil)
				return
			case <-timer.C:
				// A failed publication may deliberately leave the forwarder installed
				// for shutdown cleanup. Re-enter once without waiting so that stale
				// transport returns the normal overlapping-request 400 response.
				waitFinished = forwarder
			}
		}
	}
	transport := s.Transport()
	if transport != nil {
		transport.OnRequest(ctx)
	}
}

func (s *socket) Protocol() int {
	return s.protocol
}

func (s *socket) Upgraded() bool {
	return s.upgraded.Load()
}

func (s *socket) Upgrading() bool {
	return s.upgrading.Load()
}

func (s *socket) beginUpgrade() (uint64, bool) {
	s.upgradeStateMu.Lock()
	defer s.upgradeStateMu.Unlock()
	if s.ReadyState() != "open" || s.upgraded.Load() || s.activeUpgrade != 0 {
		return 0, false
	}
	s.upgradeGeneration++
	if s.upgradeGeneration == 0 {
		s.upgradeGeneration++
	}
	s.activeUpgrade = s.upgradeGeneration
	s.upgrading.Store(true)
	return s.activeUpgrade, true
}

func (s *socket) finishUpgrade(token uint64, success bool) bool {
	s.upgradeStateMu.Lock()
	defer s.upgradeStateMu.Unlock()
	if token == 0 || s.activeUpgrade != token {
		return false
	}
	if success && s.ReadyState() != "open" {
		s.activeUpgrade = 0
		s.upgrading.Store(false)
		return false
	}
	if success {
		s.upgraded.Store(true)
	}
	s.activeUpgrade = 0
	s.upgrading.Store(false)
	return true
}

func (s *socket) hasActiveUpgrade(token uint64) bool {
	s.upgradeStateMu.Lock()
	active := token != 0 && s.activeUpgrade == token
	s.upgradeStateMu.Unlock()
	return active
}

func (s *socket) Id() string {
	return s.id
}

func (s *socket) RemoteAddress() string {
	return s.remoteAddress
}

func (s *socket) Request() *types.HttpContext {
	return s.request
}

func (s *socket) Transport() transports.Transport {
	if v := s.transport.Load(); v != nil {
		return *v
	}
	return nil
}

func (s *socket) Server() BaseServer {
	return s.server
}

func (s *socket) ReadyState() string {
	return s.readyState.Load()
}

func (s *socket) SetReadyState(state string) {
	socketLog.Debug("readyState updated from %s to %s", s.ReadyState(), state)

	s.readyState.Store(state)
}

// Client class.
func MakeSocket() Socket {
	s := makeSocket()
	s.markConnectionReady()
	return s
}

func makeSocket() *socket {
	s := &socket{
		EventEmitter: types.NewEventEmitter(),

		writeBuffer:         types.NewSlice[*packet.Packet](),
		packetsFn:           types.NewSlice[SendCallback](),
		sentCallbackFn:      types.NewSlice[[]SendCallback](),
		cleanupFn:           types.NewSlice[types.Callable](),
		connectionReady:     make(chan struct{}),
		openEventDone:       make(chan struct{}),
		connectionAnnounced: make(chan struct{}),
	}
	s.readyState.Store("opening")

	return s
}

// Client class.
func NewSocket(id string, server BaseServer, transport transports.Transport, ctx *types.HttpContext, protocol int) Socket {
	s := MakeSocket()

	s.Construct(id, server, transport, ctx, protocol)

	return s
}

// newSocketForHandshake prevents packets sent immediately after the Engine.IO
// OPEN packet from being dispatched until BaseServer has notified connection
// listeners. This recreates the ordering guaranteed by the official Node.js
// implementation's single-threaded event loop.
func newSocketForHandshake(id string, server BaseServer, transport transports.Transport, ctx *types.HttpContext, protocol int) Socket {
	s := makeSocket()
	s.initialize(id, server, transport, ctx, protocol)
	return s
}

func (s *socket) markConnectionReady() {
	s.connectionReadyOnce.Do(func() {
		close(s.connectionReady)
	})
}

func (s *socket) waitConnectionReady() {
	<-s.connectionReady
}

func (s *socket) beginConnectionAnnouncement() {
	s.connectionAnnouncing.Store(true)
}

func (s *socket) finishConnectionAnnouncement() {
	s.connectionAnnounceOnce.Do(func() { close(s.connectionAnnounced) })
}

func (s *socket) beginUpgradeAnnouncement() *socketAnnouncement {
	announcement := newSocketAnnouncement()
	s.upgradeAnnouncement.Store(announcement)
	return announcement
}

func (s *socket) finishUpgradeAnnouncement(announcement *socketAnnouncement) {
	announcement.finish()
	s.upgradeAnnouncement.CompareAndSwap(announcement, nil)
}

func (s *socket) Construct(id string, server BaseServer, transport transports.Transport, ctx *types.HttpContext, protocol int) {
	s.initialize(id, server, transport, ctx, protocol)
	_ = s.onOpen()
	s.markConnectionReady()
}

func (s *socket) initialize(id string, server BaseServer, transport transports.Transport, ctx *types.HttpContext, protocol int) {
	s.id = id
	s.server = server
	s.request = ctx
	s.protocol = protocol

	// Cache IP since it might not be in the req later
	if ctx.WebTransport != nil {
		s.remoteAddress = remoteHost(ctx.WebTransport.RemoteAddr().String())
	} else if ctx.Websocket != nil && ctx.Websocket.WebSocketConnection != nil {
		s.remoteAddress = remoteHost(ctx.Websocket.RemoteAddr().String())
	} else {
		s.remoteAddress = remoteHost(ctx.Request().RemoteAddr)
	}

	s.setTransport(transport)
}

func remoteHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	// Preserve non-host:port values supplied by custom HTTP integrations while
	// normalizing bracketed IPv6 literals.
	return strings.Trim(address, "[]")
}

// Called upon transport considered open.
func (s *socket) onOpen() bool {
	if !s.readyState.CompareAndSwap("opening", "open") {
		return false
	}

	// sends an `open` packet
	s.Transport().SetSid(s.id)

	data, err := json.Marshal(map[string]any{
		"sid":          s.id,
		"upgrades":     s.getAvailableUpgrades(),
		"pingInterval": int64(s.server.Opts().PingInterval() / time.Millisecond),
		"pingTimeout":  int64(s.server.Opts().PingTimeout() / time.Millisecond),
		"maxPayload":   s.server.Opts().MaxHttpBufferSize(),
	})

	if err != nil {
		socketLog.Debug("json.Marshal err: %s", err)
		s.OnClose("encode error")
		return false
	}
	s.queuePacket(
		packet.OPEN,
		types.NewStringBuffer(data),
		nil, nil,
	)

	if i := s.server.Opts().InitialPacket(); i != nil {
		if template, ok := i.(types.BufferInterface); ok {
			i = template.Clone()
		}
		s.queuePacket(packet.MESSAGE, i, nil, nil)
	}
	s.flush()

	if s.protocol == 3 {
		// in protocol v3, the client sends a ping, and the server answers with a pong
		s.resetPingTimeout()
	} else {
		// in protocol v4, the server sends a ping, and the client answers with a pong
		s.schedulePing()
	}
	if s.ReadyState() != "open" {
		utils.ClearTimeout(s.pingIntervalTimer.Load())
		utils.ClearTimeout(s.pingTimeoutTimer.Load())
		return false
	}
	s.openEventStarted.Store(true)
	if s.ReadyState() != "open" {
		s.openEventDoneOnce.Do(func() { close(s.openEventDone) })
		return false
	}
	s.Emit("open")
	s.openEventDoneOnce.Do(func() { close(s.openEventDone) })
	return s.ReadyState() == "open"
}

// Called upon transport packet.
func (s *socket) onPacket(data *packet.Packet) {
	s.waitConnectionReady()
	s.packetMu.Lock()
	defer s.packetMu.Unlock()
	s.onPacketLocked(data)
}

// onPacketLocked processes a packet while packetMu is held. Cluster handoff
// paths use it to replay their pre-connection buffer before newly arriving
// packets are allowed through.
func (s *socket) onPacketLocked(data *packet.Packet) {
	if data == nil {
		socketLog.Debug("packet received nil")
		return
	}

	if s.ReadyState() != "open" {
		socketLog.Debug("packet received with closed socket")
		return
	}

	// export packet event
	socketLog.Debug(`received packet %s`, data.Type)
	s.Emit("packet", clonePacketForEvent(data))

	switch data.Type {
	case packet.PING:
		if s.Transport().Protocol() != 3 {
			s.onError(errors.ErrInvalidHeartbeat)
			return
		}
		socketLog.Debug("got ping")
		if timer := s.pingTimeoutTimer.Load(); timer != nil {
			timer.Refresh()
		}
		s.sendPacket(packet.PONG, nil, nil, nil)
		s.Emit("heartbeat")
	case packet.PONG:
		if s.Transport().Protocol() == 3 {
			s.onError(errors.ErrInvalidHeartbeat)
			return
		}
		socketLog.Debug("got pong")
		utils.ClearTimeout(s.pingTimeoutTimer.Load())
		if timer := s.pingIntervalTimer.Load(); timer != nil {
			timer.Refresh()
		}
		s.Emit("heartbeat")
	case packet.ERROR:
		s.OnClose("parse error")
	case packet.MESSAGE:
		s.Emit("data", data.Data)
		s.Emit("message", data.Data)
	}
}

// Called upon transport error.
func (s *socket) onError(err error) {
	socketLog.Debug("transport error %v", err)
	s.OnClose("transport error", err)
}

// Pings client every `this.pingInterval` and expects response
// within `this.pingTimeout` or closes connection.
func (s *socket) schedulePing() {
	timer := utils.SetTimeout(func() {
		if s.ReadyState() != "open" {
			return
		}
		socketLog.Debug("writing ping packet - expecting pong within %dms", int64(s.server.Opts().PingTimeout()/time.Millisecond))
		s.sendPacket(packet.PING, nil, nil, nil)
		if s.ReadyState() != "open" {
			return
		}
		s.resetPingTimeout()
	}, s.server.Opts().PingInterval())
	s.pingIntervalTimer.Store(timer)
	if s.ReadyState() != "open" {
		utils.ClearTimeout(timer)
	}
}

// Resets ping timeout.
func (s *socket) resetPingTimeout() {
	if s.ReadyState() != "open" {
		return
	}
	utils.ClearTimeout(s.pingTimeoutTimer.Load())
	timer := utils.SetTimeout(func() {
		if s.ReadyState() == "closed" {
			return
		}
		s.OnClose("ping timeout")
	}, s.resetPingTimeoutDuration())
	s.pingTimeoutTimer.Store(timer)
	if s.ReadyState() != "open" {
		utils.ClearTimeout(timer)
	}
}
func (s *socket) resetPingTimeoutDuration() time.Duration {
	if s.protocol == 3 {
		return s.server.Opts().PingInterval() + s.server.Opts().PingTimeout()
	}
	return s.server.Opts().PingTimeout()
}

// Attaches handlers for the given transport.
func (s *socket) setTransport(transport transports.Transport) {
	onError := func(err ...any) {
		s.onError(slices.TryGetAny[error](err, 0))
	}
	onReady := func(...any) { s.flush() }
	onPacket := func(packets ...any) {
		s.onPacket(slices.TryGetAny[*packet.Packet](packets, 0))
	}
	onDrain := func(...any) { s.onDrain(transport) }
	onClose := func(...any) { s.OnClose("transport close") }

	s.transport.Store(&transport)

	_ = transport.Once("error", onError)
	_ = transport.On("ready", onReady)
	_ = transport.On("packet", onPacket)
	_ = transport.On("drain", onDrain)
	_ = transport.Once("close", onClose)

	s.cleanupFn.Push(func() {
		transport.RemoveListener("error", onError)
		transport.RemoveListener("ready", onReady)
		transport.RemoveListener("packet", onPacket)
		transport.RemoveListener("drain", onDrain)
		transport.RemoveListener("close", onClose)
	})
}

// Upon transport "drain" event
func (s *socket) onDrain(source transports.Transport) {
	// Treat a transport drain and all callbacks for its batch as one event-loop
	// turn with respect to a concurrent local transport upgrade.
	s.drainCallbackMu.Lock()
	defer s.drainCallbackMu.Unlock()
	s.flushMu.Lock()
	seqFn, err := s.sentCallbackFn.Shift()
	s.flushMu.Unlock()
	if err == nil {
		socketLog.Debug("executing batch send callback")
		for _, fn := range seqFn {
			fn(source)
		}
	}
}

// Upgrades socket to the given transport
func (s *socket) MaybeUpgrade(transport transports.Transport) {
	socketLog.Debug(`might upgrade socket transport from "%s" to "%s"`, s.Transport().Name(), transport.Name())

	upgradeToken, acquired := s.beginUpgrade()
	if !acquired {
		transport.Close()
		return
	}

	var check, cleanup func()
	var onPacket, onError, onTransportClose, onClose types.EventListener
	var upgradeTimeoutTimer, checkIntervalTimer atomic.Pointer[utils.Timer]
	var setupMu sync.Mutex
	var transitionMu sync.Mutex
	var cleanupOnce sync.Once
	var upgradeCommitted atomic.Bool
	var candidateFailed atomic.Bool
	var upgradeWaitingDrain atomic.Bool

	onPacket = func(datas ...any) {
		data, ok := datas[0].(*packet.Packet)
		if !ok {
			return
		}
		probe := ""
		if data.Type == packet.PING && data.Data != nil {
			sb := new(strings.Builder)
			_, _ = io.Copy(sb, data.Data)
			probe = sb.String()
		}
		if data.Type == packet.PING && probe == "probe" {
			socketLog.Debug("got probe ping packet, sending pong")
			transport.Send([]*packet.Packet{{Type: packet.PONG, Data: strings.NewReader("probe")}})
			s.Emit("upgrading", transport)

			utils.ClearInterval(checkIntervalTimer.Load())
			checkIntervalTimer.Store(utils.SetInterval(check, DefaultUpgradeCheckInterval))

		} else if packet.UPGRADE == data.Type && s.ReadyState() == "open" {
			transitionMu.Lock()
			defer transitionMu.Unlock()
			// A clustered polling handshake may still be waiting for its delayed
			// connection announcement. doConnect holds packetMu across that
			// announcement and its synthetic upgrade event. Sharing the same gate
			// here recreates the official implementation's event-loop ordering:
			// either the real upgrade completes first and doConnect replays it, or
			// connection listeners are installed before this event is emitted.
			s.packetMu.Lock()
			defer s.packetMu.Unlock()
			socketLog.Debug("got upgrade packet - upgrading")
			if !s.hasActiveUpgrade(upgradeToken) || s.ReadyState() != "open" ||
				candidateFailed.Load() || transport.ReadyState() != "open" {
				s.finishUpgrade(upgradeToken, false)
				cleanup()
				transport.Close()
				return
			}
			// Socket.flush holds flushMu from the transport writable check through
			// Send. If the previous transport still owns an asynchronous batch,
			// finish that batch before swapping transports. This is the Go
			// equivalent of the polling pause/drain barrier in the official
			// implementation and keeps callbacks attached to the transport which
			// actually wrote their packets.
			s.drainCallbackMu.Lock()
			s.flushMu.Lock()
			if s.sentCallbackFn.Len() > 0 {
				if upgradeWaitingDrain.CompareAndSwap(false, true) {
					queued := false
					s.sentCallbackFn.DoWrite(func(batches [][]SendCallback) [][]SendCallback {
						if len(batches) > 0 {
							queued = true
							batches[0] = append(batches[0], func(transports.Transport) {
								upgradeWaitingDrain.Store(false)
								// This callback runs inside onDrain's callback epoch.
								// Resume on another goroutine so the upgrade cannot
								// overtake callbacks later in the same old batch.
								go onPacket(&packet.Packet{Type: packet.UPGRADE})
							})
						}
						return batches
					})
					if !queued {
						upgradeWaitingDrain.Store(false)
					}
				}
				if upgradeWaitingDrain.Load() {
					s.flushMu.Unlock()
					s.drainCallbackMu.Unlock()
					return
				}
			}
			flushLocked := true
			unlockFlush := func() {
				if flushLocked {
					flushLocked = false
					s.flushMu.Unlock()
					s.drainCallbackMu.Unlock()
				}
			}
			defer unlockFlush()
			upgradeAnnouncement := s.beginUpgradeAnnouncement()
			previous := s.Transport()
			var previousFailed atomic.Bool
			previousError := types.EventListener(func(values ...any) {
				previousFailed.Store(true)
				s.OnClose("transport error", slices.TryGetAny[error](values, 0))
			})
			previousClose := types.EventListener(func(...any) {
				previousFailed.Store(true)
				s.OnClose("transport close")
			})
			_ = previous.Once("error", previousError)
			_ = previous.Once("close", previousClose)
			removePreviousGuard := func() {
				previous.RemoveListener("error", previousError)
				previous.RemoveListener("close", previousClose)
			}
			// Attach the new transport's permanent listeners before removing its
			// probe listeners. A close/error is continuously owned across handoff.
			s.cleanupTransportListeners()
			s.setTransport(transport)
			rollback := func() {
				s.cleanupTransportListeners()
				if s.ReadyState() == "open" && !previousFailed.Load() && previous.ReadyState() == "open" {
					s.setTransport(previous)
					removePreviousGuard()
				} else {
					removePreviousGuard()
					if s.ReadyState() == "open" {
						s.OnClose("transport close")
					}
					previous.Discard()
					s.closeDetachedTransport(previous)
				}
				s.finishUpgrade(upgradeToken, false)
				cleanup()
				unlockFlush()
				transport.Close()
				s.finishUpgradeAnnouncement(upgradeAnnouncement)
			}
			if candidateFailed.Load() || transport.ReadyState() != "open" ||
				previousFailed.Load() || previous.ReadyState() != "open" || s.ReadyState() != "open" {
				rollback()
				return
			}
			if !s.finishUpgrade(upgradeToken, true) {
				rollback()
				return
			}
			upgradeCommitted.Store(true)
			removePreviousGuard()
			unlockFlush()
			previous.Discard()
			cleanup()
			s.closeDetachedTransport(previous)
			s.Emit("upgrade", transport)
			s.finishUpgradeAnnouncement(upgradeAnnouncement)
			s.flush()
			if s.ReadyState() == "closing" {
				transport.Close(func() {
					s.OnClose("forced close")
				})
			}
		} else {
			transitionMu.Lock()
			defer transitionMu.Unlock()
			if !s.finishUpgrade(upgradeToken, false) && upgradeCommitted.Load() {
				return
			}
			cleanup()
			transport.Close()
		}
	}

	// we force a polling cycle to ensure a fast upgrade
	check = func() {
		if transports.POLLING == s.Transport().Name() && s.Transport().Writable() {
			socketLog.Debug("writing a noop packet to polling for fast upgrade")
			s.Transport().Send([]*packet.Packet{{Type: packet.NOOP}})
		}
	}

	cleanup = func() {
		setupMu.Lock()
		cleanupOnce.Do(func() {
			utils.ClearInterval(checkIntervalTimer.Load())
			utils.ClearTimeout(upgradeTimeoutTimer.Load())

			if transport != nil {
				transport.RemoveListener("packet", onPacket)
				transport.RemoveListener("close", onTransportClose)
				transport.RemoveListener("error", onError)
			}
			s.RemoveListener("close", onClose)
		})
		setupMu.Unlock()
	}

	onError = func(errs ...any) {
		candidateFailed.Store(true)
		transitionMu.Lock()
		defer transitionMu.Unlock()
		err := slices.TryGetAny[error](errs, 0)
		socketLog.Debug("client did not complete upgrade - %v", err)
		if !s.finishUpgrade(upgradeToken, false) && upgradeCommitted.Load() {
			s.OnClose("transport error", err)
			return
		}
		cleanup()
		if transport != nil {
			transport.Close()
		}
	}

	onTransportClose = func(...any) {
		candidateFailed.Store(true)
		onError("transport closed")
	}

	onClose = func(...any) {
		onError("socket closed")
	}

	// Listener installation and timer arming are one setup transaction. The
	// timeout or a concurrent socket close may fire immediately, but its cleanup
	// cannot run until every listener it must detach has been installed.
	setupMu.Lock()
	_ = transport.On("packet", onPacket)
	_ = transport.Once("close", onTransportClose)
	_ = transport.Once("error", onError)
	_ = s.Once("close", onClose)
	upgradeTimeoutTimer.Store(utils.SetTimeout(func() {
		transitionMu.Lock()
		defer transitionMu.Unlock()
		socketLog.Debug("client did not complete upgrade - closing transport")
		if !s.finishUpgrade(upgradeToken, false) && upgradeCommitted.Load() {
			return
		}
		cleanup()
		if transport != nil && transport.ReadyState() == "open" {
			transport.Close()
		}
	}, s.server.Opts().UpgradeTimeout()))
	invalidState := s.ReadyState() != "open" || transport.ReadyState() == "closed"
	setupMu.Unlock()
	if invalidState {
		onError("socket or transport closed during upgrade setup")
	}
}

func (s *socket) cleanupTransportListeners() {
	s.cleanupFn.DoWrite(func(cleanups []types.Callable) []types.Callable {
		for _, cleanup := range cleanups {
			cleanup()
		}
		return cleanups[:0]
	})
}

func (s *socket) closeDetachedTransport(transport transports.Transport) {
	if transport == nil {
		return
	}
	_ = transport.On("error", func(...any) {
		socketLog.Debug("error triggered by discarded transport")
	})
	transport.Close()
	utils.ClearTimeout(s.pingTimeoutTimer.Load())
}

// Clears listeners and timers associated with current transport.
func (s *socket) clearTransport() {
	s.cleanupTransportListeners()

	// silence further transport errors and prevent uncaught exceptions
	s.closeDetachedTransport(s.Transport())
}

// Called upon transport considered closed.
// Possible reasons: `ping timeout`, `client error`, `parse error`,
// `transport error`, `server close`, `transport close`
func (s *socket) OnClose(reason string, description ...error) {
	s.upgradeStateMu.Lock()
	if previous := s.readyState.Swap("closed"); previous == "closed" {
		s.upgradeStateMu.Unlock()
		return
	}
	s.activeUpgrade = 0
	s.upgrading.Store(false)
	s.upgradeStateMu.Unlock()
	description = append(description, nil)

	// clear timers
	utils.ClearTimeout(s.pingIntervalTimer.Load())

	utils.ClearTimeout(s.pingTimeoutTimer.Load())

	s.packetsFn.Clear()

	s.sentCallbackFn.Clear()

	s.clearTransport()
	emitClose := func() {
		s.Emit("close", reason, description[0])
		// Developers can still inspect writeBuffer from the close listener.
		s.writeBuffer.Clear()
	}
	waitForOpen := s.openEventStarted.Load()
	waitForConnection := s.connectionAnnouncing.Load()
	upgradeAnnouncement := s.upgradeAnnouncement.Load()
	ready := true
	if waitForOpen {
		select {
		case <-s.openEventDone:
		default:
			ready = false
		}
	}
	if waitForConnection {
		select {
		case <-s.connectionAnnounced:
		default:
			ready = false
		}
	}
	if upgradeAnnouncement != nil {
		select {
		case <-upgradeAnnouncement.done:
		default:
			ready = false
		}
	}
	if !ready {
		go func() {
			if waitForOpen {
				<-s.openEventDone
			}
			if waitForConnection {
				<-s.connectionAnnounced
			}
			if upgradeAnnouncement != nil {
				<-upgradeAnnouncement.done
			}
			emitClose()
		}()
		return
	}
	emitClose()
}

// Sends a message packet.
func (s *socket) Send(
	data io.Reader,
	options *packet.Options,
	callback SendCallback,
) Socket {
	s.sendPacket(packet.MESSAGE, data, options, callback)
	return s
}

// Alias of [Send]
func (s *socket) Write(
	data io.Reader,
	options *packet.Options,
	callback SendCallback,
) Socket {
	s.sendPacket(packet.MESSAGE, data, options, callback)
	return s
}

// Sends a packet.
func (s *socket) sendPacket(
	packetType packet.Type,
	data io.Reader,
	options *packet.Options,
	callback SendCallback,
) {
	s.queuePacket(packetType, data, options, callback)
	s.flush()
}

// queuePacket appends a packet without flushing it. The handshake uses this
// to put OPEN and InitialPacket in the same Polling payload, matching the
// official Engine.IO server contract.
func (s *socket) queuePacket(
	packetType packet.Type,
	data io.Reader,
	options *packet.Options,
	callback SendCallback,
) {
	if readystate := s.ReadyState(); readystate != "closing" && readystate != "closed" {
		socketLog.Debug(`sending packet "%s" (%p)`, packetType, data)

		// Clone options to avoid data races when the same Options pointer
		// is shared across multiple sockets during broadcast.
		opts := &packet.Options{}
		if options != nil {
			opts.WsPreEncodedFrame = options.WsPreEncodedFrame
			if options.Compress != nil {
				compress := *options.Compress
				opts.Compress = &compress
			}
		}

		if opts.Compress == nil || *opts.Compress {
			opts.Compress = utils.Ptr(true)
		} else {
			opts.Compress = utils.Ptr(false)
		}

		packet := &packet.Packet{
			Type:    packetType,
			Data:    data,
			Options: opts,
		}

		// exports packetCreate event
		s.Emit("packetCreate", clonePacketForEvent(packet))

		s.writeBuffer.Push(packet)

		// add send callback to object, if defined
		if callback != nil {
			s.packetsFn.Push(callback)
		}

	}
}

func clonePacketForEvent(source *packet.Packet) *packet.Packet {
	if source == nil {
		return nil
	}
	cloned := &packet.Packet{Type: source.Type}
	if source.Options != nil {
		cloned.Options = &packet.Options{}
		if source.Options.Compress != nil {
			compress := *source.Options.Compress
			cloned.Options.Compress = &compress
		}
		if source.Options.WsPreEncodedFrame != nil {
			cloned.Options.WsPreEncodedFrame = source.Options.WsPreEncodedFrame.Clone()
		}
	}
	switch data := source.Data.(type) {
	case types.BufferInterface:
		cloned.Data = data.Clone()
	case *strings.Reader:
		position, err := data.Seek(0, io.SeekCurrent)
		if err == nil {
			payload, readErr := io.ReadAll(data)
			_, seekErr := data.Seek(position, io.SeekStart)
			if readErr == nil && seekErr == nil {
				cloned.Data = strings.NewReader(string(payload))
				break
			}
		}
		cloned.Data = source.Data
	case *bytes.Reader:
		position, err := data.Seek(0, io.SeekCurrent)
		if err == nil {
			payload, readErr := io.ReadAll(data)
			_, seekErr := data.Seek(position, io.SeekStart)
			if readErr == nil && seekErr == nil {
				cloned.Data = bytes.NewReader(payload)
				break
			}
		}
		cloned.Data = source.Data
	default:
		cloned.Data = source.Data
	}
	return cloned
}

// Attempts to flush the packets buffer.
func (s *socket) flush() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if s.ReadyState() != "closed" && s.Transport().Writable() {
		if wbuf := s.writeBuffer.AllAndClear(); len(wbuf) > 0 {
			socketLog.Debug("flushing buffer to transport")
			s.Emit("flush", wbuf)
			s.server.Emit("flush", s, wbuf)
			if packetsFn := s.packetsFn.AllAndClear(); len(packetsFn) > 0 {
				s.sentCallbackFn.Push(packetsFn)
			} else {
				s.sentCallbackFn.Push(nil)
			}
			s.Transport().Send(wbuf)
			s.Emit("drain")
			s.server.Emit("drain", s)
		}
	}
}

// Get available upgrades for this socket.
func (s *socket) getAvailableUpgrades() []string {
	availableUpgrades := []string{}
	for _, upg := range s.server.Upgrades(s.Transport().Name()) {
		if s.server.Transports().Has(upg) {
			availableUpgrades = append(availableUpgrades, upg)
		}
	}
	return availableUpgrades
}

// Closes the socket and underlying transport.
func (s *socket) Close(discard bool) {
	if discard {
		for {
			switch s.ReadyState() {
			case "open":
				if !s.readyState.CompareAndSwap("open", "closing") {
					continue
				}
				s.closeTransport(true)
				return
			case "closing":
				s.closeTransport(true)
				return
			default:
				return
			}
		}
	}

	if !s.readyState.CompareAndSwap("open", "closing") {
		return
	}

	s.flushMu.Lock()
	if length := s.writeBuffer.Len(); length > 0 {
		socketLog.Debug("there are %d remaining packets in the buffer, waiting for the 'drain' event", length)
		// Associate the close with this buffered batch. Socket-level "drain"
		// only means that the buffer was handed to Transport.Send; Go
		// transports serialize writes asynchronously, so closing there can
		// overtake the payload. Batch callbacks run from the transport's real
		// drain event and cannot be confused with an earlier in-flight batch.
		s.packetsFn.Push(func(transports.Transport) {
			socketLog.Debug("all packets have been sent, closing the transport")
			s.closeTransport(discard)
		})
		s.flushMu.Unlock()
		return
	}

	// Go transports write asynchronously. A packet can already have left the
	// socket writeBuffer while its transport write is still in flight. Attach
	// graceful close to that batch so Close cannot overtake the final frame.
	if s.sentCallbackFn.Len() > 0 {
		s.sentCallbackFn.DoWrite(func(batches [][]SendCallback) [][]SendCallback {
			batches[0] = append(batches[0], func(transports.Transport) {
				socketLog.Debug("in-flight packets have been sent, closing the transport")
				s.closeTransport(discard)
			})
			return batches
		})
		s.flushMu.Unlock()
		return
	}
	s.flushMu.Unlock()

	socketLog.Debug("the buffer is empty, closing the transport right away")
	s.closeTransport(discard)
}

// Closes the underlying transport.
func (s *socket) closeTransport(discard bool) {
	socketLog.Debug("closing the transport (discard? %t)", discard)
	if discard {
		s.Transport().Discard()
	}
	s.Transport().Close(func() { s.OnClose("forced close") })
}
