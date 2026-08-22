package engine

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

const (
	DefaultClusterResponseTimeout                 = time.Second
	DefaultClusterNoopUpgradeInterval             = 200 * time.Millisecond
	DefaultClusterDelayedConnectionTimeout        = 300 * time.Millisecond
	maxJavaScriptSafeRequestID             uint64 = 1<<53 - 1
)

var ErrClusterBusRequired = errors.New("engine: cluster bus is required")

// ClusterOptions controls the distributed lock and fast-upgrade lifecycle.
type ClusterOptions struct {
	ResponseTimeout          time.Duration
	NoopUpgradeInterval      time.Duration
	DelayedConnectionTimeout time.Duration
}

func normalizeClusterOptions(options *ClusterOptions) ClusterOptions {
	result := ClusterOptions{
		ResponseTimeout:          DefaultClusterResponseTimeout,
		NoopUpgradeInterval:      DefaultClusterNoopUpgradeInterval,
		DelayedConnectionTimeout: DefaultClusterDelayedConnectionTimeout,
	}
	if options == nil {
		return result
	}
	if options.ResponseTimeout > 0 {
		result.ResponseTimeout = options.ResponseTimeout
	}
	if options.NoopUpgradeInterval > 0 {
		result.NoopUpgradeInterval = options.NoopUpgradeInterval
	}
	if options.DelayedConnectionTimeout > 0 {
		result.DelayedConnectionTimeout = options.DelayedConnectionTimeout
	}
	return result
}

// ClusterServer is an Engine.IO server whose follow-up requests may arrive at
// any listener sharing the same ClusterBus.
type ClusterServer interface {
	Server
	NodeID() string
	PendingClusterRequests() int
	RemoteTransportCount() int
}

type clusterPendingRequest struct {
	response chan ClusterMessage
}

type remoteRequest struct {
	sid          string
	ownerID      string
	transport    string
	lock         ClusterLockType
	expectsDrain bool
	stopCleanup  func() bool
}

type delayedConnection struct {
	socket   *socket
	listener types.EventListener
	timer    *utils.Timer
	packets  []ClusterPacket
}

type clusterUpgradeGrant struct {
	senderID      string
	socket        *socket
	token         uint64
	closeListener types.EventListener
}

type trackedTransportPublish struct {
	cancel       context.CancelFunc
	stopShutdown func() bool
}

type transportPublishPermit struct {
	server       *clusterServer
	id           uint64
	ctx          context.Context
	parentCancel context.CancelFunc
	releaseOnce  sync.Once
}

type clusterWriteGrantKey struct {
	sid      string
	senderID string
}

type remoteTransport struct {
	server    *clusterServer
	sid       string
	ownerID   string
	transport transports.Transport

	packetListener   types.EventListener
	errorListener    types.EventListener
	closeListener    types.EventListener
	closed           atomic.Bool
	handoffMu        sync.Mutex
	handedOff        atomic.Pointer[socket]
	transitionMu     sync.Mutex
	transitionPermit *transportPublishPermit
	drainMu          sync.Mutex
	drainArmed       bool
	pendingDrains    [][]*enginepacket.Packet
	upgradePending   atomic.Bool
}

type clusterServer struct {
	Server

	bus     ClusterBus
	options ClusterOptions
	nodeID  string

	unsubscribe          func()
	closed               chan struct{}
	closing              atomic.Bool
	closeStarted         atomic.Bool
	beginOnce            sync.Once
	closeOnce            sync.Once
	lifecycle            context.Context
	cancel               context.CancelFunc
	stateMu              sync.Mutex
	publishStateMu       sync.Mutex
	publishStateClosing  bool
	publishShutdown      context.Context
	transportPublishNext uint64
	transportPublishes   map[uint64]trackedTransportPublish

	requestSequence atomic.Uint64
	requestsMu      sync.Mutex
	requests        map[uint64]*clusterPendingRequest

	remoteRequests sync.Map // map[*types.HttpContext]remoteRequest
	acquireMu      sync.Mutex

	remoteMu         sync.Mutex
	remoteTransports map[string]*remoteTransport
	probeMu          sync.Mutex
	probes           map[transports.Transport]struct{}
	earlyDrainMu     sync.Mutex
	expectedDrains   map[string]int
	earlyDrains      map[string][]ClusterPacket

	delayedMu sync.Mutex
	delayed   map[string]*delayedConnection

	upgradeMu         sync.Mutex
	upgradeNoopTimers map[string]*utils.Timer
	upgradeLockTimers map[string]*utils.Timer
	upgradeGrants     map[string]clusterUpgradeGrant
	writeGrantMu      sync.Mutex
	writeGrants       map[clusterWriteGrantKey]*utils.Timer
	readGrantMu       sync.Mutex
	readGrants        map[clusterWriteGrantKey]*utils.Timer
}

// NewClusterServer constructs and subscribes a cluster-aware Engine.IO server.
// Each server must have its own subscription, while the bus itself may be shared.
func NewClusterServer(bus ClusterBus, serverOptions any, clusterOptions *ClusterOptions) (ClusterServer, error) {
	if bus == nil {
		return nil, ErrClusterBusRequired
	}

	base := MakeServer()
	lifecycle, cancel := context.WithCancel(context.Background())
	server := &clusterServer{
		Server:             base,
		bus:                bus,
		options:            normalizeClusterOptions(clusterOptions),
		nodeID:             newClusterNodeID(),
		closed:             make(chan struct{}),
		lifecycle:          lifecycle,
		cancel:             cancel,
		requests:           make(map[uint64]*clusterPendingRequest),
		transportPublishes: make(map[uint64]trackedTransportPublish),
		remoteTransports:   make(map[string]*remoteTransport),
		probes:             make(map[transports.Transport]struct{}),
		expectedDrains:     make(map[string]int),
		earlyDrains:        make(map[string][]ClusterPacket),
		delayed:            make(map[string]*delayedConnection),
		upgradeNoopTimers:  make(map[string]*utils.Timer),
		upgradeLockTimers:  make(map[string]*utils.Timer),
		upgradeGrants:      make(map[string]clusterUpgradeGrant),
		writeGrants:        make(map[clusterWriteGrantKey]*utils.Timer),
		readGrants:         make(map[clusterWriteGrantKey]*utils.Timer),
	}

	unsubscribe, err := bus.Subscribe(server.nodeID, server.onClusterMessage)
	if err != nil {
		return nil, fmt.Errorf("engine: subscribe cluster bus: %w", err)
	}
	server.unsubscribe = unsubscribe

	base.Prototype(server)
	base.Construct(serverOptions)
	return server, nil
}

func newClusterNodeID() string {
	value := make([]byte, 3)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%06x", time.Now().UnixNano()&0xffffff)
	}
	return hex.EncodeToString(value)
}

func (s *clusterServer) NodeID() string { return s.nodeID }

// Close marks the cluster lifecycle closed before BaseServer snapshots local
// clients. This prevents an in-flight upgrade takeover from registering a new
// client after that snapshot and escaping shutdown.
func (s *clusterServer) Close() BaseServer {
	s.beginClose()
	if !s.closeStarted.CompareAndSwap(false, true) {
		return s
	}
	s.Server.Close()
	return s
}

func (s *clusterServer) beginClose() {
	s.beginOnce.Do(func() {
		s.stateMu.Lock()
		s.closing.Store(true)
		s.publishStateMu.Lock()
		s.publishStateClosing = true
		s.publishStateMu.Unlock()
		s.cancel()
		close(s.closed)
		s.stateMu.Unlock()
	})
}

// GenerateId uses the 20-character Engine.IO SID shape required by the
// official cluster-engine verifier. A user-supplied ServerOptions generator
// still takes precedence in baseServer.generateId.
func (s *clusterServer) GenerateId(*types.HttpContext) string {
	value := make([]byte, 15)
	if _, err := rand.Read(value); err == nil {
		return base64.RawURLEncoding.EncodeToString(value)
	}
	// crypto/rand failures are exceptionally rare. Keep the fallback within the
	// same official length/character contract so cross-runtime routing survives.
	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%015d", time.Now().UnixNano()%1e15))
}

func (s *clusterServer) PendingClusterRequests() int {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	return len(s.requests)
}

func (s *clusterServer) RemoteTransportCount() int {
	s.remoteMu.Lock()
	defer s.remoteMu.Unlock()
	return len(s.remoteTransports)
}

// Verify extends the official server verification only for a well-formed SID
// that is unknown locally. The owner grants a read/write/upgrade lock over the
// bus; all other verification errors retain their original code and context.
func (s *clusterServer) Verify(ctx *types.HttpContext, upgrade bool) (*types.CodeMessage, map[string]any) {
	codeMessage, errorContext := s.Server.Verify(ctx, upgrade)
	if codeMessage != UNKNOWN_SID {
		return codeMessage, errorContext
	}

	sid := ctx.Query().Peek("sid")
	if len(sid) != 20 || !utils.IsValidSid(sid) {
		return codeMessage, errorContext
	}

	lockType := ClusterWriteLock
	if ctx.Method() == http.MethodGet {
		lockType = ClusterReadLock
	}
	ownerID, ok, releasePermit := s.acquireLockWithPermit(
		ctx.Context(),
		sid,
		ctx.Query().Peek("transport"),
		lockType,
	)
	if !ok {
		return codeMessage, errorContext
	}

	expectsDrain := ctx.Query().Peek("transport") == transports.POLLING && lockType == ClusterReadLock
	stopCleanup := context.AfterFunc(ctx.Context(), func() {
		s.forgetRemoteRequest(ctx)
	})
	s.stateMu.Lock()
	if s.closing.Load() {
		s.stateMu.Unlock()
		s.releaseRemoteRequestWithPermit(&remoteRequest{
			sid:          sid,
			ownerID:      ownerID,
			transport:    ctx.Query().Peek("transport"),
			lock:         lockType,
			expectsDrain: expectsDrain,
			stopCleanup:  stopCleanup,
		}, "transport close", releasePermit)
		return codeMessage, errorContext
	}
	s.remoteRequests.Store(ctx, remoteRequest{
		sid:          sid,
		ownerID:      ownerID,
		transport:    ctx.Query().Peek("transport"),
		lock:         lockType,
		expectsDrain: expectsDrain,
		stopCleanup:  stopCleanup,
	})
	s.stateMu.Unlock()
	releasePermit.release()
	// context.AfterFunc may run before Store when the request was already
	// canceled. Re-check after publication so no remote request can be orphaned.
	if ctx.Context().Err() != nil {
		s.forgetRemoteRequest(ctx)
	}
	return nil, nil
}

func (s *clusterServer) acquireLock(
	ctx context.Context,
	sid string,
	transportName string,
	lockType ClusterLockType,
) (string, bool) {
	ownerID, success, releasePermit := s.acquireLockWithPermit(ctx, sid, transportName, lockType)
	if releasePermit != nil {
		releasePermit.release()
	}
	return ownerID, success
}

func (s *clusterServer) acquireLockWithPermit(
	ctx context.Context,
	sid string,
	transportName string,
	lockType ClusterLockType,
) (string, bool, *transportPublishPermit) {
	operationContext, cancelOperation := context.WithTimeout(ctx, s.options.ResponseTimeout)
	defer cancelOperation()
	expectsDrain := transportName == transports.POLLING && lockType == ClusterReadLock
	if expectsDrain {
		s.expectEarlyDrain(sid)
	}
	releaseExpectedDrain := func() {
		if expectsDrain {
			s.releaseEarlyDrain(sid)
		}
	}
	releasePermit, err := s.reserveTimedTransportPublish(false)
	if err != nil {
		releaseExpectedDrain()
		return "", false, nil
	}
	request := &clusterPendingRequest{response: make(chan ClusterMessage, 1)}
	requestID := s.registerRequest(request)

	if err := s.publishContext(operationContext, &ClusterMessage{
		RequestID: requestID,
		Type:      ClusterMessageAcquireLock,
		SID:       sid,
		Transport: transportName,
		LockType:  lockType,
	}); err != nil {
		s.abandonLockRequest(requestID, request, sid, transportName, lockType, releasePermit)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			s.reportClusterError(err)
		}
		return "", false, nil
	}

	select {
	case response := <-request.response:
		if !response.Success {
			releaseExpectedDrain()
			releasePermit.release()
			return response.SenderID, false, nil
		}
		return response.SenderID, true, releasePermit
	case <-operationContext.Done():
		s.abandonLockRequest(requestID, request, sid, transportName, lockType, releasePermit)
		return "", false, nil
	case <-s.closed:
		s.abandonLockRequest(requestID, request, sid, transportName, lockType, releasePermit)
		return "", false, nil
	}
}

func (s *clusterServer) abandonLockRequest(
	requestID uint64,
	request *clusterPendingRequest,
	sid, transportName string,
	lockType ClusterLockType,
	releasePermit *transportPublishPermit,
) {
	go func() {
		var publishErr error
		if transportName == transports.POLLING && lockType == ClusterReadLock {
			defer s.releaseEarlyDrain(sid)
		}
		timer := time.NewTimer(s.options.ResponseTimeout)
		defer timer.Stop()
		select {
		case response := <-request.response:
			if response.Success {
				if message := grantedLockReleaseMessage(
					response.SenderID,
					sid,
					transportName,
					lockType,
					"transport close",
				); message != nil {
					publishErr = releasePermit.publish(message)
				}
			}
		case <-timer.C:
			s.deleteRequest(requestID)
		}
		releasePermit.release()
		s.reportClusterError(publishErr)
	}()
}

func (s *clusterServer) expectEarlyDrain(sid string) {
	s.earlyDrainMu.Lock()
	if s.expectedDrains[sid] == 0 {
		delete(s.earlyDrains, sid)
	}
	s.expectedDrains[sid]++
	s.earlyDrainMu.Unlock()
}

func (s *clusterServer) releaseEarlyDrain(sid string) {
	s.earlyDrainMu.Lock()
	s.releaseEarlyDrainLocked(sid)
	s.earlyDrainMu.Unlock()
}

func (s *clusterServer) releaseEarlyDrainLocked(sid string) {
	current := s.expectedDrains[sid]
	if current == 0 {
		return
	}
	remaining := current - 1
	if remaining > 0 {
		s.expectedDrains[sid] = remaining
		return
	}
	delete(s.expectedDrains, sid)
	delete(s.earlyDrains, sid)
}

func (s *clusterServer) bufferEarlyDrain(sid string, packets []ClusterPacket) bool {
	s.earlyDrainMu.Lock()
	defer s.earlyDrainMu.Unlock()
	if s.expectedDrains[sid] == 0 {
		return false
	}
	s.earlyDrains[sid] = append(s.earlyDrains[sid], packets...)
	return true
}

func (s *clusterServer) takeEarlyDrain(sid string) ([]ClusterPacket, bool) {
	s.earlyDrainMu.Lock()
	packets, found := s.earlyDrains[sid]
	delete(s.earlyDrains, sid)
	s.releaseEarlyDrainLocked(sid)
	s.earlyDrainMu.Unlock()
	return packets, found
}

func (s *clusterServer) deleteRequest(requestID uint64) {
	s.requestsMu.Lock()
	delete(s.requests, requestID)
	s.requestsMu.Unlock()
}

func (s *clusterServer) registerRequest(request *clusterPendingRequest) uint64 {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	for {
		current := s.requestSequence.Load()
		next := current + 1
		if next > maxJavaScriptSafeRequestID {
			next = 1
		}
		s.requestSequence.Store(next)
		if _, pending := s.requests[next]; !pending {
			s.requests[next] = request
			return next
		}
	}
}

func (s *clusterServer) takeRequest(requestID uint64) *clusterPendingRequest {
	s.requestsMu.Lock()
	request := s.requests[requestID]
	delete(s.requests, requestID)
	s.requestsMu.Unlock()
	return request
}

func (s *clusterServer) publish(message *ClusterMessage) error {
	ctx, cancel := context.WithTimeout(s.lifecycle, s.options.ResponseTimeout)
	defer cancel()
	return s.publishContext(ctx, message)
}

func (s *clusterServer) publishContext(ctx context.Context, message *ClusterMessage) error {
	message.Source = clusterMessageSource
	message.SenderID = s.nodeID
	if ctx == nil {
		ctx = context.Background()
	}
	publishContext, cancel := context.WithCancel(ctx)
	stopLifecycle := context.AfterFunc(s.lifecycle, cancel)
	defer func() {
		stopLifecycle()
		cancel()
	}()
	return s.bus.Publish(publishContext, message)
}

// publishTransportMessage keeps the owner/edge close notification available
// during shutdown, while bounding a misbehaving or unavailable bus. Ordinary
// messages use the server lifecycle context and are canceled immediately.
func (s *clusterServer) publishTransportMessage(message *ClusterMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.options.ResponseTimeout)
	defer cancel()
	return s.publishTransportMessageTracked(ctx, message, false)
}

func (s *clusterServer) publishTransportMessageContext(ctx context.Context, message *ClusterMessage) error {
	return s.publishTransportMessageTracked(ctx, message, true)
}

func (s *clusterServer) publishTransportMessageTracked(
	ctx context.Context,
	message *ClusterMessage,
	allowClosing bool,
) error {
	permit, err := s.reserveTransportPublish(ctx, allowClosing, nil)
	if err != nil {
		return err
	}
	defer permit.release()
	return permit.publish(message)
}

func (s *clusterServer) reserveTimedTransportPublish(
	allowClosing bool,
) (*transportPublishPermit, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return s.reserveTransportPublish(ctx, allowClosing, cancel)
}

func (s *clusterServer) reserveTransportPublish(
	ctx context.Context,
	allowClosing bool,
	parentCancel context.CancelFunc,
) (*transportPublishPermit, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	publishCtx, cancel := context.WithCancel(ctx)
	s.publishStateMu.Lock()
	if s.publishStateClosing && !allowClosing {
		s.publishStateMu.Unlock()
		cancel()
		if parentCancel != nil {
			parentCancel()
		}
		return nil, context.Canceled
	}
	s.transportPublishNext++
	if s.transportPublishNext == 0 {
		s.transportPublishNext++
	}
	publishID := s.transportPublishNext
	tracked := trackedTransportPublish{cancel: cancel}
	if s.publishShutdown != nil {
		tracked.stopShutdown = context.AfterFunc(s.publishShutdown, cancel)
	}
	s.transportPublishes[publishID] = tracked
	s.publishStateMu.Unlock()
	return &transportPublishPermit{
		server:       s,
		id:           publishID,
		ctx:          publishCtx,
		parentCancel: parentCancel,
	}, nil
}

func (p *transportPublishPermit) publish(message *ClusterMessage) error {
	if p == nil {
		return context.Canceled
	}
	message.Source = clusterMessageSource
	message.SenderID = p.server.nodeID
	ctx, cancel := context.WithTimeout(p.ctx, p.server.options.ResponseTimeout)
	defer cancel()
	return p.server.bus.Publish(ctx, message)
}

func (p *transportPublishPermit) release() {
	if p == nil {
		return
	}
	p.releaseOnce.Do(func() {
		p.server.publishStateMu.Lock()
		tracked := p.server.transportPublishes[p.id]
		delete(p.server.transportPublishes, p.id)
		p.server.publishStateMu.Unlock()
		if tracked.stopShutdown != nil {
			tracked.stopShutdown()
		}
		if tracked.cancel != nil {
			tracked.cancel()
		}
		if p.parentCancel != nil {
			p.parentCancel()
		}
	})
}

func (s *clusterServer) beginTransportPublishShutdown(ctx context.Context) {
	s.publishStateMu.Lock()
	s.publishStateClosing = true
	s.publishShutdown = ctx
	for publishID, tracked := range s.transportPublishes {
		if tracked.stopShutdown == nil {
			tracked.stopShutdown = context.AfterFunc(ctx, tracked.cancel)
			s.transportPublishes[publishID] = tracked
		}
	}
	s.publishStateMu.Unlock()
}

func (s *clusterServer) activeTransportPublishes() int {
	s.publishStateMu.Lock()
	count := len(s.transportPublishes)
	s.publishStateMu.Unlock()
	return count
}

func (s *clusterServer) cancelTransportPublishes() {
	s.publishStateMu.Lock()
	cancellations := make([]context.CancelFunc, 0, len(s.transportPublishes))
	for _, tracked := range s.transportPublishes {
		cancellations = append(cancellations, tracked.cancel)
	}
	s.publishStateMu.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}

func (s *clusterServer) reportClusterError(err error) {
	if err != nil {
		s.Emit("cluster_error", err)
	}
}

func (s *clusterServer) reportClusterErrors(errs []error) {
	for _, err := range errs {
		s.reportClusterError(err)
	}
}

func (s *clusterServer) releaseTransportPermitAndReport(
	permit *transportPublishPermit,
	errs []error,
) {
	if permit != nil {
		permit.release()
	}
	s.reportClusterErrors(errs)
}

func (s *clusterServer) onClusterMessage(message *ClusterMessage) {
	if message == nil {
		return
	}
	if s.closing.Load() &&
		message.Type != ClusterMessageAcquireLockResponse &&
		message.Type != ClusterMessageUpgradeResponse {
		return
	}
	if message.Source != clusterMessageSource || message.SenderID == "" || message.SenderID == s.nodeID {
		return
	}
	if message.RecipientID != "" && message.RecipientID != s.nodeID {
		return
	}

	switch message.Type {
	case ClusterMessageAcquireLock:
		s.onAcquireLock(message)
	case ClusterMessageAcquireLockResponse, ClusterMessageUpgradeResponse:
		if request := s.takeRequest(message.RequestID); request != nil {
			request.response <- *message
		}
	case ClusterMessageDrain:
		s.onDrain(message)
	case ClusterMessagePacket:
		s.onRemotePacket(message)
	case ClusterMessageUpgrade:
		s.onRemoteUpgrade(message)
	case ClusterMessageClose:
		s.onRemoteClose(message)
	}
}

func (s *clusterServer) onAcquireLock(message *ClusterMessage) {
	if s.closing.Load() {
		return
	}
	client, found := s.Clients().Load(message.SID)
	if !found {
		return
	}
	socket, ok := client.(*socket)
	if !ok {
		return
	}

	// Redis dispatches messages serially, as does the JavaScript event loop used
	// by the official implementation. MemoryClusterBus may invoke this method
	// concurrently, so serialize the lock decision and its state transition.
	//
	// A bus is allowed to deliver DRAIN before the owner's Publish call returns.
	// In that case the client can issue its next polling GET while the one-shot
	// forwarder is only a few instructions away from restoring the base transport.
	// The official Node.js implementation cannot interleave another message in
	// that same call stack. Recreate that ordering here by waiting only after a
	// DRAIN publication has begun. sendMu makes the wait-or-reject decision
	// linearizable with drainQueue setting releasePending before Publish.
	var waitFinished *forwardingTransport
	for {
		s.acquireMu.Lock()
		current, forwarding := socket.Transport().(*forwardingTransport)
		if !forwarding || !current.oneShot || current == waitFinished {
			break
		}
		if !current.oneShotReleasePending() {
			break
		}
		s.acquireMu.Unlock()

		timer := time.NewTimer(s.options.ResponseTimeout)
		select {
		case <-current.done:
			timer.Stop()
			// Normal completion restores another transport before closing done.
			// A terminal CLOSE deliberately leaves this inactive forwarder in the
			// slot; bypass the wait once so the ordinary state checks reject it.
			waitFinished = current
			continue
		case <-s.closed:
			timer.Stop()
			return
		case <-timer.C:
			return
		}
	}
	defer s.acquireMu.Unlock()

	s.stateMu.Lock()
	if s.closing.Load() {
		s.stateMu.Unlock()
		return
	}
	socket.requestMu.Lock()
	success := socket.ReadyState() == "open" && isClusterSocketLockable(socket, message.Transport, message.LockType)
	var reserved *forwardingTransport
	var upgradeToken uint64
	if success {
		switch message.Transport {
		case transports.POLLING:
			if message.LockType == ClusterReadLock {
				// Prevent concurrent Send/heartbeat flushes from consuming owner
				// packets before the lock response is durably published.
				socket.flushMu.Lock()
				expected := socket.transport.Load()
				success = expected != nil && isClusterSocketLockable(socket, message.Transport, message.LockType)
				if success {
					reserved = newForwardingTransport(s, socket, *expected, message.SenderID, true)
					success = socket.reserveClusterTransportCAS(reserved, expected)
				}
				if !success {
					reserved = nil
					socket.flushMu.Unlock()
				}
			}
		case transports.WEBSOCKET, transports.WEBTRANSPORT:
			upgradeToken, success = socket.beginUpgrade()
			if success {
				s.registerUpgradeAttempt(socket, message.SenderID, upgradeToken)
			}
		}
	}
	socket.requestMu.Unlock()
	if success && message.Transport == transports.POLLING && message.LockType == ClusterWriteLock {
		s.recordWriteGrant(message.SID, message.SenderID)
	}

	err := s.publish(&ClusterMessage{
		RecipientID: message.SenderID,
		RequestID:   message.RequestID,
		Type:        ClusterMessageAcquireLockResponse,
		Success:     success,
	})
	if err != nil {
		if reserved != nil {
			// Publish may report an error after the Success response was delivered.
			// Complete that possible remote poll with CLOSE before rolling back the
			// reservation; the requester already has an early-drain slot armed.
			if closeErr := s.publishTransportMessage(&ClusterMessage{
				RecipientID: message.SenderID,
				Type:        ClusterMessageDrain,
				SID:         message.SID,
				Packets: []ClusterPacket{{
					Type: enginepacket.CLOSE.String(),
				}},
			}); closeErr != nil {
				s.reportClusterError(closeErr)
			}
			reserved.deactivateAndRestore()
			socket.flushMu.Unlock()
		}
		if success && (message.Transport == transports.WEBSOCKET || message.Transport == transports.WEBTRANSPORT) {
			s.clearUpgradeAttempt(message.SID, message.SenderID, socket, upgradeToken)
		}
		s.stateMu.Unlock()
		s.reportClusterError(err)
		return
	}
	if success && (message.Transport == transports.WEBSOCKET || message.Transport == transports.WEBTRANSPORT) {
		s.armUpgradeAttempt(message.SID, message.SenderID, socket, upgradeToken)
	}
	// Match the official event ordering: grant the lock before publishing any
	// queued packets. The requester buffers a drain until its transport is armed.
	if reserved != nil {
		socket.flushMu.Unlock()
	}
	s.stateMu.Unlock()
	if reserved != nil {
		if s.closing.Load() || socket.ReadyState() != "open" {
			return
		}
		// Reservation happened before the response so a local GET cannot overlap;
		// flush only afterwards to preserve ACQUIRE_LOCK_RESPONSE -> DRAIN order.
		socket.flush()
	}
}

func (s *clusterServer) recordWriteGrant(sid, senderID string) {
	key := clusterWriteGrantKey{sid: sid, senderID: senderID}
	lease := s.Opts().IdleTimeout()
	if pingTimeout := s.Opts().PingTimeout(); pingTimeout > lease {
		lease = pingTimeout
	}
	if lease <= 0 {
		lease = s.options.ResponseTimeout
	}
	s.writeGrantMu.Lock()
	previous := s.writeGrants[key]
	var timer *utils.Timer
	timer = utils.SetTimeout(func() {
		s.writeGrantMu.Lock()
		if s.writeGrants[key] == timer {
			delete(s.writeGrants, key)
		}
		s.writeGrantMu.Unlock()
	}, lease)
	s.writeGrants[key] = timer
	s.writeGrantMu.Unlock()
	utils.ClearTimeout(previous)
}

func (s *clusterServer) clearWriteGrant(sid, senderID string) bool {
	key := clusterWriteGrantKey{sid: sid, senderID: senderID}
	s.writeGrantMu.Lock()
	timer, found := s.writeGrants[key]
	delete(s.writeGrants, key)
	s.writeGrantMu.Unlock()
	utils.ClearTimeout(timer)
	return found
}

// A completed one-shot read keeps a short release lease. A requester may have
// timed out before observing the grant and send CLOSE only after the owner has
// published DRAIN and restored its physical transport.
func (s *clusterServer) recordReadGrant(sid, senderID string) {
	key := clusterWriteGrantKey{sid: sid, senderID: senderID}
	s.readGrantMu.Lock()
	previous := s.readGrants[key]
	var timer *utils.Timer
	timer = utils.SetTimeout(func() {
		s.readGrantMu.Lock()
		if s.readGrants[key] == timer {
			delete(s.readGrants, key)
		}
		s.readGrantMu.Unlock()
	}, 2*s.options.ResponseTimeout)
	s.readGrants[key] = timer
	s.readGrantMu.Unlock()
	utils.ClearTimeout(previous)
}

func (s *clusterServer) clearReadGrant(sid, senderID string) bool {
	key := clusterWriteGrantKey{sid: sid, senderID: senderID}
	s.readGrantMu.Lock()
	timer, found := s.readGrants[key]
	delete(s.readGrants, key)
	s.readGrantMu.Unlock()
	utils.ClearTimeout(timer)
	return found
}

func isClusterSocketLockable(socket *socket, transportName string, lockType ClusterLockType) bool {
	current := socket.Transport()
	if current == nil {
		return false
	}
	switch transportName {
	case transports.POLLING:
		if forwarder, forwarding := current.(*forwardingTransport); forwarding {
			// A forwarding edge owns the read half until its ordered drain restores
			// the original transport. Writes remain an independent polling channel.
			return forwarder.active.Load() && lockType == ClusterWriteLock
		}
		return current.Name() == transports.POLLING &&
			(lockType == ClusterWriteLock || !current.Writable())
	case transports.WEBSOCKET, transports.WEBTRANSPORT:
		return current.Name() == transports.POLLING && !socket.Upgrading() && !socket.Upgraded()
	default:
		return false
	}
}

func (s *clusterServer) registerUpgradeAttempt(socket *socket, senderID string, token uint64) {
	sid := socket.Id()
	closeListener := types.EventListener(func(...any) {
		s.clearAllUpgradeAttempts(sid, socket)
	})
	s.upgradeMu.Lock()
	previousGrant := s.upgradeGrants[sid]
	previousNoop := s.upgradeNoopTimers[sid]
	previousLock := s.upgradeLockTimers[sid]
	delete(s.upgradeNoopTimers, sid)
	delete(s.upgradeLockTimers, sid)
	s.upgradeGrants[sid] = clusterUpgradeGrant{
		senderID:      senderID,
		socket:        socket,
		token:         token,
		closeListener: closeListener,
	}
	if previousGrant.closeListener != nil {
		previousGrant.socket.RemoveListener("close", previousGrant.closeListener)
	}
	_ = socket.Once("close", closeListener)
	s.upgradeMu.Unlock()
	utils.ClearTimeout(previousNoop)
	utils.ClearTimeout(previousLock)
}

// armUpgradeAttempt starts the lease only after the successful lock response
// has been published. This prevents a slow bus from expiring the owner grant
// and then delivering a stale Success=true response to the edge.
func (s *clusterServer) armUpgradeAttempt(sid, senderID string, socket *socket, token uint64) {
	s.upgradeMu.Lock()
	grant, found := s.upgradeGrants[sid]
	if !found || grant.socket != socket || grant.senderID != senderID || grant.token != token {
		s.upgradeMu.Unlock()
		return
	}
	previousNoop := s.upgradeNoopTimers[sid]
	previousLock := s.upgradeLockTimers[sid]
	var noopTimer *utils.Timer
	noopTimer = utils.SetTimeout(func() {
		s.upgradeMu.Lock()
		grant := s.upgradeGrants[sid]
		if s.upgradeNoopTimers[sid] != noopTimer || grant.socket != socket || grant.senderID != senderID || grant.token != token {
			s.upgradeMu.Unlock()
			return
		}
		delete(s.upgradeNoopTimers, sid)
		s.upgradeMu.Unlock()
		if !s.closing.Load() && socket.Upgrading() && socket.ReadyState() == "open" {
			socket.sendPacket(enginepacket.NOOP, nil, nil, nil)
		}
	}, s.options.NoopUpgradeInterval)
	var lockTimer *utils.Timer
	lockTimer = utils.SetTimeout(func() {
		s.upgradeMu.Lock()
		grant := s.upgradeGrants[sid]
		if s.upgradeLockTimers[sid] != lockTimer || grant.socket != socket || grant.senderID != senderID || grant.token != token {
			s.upgradeMu.Unlock()
			return
		}
		delete(s.upgradeLockTimers, sid)
		delete(s.upgradeGrants, sid)
		pendingNoop := s.upgradeNoopTimers[sid]
		delete(s.upgradeNoopTimers, sid)
		grant.socket.RemoveListener("close", grant.closeListener)
		s.upgradeMu.Unlock()
		utils.ClearTimeout(pendingNoop)
		socket.finishUpgrade(token, false)
	}, s.Opts().UpgradeTimeout())
	s.upgradeNoopTimers[sid] = noopTimer
	s.upgradeLockTimers[sid] = lockTimer
	s.upgradeMu.Unlock()
	utils.ClearTimeout(previousNoop)
	utils.ClearTimeout(previousLock)
}

func (s *clusterServer) clearUpgradeAttempt(sid, senderID string, socket *socket, token uint64) bool {
	s.upgradeMu.Lock()
	grant, found := s.upgradeGrants[sid]
	if !found || grant.socket != socket || grant.senderID != senderID || grant.token != token {
		s.upgradeMu.Unlock()
		return false
	}
	noopTimer := s.upgradeNoopTimers[sid]
	lockTimer := s.upgradeLockTimers[sid]
	delete(s.upgradeNoopTimers, sid)
	delete(s.upgradeLockTimers, sid)
	delete(s.upgradeGrants, sid)
	grant.socket.RemoveListener("close", grant.closeListener)
	s.upgradeMu.Unlock()
	utils.ClearTimeout(noopTimer)
	utils.ClearTimeout(lockTimer)
	socket.finishUpgrade(grant.token, false)
	return true
}

func (s *clusterServer) clearAllUpgradeAttempts(sid string, socket *socket) {
	s.upgradeMu.Lock()
	grant, found := s.upgradeGrants[sid]
	if !found || (socket != nil && grant.socket != socket) {
		s.upgradeMu.Unlock()
		return
	}
	noopTimer := s.upgradeNoopTimers[sid]
	lockTimer := s.upgradeLockTimers[sid]
	delete(s.upgradeNoopTimers, sid)
	delete(s.upgradeLockTimers, sid)
	delete(s.upgradeGrants, sid)
	grant.socket.RemoveListener("close", grant.closeListener)
	s.upgradeMu.Unlock()
	utils.ClearTimeout(noopTimer)
	utils.ClearTimeout(lockTimer)
	grant.socket.finishUpgrade(grant.token, false)
}

func (s *clusterServer) consumeUpgradeGrant(sid, senderID string, socket *socket) (uint64, bool) {
	s.upgradeMu.Lock()
	grant, found := s.upgradeGrants[sid]
	if !found || grant.senderID != senderID || grant.socket != socket || !socket.hasActiveUpgrade(grant.token) {
		s.upgradeMu.Unlock()
		return 0, false
	}
	noopTimer := s.upgradeNoopTimers[sid]
	lockTimer := s.upgradeLockTimers[sid]
	delete(s.upgradeNoopTimers, sid)
	delete(s.upgradeLockTimers, sid)
	delete(s.upgradeGrants, sid)
	grant.socket.RemoveListener("close", grant.closeListener)
	s.upgradeMu.Unlock()
	utils.ClearTimeout(noopTimer)
	utils.ClearTimeout(lockTimer)
	return grant.token, true
}

func (s *clusterServer) handleRemoteRequest(ctx *types.HttpContext) bool {
	if s.closing.Load() {
		s.forgetRemoteRequest(ctx)
		return false
	}
	request, permit, found := s.takeRemoteRequest(ctx)
	if !found {
		return false
	}
	if request.stopCleanup != nil {
		request.stopCleanup()
	}
	if ctx.Context().Err() != nil {
		s.releaseRemoteRequestWithPermit(&request, "transport close", permit)
		return false
	}
	transport, err := s.CreateTransport(ctx.Query().Peek("transport"), ctx)
	if err != nil {
		s.releaseRemoteRequestWithPermit(&request, "transport error", permit)
		s.reportClusterError(err)
		return false
	}
	if s.closing.Load() {
		s.releaseRemoteRequestWithPermit(&request, "transport close", permit)
		transport.Discard()
		transport.Close()
		return false
	}
	s.configureRemoteTransport(transport)
	remote := s.hookRemoteTransport(request.sid, request.ownerID, transport, false)
	remote.setTransitionPermit(permit)

	// Arm the HTTP edge first. A DRAIN that arrives before registration is held
	// by earlyDrains; registering first would let Cleanup close the transport and
	// then OnRequest resurrect a writable orphan. The request's terminal permit
	// is temporarily owned by remote: any synchronous transport failure consumes
	// and releases it before emitting cluster_error.
	transport.OnRequest(ctx)
	if request.lock == ClusterReadLock {
		if !s.storeRemoteTransport(remote) {
			s.releaseRemoteRequestWithPermit(
				&request,
				"transport close",
				remote.takeTransitionPermit(),
			)
			return false
		}
	}

	if request.lock == ClusterWriteLock {
		if transitionPermit := remote.takeTransitionPermit(); transitionPermit != nil {
			transitionPermit.release()
		}
		remote.stop()
	} else {
		// The remote map is now the shutdown-visible owner of this polling edge;
		// release the transition before armDrain can synchronously write/close it.
		if transitionPermit := remote.takeTransitionPermit(); transitionPermit != nil {
			transitionPermit.release()
		}
		remote.armDrain()
		if request.expectsDrain {
			if packets, found := s.takeEarlyDrain(remote.sid); found {
				remote.deliverDrain(clusterPacketsToPackets(packets))
			}
		}
	}
	return true
}

func (s *clusterServer) configureRemoteTransport(transport transports.Transport) {
	switch transport.Name() {
	case transports.POLLING:
		transport.SetMaxHttpBufferSize(s.Opts().MaxHttpBufferSize())
		transport.SetHttpCompression(s.Opts().HttpCompression())
	case transports.WEBSOCKET:
		transport.SetPerMessageDeflate(s.Opts().PerMessageDeflate())
	}
}

func (s *clusterServer) hookRemoteTransport(
	sid, ownerID string,
	transport transports.Transport,
	upgradePending bool,
) *remoteTransport {
	remote := &remoteTransport{server: s, sid: sid, ownerID: ownerID, transport: transport}
	remote.upgradePending.Store(upgradePending)
	remote.packetListener = func(values ...any) {
		packet, _ := values[0].(*enginepacket.Packet)
		wirePacket, err := packetToClusterPacket(packet)
		if err != nil {
			remote.fail()
			s.reportClusterError(err)
			return
		}
		if err := s.publish(&ClusterMessage{
			RecipientID: ownerID,
			Type:        ClusterMessagePacket,
			SID:         sid,
			Packet:      &wirePacket,
		}); err != nil {
			// A packet cannot be acknowledged locally after it was lost on the
			// cluster bus. Close both halves so POST does not return as a silent
			// success and an upgraded edge cannot remain falsely healthy.
			remote.fail()
			s.reportClusterError(err)
		}
	}
	remote.errorListener = func(...any) { remote.fail() }
	remote.closeListener = func(...any) { remote.notifyClose("transport close") }
	_ = transport.On("packet", remote.packetListener)
	_ = transport.Once("error", remote.errorListener)
	_ = transport.Once("close", remote.closeListener)
	return remote
}

func (r *remoteTransport) setTransitionPermit(permit *transportPublishPermit) {
	r.transitionMu.Lock()
	r.transitionPermit = permit
	r.transitionMu.Unlock()
}

func (r *remoteTransport) takeTransitionPermit() *transportPublishPermit {
	r.transitionMu.Lock()
	permit := r.transitionPermit
	r.transitionPermit = nil
	r.transitionMu.Unlock()
	return permit
}

func (r *remoteTransport) notifyClose(reason string) {
	permit := r.takeTransitionPermit()
	if permit == nil {
		permit, _ = r.server.reserveTimedTransportPublish(false)
	}
	if permit == nil {
		// Shutdown admission closed before this remote detached. Leave it in the
		// registry so Cleanup's atomic snapshot can publish/close it.
		return
	}
	r.handoffMu.Lock()
	if r.closed.Swap(true) {
		handedOff := r.handedOff.Load()
		r.handoffMu.Unlock()
		permit.release()
		if handedOff != nil {
			handedOff.OnClose(reason)
		}
		return
	}
	r.removeListeners()
	r.server.deleteRemoteTransport(r.sid, r)
	r.handoffMu.Unlock()
	publishErrs := make([]error, 0, 2)
	if r.upgradePending.Load() {
		if message := grantedLockReleaseMessage(
			r.ownerID,
			r.sid,
			r.transport.Name(),
			ClusterReadLock,
			reason,
		); message != nil {
			publishErrs = append(publishErrs, permit.publish(message))
		}
	}
	publishErrs = append(publishErrs, permit.publish(&ClusterMessage{
		RecipientID: r.ownerID,
		Type:        ClusterMessageClose,
		SID:         r.sid,
		Reason:      reason,
	}))
	permit.release()
	r.server.reportClusterErrors(publishErrs)
}

func (r *remoteTransport) fail() {
	r.notifyClose("transport error")
	r.transport.Discard()
	r.transport.Close()
}

func (r *remoteTransport) stop() {
	r.handoffMu.Lock()
	if r.closed.Swap(true) {
		r.handoffMu.Unlock()
		return
	}
	r.removeListeners()
	r.server.deleteRemoteTransport(r.sid, r)
	r.handoffMu.Unlock()
	r.transport.Discard()
	r.transport.Close()
}

func (r *remoteTransport) removeListeners() {
	r.transport.RemoveListener("packet", r.packetListener)
	r.transport.RemoveListener("error", r.errorListener)
	r.transport.RemoveListener("close", r.closeListener)
}

func (s *clusterServer) storeRemoteTransport(remote *remoteTransport) bool {
	s.remoteMu.Lock()
	if s.closing.Load() || remote.closed.Load() {
		s.remoteMu.Unlock()
		remote.stop()
		return false
	}
	previous := s.remoteTransports[remote.sid]
	s.remoteTransports[remote.sid] = remote
	s.remoteMu.Unlock()
	if previous != nil && previous != remote {
		previous.stop()
	}
	return true
}

func (s *clusterServer) loadRemoteTransport(sid string) *remoteTransport {
	s.remoteMu.Lock()
	remote := s.remoteTransports[sid]
	s.remoteMu.Unlock()
	return remote
}

func (s *clusterServer) deleteRemoteTransport(sid string, expected *remoteTransport) {
	s.remoteMu.Lock()
	if s.remoteTransports[sid] == expected {
		delete(s.remoteTransports, sid)
	}
	s.remoteMu.Unlock()
}

func (s *clusterServer) takeRemoteTransport(sid string, expected *remoteTransport) *remoteTransport {
	s.remoteMu.Lock()
	remote := s.remoteTransports[sid]
	if remote == expected {
		delete(s.remoteTransports, sid)
	} else {
		remote = nil
	}
	s.remoteMu.Unlock()
	return remote
}

func (s *clusterServer) onDrain(message *ClusterMessage) {
	remote := s.loadRemoteTransport(message.SID)
	if remote == nil {
		s.bufferEarlyDrain(message.SID, message.Packets)
		return
	}
	if remote.closed.Load() {
		return
	}
	if remote.transport.Name() == transports.POLLING {
		remote = s.takeRemoteTransport(message.SID, remote)
		if remote == nil || remote.closed.Load() {
			return
		}
	}
	packets := clusterPacketsToPackets(message.Packets)
	remote.deliverDrain(packets)
}

func (r *remoteTransport) armDrain() {
	r.drainMu.Lock()
	if r.closed.Load() {
		r.pendingDrains = nil
		r.drainMu.Unlock()
		return
	}
	r.drainArmed = true
	pending := r.pendingDrains
	r.pendingDrains = nil
	for _, packets := range pending {
		r.sendDrain(packets)
	}
	r.drainMu.Unlock()
}

func (r *remoteTransport) deliverDrain(packets []*enginepacket.Packet) {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	if r.closed.Load() {
		return
	}
	if !r.drainArmed {
		r.pendingDrains = append(r.pendingDrains, packets)
		return
	}
	r.sendDrain(packets)
}

func (r *remoteTransport) sendDrain(packets []*enginepacket.Packet) {
	if r.transport.Name() == transports.POLLING {
		_ = r.transport.Once("drain", func(...any) {
			r.stop()
		})
		r.transport.Send(packets)
		return
	}
	// WebSocket transports remain in remoteTransports for their whole lifetime;
	// removing and reinserting here races close and can resurrect a closed edge.
	r.transport.Send(packets)
}

func (s *clusterServer) onRemotePacket(message *ClusterMessage) {
	if message.Packet == nil {
		return
	}
	client, found := s.Clients().Load(message.SID)
	if !found {
		return
	}
	socket, ok := client.(*socket)
	if !ok {
		return
	}
	socket.onPacket(clusterPacketToPacket(*message.Packet))
}

func (s *clusterServer) onRemoteClose(message *ClusterMessage) {
	// CLOSE is bidirectional. On an edge node it tears down the transport whose
	// owner rejected or lost an upgrade; on the owner it is validated against the
	// active forwarder/write grant before closing the authoritative Socket.
	if remote := s.loadRemoteTransport(message.SID); remote != nil && remote.ownerID == message.SenderID {
		remote.stop()
		return
	}
	client, found := s.Clients().Load(message.SID)
	if !found {
		return
	}
	socket, ok := client.(*socket)
	if !ok {
		return
	}
	forwarder, forwarded := socket.Transport().(*forwardingTransport)
	if forwarded && forwarder.active.Load() {
		if forwarder.targetID != message.SenderID {
			return
		}
	} else {
		if !s.clearReadGrant(message.SID, message.SenderID) &&
			!s.clearWriteGrant(message.SID, message.SenderID) {
			return
		}
	}
	s.doConnect(message.SID, socket)
	reason := message.Reason
	if reason == "" {
		reason = "transport close"
	}
	socket.OnClose(reason)
}

func (s *clusterServer) forgetRemoteRequest(ctx *types.HttpContext) {
	request, permit, found := s.takeRemoteRequest(ctx)
	if !found {
		return
	}
	s.releaseRemoteRequestWithPermit(&request, "transport close", permit)
}

func (s *clusterServer) takeRemoteRequest(
	ctx *types.HttpContext,
) (remoteRequest, *transportPublishPermit, bool) {
	permit, err := s.reserveTimedTransportPublish(false)
	if err != nil {
		// Admission closes before Cleanup snapshots remoteRequests. Keeping the
		// entry visible lets the shutdown batch release the owner grant.
		return remoteRequest{}, nil, false
	}
	value, found := s.remoteRequests.LoadAndDelete(ctx)
	if !found {
		permit.release()
		return remoteRequest{}, nil, false
	}
	return value.(remoteRequest), permit, true
}

func (s *clusterServer) releaseRemoteRequestWithPermit(
	request *remoteRequest,
	reason string,
	permit *transportPublishPermit,
) {
	if request.stopCleanup != nil {
		request.stopCleanup()
	}
	if request.expectsDrain {
		s.releaseEarlyDrain(request.sid)
	}
	if permit == nil {
		return
	}
	message := grantedLockReleaseMessage(request.ownerID, request.sid, request.transport, request.lock, reason)
	if message == nil {
		permit.release()
		return
	}
	publishErr := permit.publish(message)
	permit.release()
	s.reportClusterError(publishErr)
}

func grantedLockReleaseMessage(
	ownerID, sid, transportName string,
	lockType ClusterLockType,
	reason string,
) *ClusterMessage {
	if transportName == transports.POLLING &&
		(lockType == ClusterReadLock || lockType == ClusterWriteLock) {
		return &ClusterMessage{
			RecipientID: ownerID,
			Type:        ClusterMessageClose,
			SID:         sid,
			Reason:      reason,
		}
	}
	if lockType == ClusterReadLock &&
		(transportName == transports.WEBSOCKET || transportName == transports.WEBTRANSPORT) {
		return &ClusterMessage{
			RecipientID: ownerID,
			Type:        ClusterMessageUpgrade,
			SID:         sid,
			Success:     false,
		}
	}
	return nil
}

func (s *clusterServer) publishUpgradeFailure(
	permit *transportPublishPermit,
	ownerID, sid, transportName string,
	includeClose bool,
) []error {
	if permit == nil {
		return nil
	}
	const reason = "transport close"
	publishErrs := make([]error, 0, 2)
	if message := grantedLockReleaseMessage(
		ownerID,
		sid,
		transportName,
		ClusterReadLock,
		reason,
	); message != nil {
		publishErrs = append(publishErrs, permit.publish(message))
	}
	if includeClose {
		publishErrs = append(publishErrs, permit.publish(&ClusterMessage{
			RecipientID: ownerID,
			Type:        ClusterMessageClose,
			SID:         sid,
			Reason:      reason,
		}))
	}
	return publishErrs
}

func (s *clusterServer) handleRemoteWebSocket(ctx *types.HttpContext, _ *types.WebSocketConn) bool {
	if s.closing.Load() {
		s.forgetRemoteRequest(ctx)
		return false
	}
	request, permit, found := s.takeRemoteRequest(ctx)
	if !found {
		return false
	}
	if request.stopCleanup != nil {
		request.stopCleanup()
	}
	if ctx.Context().Err() != nil {
		s.releaseRemoteRequestWithPermit(&request, "transport close", permit)
		return false
	}
	ctx.IdleTimeout = s.Opts().IdleTimeout()
	transport, err := s.CreateTransport(ctx.Query().Peek("transport"), ctx)
	if err != nil {
		s.releaseRemoteRequestWithPermit(&request, "transport error", permit)
		s.reportClusterError(err)
		return false
	}
	if s.closing.Load() {
		s.releaseRemoteRequestWithPermit(&request, "transport close", permit)
		transport.Discard()
		transport.Close()
		return false
	}
	s.configureRemoteTransport(transport)
	sid := ctx.Query().Peek("sid")
	return s.tryRemoteUpgrade(sid, request.ownerID, transport, ctx, permit)
}

func (s *clusterServer) tryRemoteUpgrade(
	sid, ownerID string,
	transport transports.Transport,
	ctx *types.HttpContext,
	terminalPermit *transportPublishPermit,
) bool {
	if !s.trackProbe(transport) {
		publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), false)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		transport.Discard()
		transport.Close()
		return false
	}
	var setupMu sync.Mutex
	var packetListener, errorListener, closeListener types.EventListener
	var once sync.Once
	var timer *utils.Timer
	var committedRemote atomic.Pointer[remoteTransport]
	finish := func(success bool) bool {
		setupMu.Lock()
		completed := false
		var remote *remoteTransport
		onFinish := func() {
			completed = true
			// On success, attach the long-lived remote listeners before removing
			// probe listeners. A concurrent WebSocket close is therefore observed
			// by at least one owner throughout the handoff.
			if success {
				remote = s.hookRemoteTransport(sid, ownerID, transport, true)
				committedRemote.Store(remote)
			}
			s.untrackProbe(transport)
			utils.ClearTimeout(timer)
			transport.RemoveListener("packet", packetListener)
			transport.RemoveListener("error", errorListener)
			transport.RemoveListener("close", closeListener)
		}
		once.Do(onFinish)
		setupMu.Unlock()
		if !completed {
			return false
		}
		if success {
			s.onRemoteUpgradeSuccess(remote, ctx, terminalPermit)
		} else {
			publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), false)
			s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
			transport.Close()
		}
		return true
	}
	fail := func() { finish(false) }
	transportFail := func() {
		if finish(false) {
			return
		}
		if remote := committedRemote.Load(); remote != nil {
			remote.fail()
		}
	}
	packetListener = func(values ...any) {
		packet, _ := values[0].(*enginepacket.Packet)
		if packet == nil {
			fail()
			return
		}
		if packet.Type == enginepacket.PING && packetDataString(packet) == "probe" {
			transport.Send([]*enginepacket.Packet{{Type: enginepacket.PONG, Data: strings.NewReader("probe")}})
			return
		}
		if packet.Type != enginepacket.UPGRADE {
			fail()
			return
		}
		finish(true)
	}
	errorListener = func(...any) { transportFail() }
	closeListener = func(...any) { transportFail() }

	// Listener attachment and timer arming form one setup critical section. An
	// immediate timeout cannot finish first and leave listeners attached later.
	setupMu.Lock()
	_ = transport.On("packet", packetListener)
	_ = transport.Once("error", errorListener)
	_ = transport.Once("close", closeListener)
	timer = utils.SetTimeout(func() {
		fail()
	}, s.Opts().UpgradeTimeout())
	setupMu.Unlock()
	s.probeMu.Lock()
	_, tracked := s.probes[transport]
	if s.closing.Load() || !tracked {
		s.probeMu.Unlock()
		fail()
		return false
	}
	startTransport(transport)
	s.probeMu.Unlock()
	return true
}

func (s *clusterServer) trackProbe(transport transports.Transport) bool {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.closing.Load() {
		return false
	}
	s.probes[transport] = struct{}{}
	return true
}

func (s *clusterServer) untrackProbe(transport transports.Transport) {
	s.probeMu.Lock()
	delete(s.probes, transport)
	s.probeMu.Unlock()
}

func (s *clusterServer) onRemoteUpgradeSuccess(
	remote *remoteTransport,
	ctx *types.HttpContext,
	terminalPermit *transportPublishPermit,
) {
	if remote == nil {
		terminalPermit.release()
		return
	}
	sid, ownerID, transport := remote.sid, remote.ownerID, remote.transport
	if s.closing.Load() {
		publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), true)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		remote.stop()
		return
	}
	remote.armDrain()
	if !s.storeRemoteTransport(remote) {
		publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), true)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		return
	}
	request := &clusterPendingRequest{response: make(chan ClusterMessage, 1)}
	requestID := s.registerRequest(request)

	if err := s.publish(&ClusterMessage{
		RecipientID: ownerID,
		RequestID:   requestID,
		Type:        ClusterMessageUpgrade,
		SID:         sid,
		Success:     true,
	}); err != nil {
		s.deleteRequest(requestID)
		publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), true)
		remote.upgradePending.Store(false)
		remote.stop()
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		s.reportClusterError(err)
		return
	}
	if remote.closed.Load() {
		s.deleteRequest(requestID)
		publishErrs := s.publishUpgradeFailure(terminalPermit, ownerID, sid, transport.Name(), true)
		remote.upgradePending.Store(false)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		return
	}

	go s.awaitUpgradeResponse(requestID, request, remote, ctx, terminalPermit)
}

func (s *clusterServer) awaitUpgradeResponse(
	requestID uint64,
	request *clusterPendingRequest,
	remote *remoteTransport,
	ctx *types.HttpContext,
	terminalPermit *transportPublishPermit,
) {
	var publishErrs []error
	timer := time.NewTimer(s.options.ResponseTimeout)
	defer timer.Stop()
	select {
	case response := <-request.response:
		if response.TakeOver {
			if s.closing.Load() {
				publishErrs = s.publishUpgradeFailure(
					terminalPermit,
					remote.ownerID,
					remote.sid,
					remote.transport.Name(),
					true,
				)
				remote.upgradePending.Store(false)
				s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
				remote.stop()
				return
			}
			remote.upgradePending.Store(false)
			s.takeOverSocketWithPermit(remote, ctx, response.Packets, terminalPermit)
			return
		}
		remote.upgradePending.Store(false)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
	case <-timer.C:
		s.deleteRequest(requestID)
		publishErrs = s.publishUpgradeFailure(terminalPermit, remote.ownerID, remote.sid, remote.transport.Name(), true)
		remote.upgradePending.Store(false)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		remote.stop()
	case <-s.closed:
		s.deleteRequest(requestID)
		publishErrs = s.publishUpgradeFailure(terminalPermit, remote.ownerID, remote.sid, remote.transport.Name(), true)
		remote.upgradePending.Store(false)
		s.releaseTransportPermitAndReport(terminalPermit, publishErrs)
		remote.stop()
	}
}

func (s *clusterServer) onRemoteUpgrade(message *ClusterMessage) {
	if s.closing.Load() {
		return
	}
	client, found := s.Clients().Load(message.SID)
	if !found {
		return
	}
	socket, ok := client.(*socket)
	if !ok {
		return
	}
	if socket.ReadyState() != "open" {
		return
	}
	// Consuming the upgrade grant removes the last shutdown-visible record of
	// this edge attempt. Reserve its terminal publication first, under the same
	// admission barrier used by Cleanup, so every post-consume failure can still
	// release the remote edge (and Cleanup can wait for the publication).
	terminalPermit, err := s.reserveTimedTransportPublish(false)
	if err != nil {
		return
	}
	upgradeToken, validGrant := s.consumeUpgradeGrant(message.SID, message.SenderID, socket)
	if !validGrant {
		terminalPermit.release()
		return
	}
	if !message.Success {
		socket.finishUpgrade(upgradeToken, false)
		terminalPermit.release()
		return
	}
	upgradeAnnouncement := socket.beginUpgradeAnnouncement()
	packets, takeOver, committed, packetLocked := s.takeDelayedPackets(
		message.SID,
		socket,
		upgradeToken,
	)
	if takeOver {
		if packetLocked {
			defer socket.packetMu.Unlock()
		}
		var publishErr error
		if committed {
			publishErr = terminalPermit.publish(&ClusterMessage{
				RecipientID: message.SenderID,
				RequestID:   message.RequestID,
				Type:        ClusterMessageUpgradeResponse,
				TakeOver:    true,
				Packets:     packets,
			})
		} else {
			publishErr = s.publishActiveEdgeClose(
				terminalPermit,
				message.SenderID,
				message.SID,
				"transport close",
			)
			terminalPermit.release()
			socket.finishUpgradeAnnouncement(upgradeAnnouncement)
			socket.Close(true)
			s.reportClusterError(publishErr)
			return
		}
		terminalPermit.release()
		// The delayed socket is no longer authoritative once takeover commits.
		// User events and the deferred close are released only after the edge has
		// received its terminal response and the publication permit is gone.
		socket.Emit("upgrade")
		socket.finishUpgradeAnnouncement(upgradeAnnouncement)
		socket.Close(true)
		s.reportClusterError(publishErr)
		return
	}

	forwarder := newForwardingTransport(s, socket, socket.Transport(), message.SenderID, false)
	if !socket.installClusterTransport(forwarder) {
		socket.finishUpgrade(upgradeToken, false)
		publishErr := s.publishActiveEdgeClose(
			terminalPermit,
			message.SenderID,
			message.SID,
			"transport close",
		)
		terminalPermit.release()
		socket.finishUpgradeAnnouncement(upgradeAnnouncement)
		s.reportClusterError(publishErr)
		return
	}
	if !socket.finishUpgrade(upgradeToken, true) {
		forwarder.deactivateAndRestore()
		publishErr := s.publishActiveEdgeClose(
			terminalPermit,
			message.SenderID,
			message.SID,
			"transport close",
		)
		terminalPermit.release()
		socket.finishUpgradeAnnouncement(upgradeAnnouncement)
		s.reportClusterError(publishErr)
		return
	}

	publishErr := terminalPermit.publish(&ClusterMessage{
		RecipientID: message.SenderID,
		RequestID:   message.RequestID,
		Type:        ClusterMessageUpgradeResponse,
		TakeOver:    false,
		Packets:     packets,
	})
	terminalPermit.release()
	// The forwarding transport is now the shutdown-visible owner. Do not carry
	// the terminal permit through user upgrade/close callbacks.
	socket.Emit("upgrade")
	socket.finishUpgradeAnnouncement(upgradeAnnouncement)
	if publishErr != nil {
		socket.Close(true)
		s.reportClusterError(publishErr)
	}
}

func (s *clusterServer) publishActiveEdgeClose(
	permit *transportPublishPermit,
	ownerID, sid, reason string,
) error {
	if permit == nil {
		return context.Canceled
	}
	return permit.publish(&ClusterMessage{
		RecipientID: ownerID,
		Type:        ClusterMessageClose,
		SID:         sid,
		Reason:      reason,
	})
}

func (s *clusterServer) takeOverSocket(
	remote *remoteTransport,
	ctx *types.HttpContext,
	buffered []ClusterPacket,
) {
	s.takeOverSocketWithPermit(remote, ctx, buffered, nil)
}

func (s *clusterServer) takeOverSocketWithPermit(
	remote *remoteTransport,
	ctx *types.HttpContext,
	buffered []ClusterPacket,
	terminalPermit *transportPublishPermit,
) {
	releaseTerminal := func() {
		if terminalPermit != nil {
			terminalPermit.release()
		}
	}
	if remote == nil {
		releaseTerminal()
		return
	}
	sid, transport := remote.sid, remote.transport
	s.stateMu.Lock()
	if s.closing.Load() {
		s.stateMu.Unlock()
		releaseTerminal()
		remote.stop()
		return
	}
	suppressed := &suppressSendTransport{Transport: transport}
	client := makeSocket()
	client.packetMu.Lock()
	client.initialize(sid, s, suppressed, ctx, 4)
	if !client.onOpen() {
		client.markConnectionReady()
		client.packetMu.Unlock()
		s.stateMu.Unlock()
		releaseTerminal()
		remote.stop()
		return
	}
	// Serialize the remote-listener -> Socket-listener ownership transfer with
	// notifyClose. Either close wins before registration, or the permanent
	// Socket listener is published before the remote listener is relinquished.
	remote.handoffMu.Lock()
	if remote.closed.Load() || transport.ReadyState() != "open" || client.ReadyState() != "open" {
		remote.handoffMu.Unlock()
		client.markConnectionReady()
		client.packetMu.Unlock()
		s.stateMu.Unlock()
		releaseTerminal()
		client.Close(true)
		remote.stop()
		return
	}
	// OnClose takes this same lock before making the ready-state transition.
	// Keep it out until the live transport and client-map close listener are
	// installed, so a close cannot leave a resurrected takeover client behind.
	client.upgradeStateMu.Lock()
	if client.ReadyState() != "open" {
		client.upgradeStateMu.Unlock()
		remote.handoffMu.Unlock()
		client.markConnectionReady()
		client.packetMu.Unlock()
		s.stateMu.Unlock()
		releaseTerminal()
		client.Close(true)
		remote.stop()
		return
	}
	// The peer already received OPEN from the original owner. Since the
	// suppressed transport is non-writable, onOpen queued (but did not flush)
	// that handshake. Drop it before attaching the live WebSocket so no phantom
	// sent-callback batch can consume the next real transport drain.
	client.flushMu.Lock()
	client.writeBuffer.Clear()
	client.packetsFn.Clear()
	client.sentCallbackFn.Clear()
	client.flushMu.Unlock()
	// Rebind the permanent Socket listeners from the temporary suppressed
	// wrapper to the actual edge before relinquishing remote ownership. Besides
	// keeping close/error continuously owned, this makes drain callbacks receive
	// the real transport identity after takeover.
	client.cleanupTransportListeners()
	client.setTransport(transport)

	s.Clients().Store(sid, client)
	var implementationError error
	if base, ok := s.baseImplementation(); ok {
		base.clientsCount.Add(1)
	} else {
		// The embedded implementation is always *server; keep this branch for
		// custom Server implementations passed through future constructors.
		implementationError = errors.New("engine: unexpected cluster server implementation")
	}
	_ = client.Once("close", func(...any) {
		s.Clients().Delete(sid)
		if base, ok := s.baseImplementation(); ok {
			base.clientsCount.Add(^uint64(0))
		}
	})
	remote.handedOff.Store(client)
	remote.closed.Store(true)
	remote.removeListeners()
	s.deleteRemoteTransport(remote.sid, remote)
	// Clients now owns the live transport and Cleanup can discover it. Release
	// the transition while the handoff locks still exclude both close paths, and
	// before connection/upgrade/close listeners can run.
	releaseTerminal()
	remote.handoffMu.Unlock()
	client.upgradeStateMu.Unlock()
	s.stateMu.Unlock()
	s.reportClusterError(implementationError)
	client.beginConnectionAnnouncement()
	if s.closing.Load() || client.ReadyState() != "open" {
		client.finishConnectionAnnouncement()
		client.markConnectionReady()
		client.packetMu.Unlock()
		client.Close(true)
		return
	}

	s.Emit("connection", Socket(client))
	client.finishConnectionAnnouncement()
	if client.ReadyState() != "open" {
		client.markConnectionReady()
		client.packetMu.Unlock()
		return
	}
	upgradeAnnouncement := client.beginUpgradeAnnouncement()
	if client.ReadyState() != "open" {
		client.finishUpgradeAnnouncement(upgradeAnnouncement)
		client.markConnectionReady()
		client.packetMu.Unlock()
		return
	}
	client.Emit("upgrade")
	client.finishUpgradeAnnouncement(upgradeAnnouncement)
	for _, wirePacket := range buffered {
		client.onPacketLocked(clusterPacketToPacket(wirePacket))
	}
	client.markConnectionReady()
	client.packetMu.Unlock()
}

func (s *clusterServer) baseImplementation() (*baseServer, bool) {
	serverImplementation, ok := s.Server.(*server)
	if !ok {
		return nil, false
	}
	base, ok := serverImplementation.BaseServer.(*baseServer)
	return base, ok
}

func (s *clusterServer) emitConnection(client Socket) {
	socket, ok := client.(*socket)
	if ok && socket.ReadyState() != "open" {
		socket.markConnectionReady()
		return
	}
	if !ok || socket.Transport().Name() == transports.WEBSOCKET {
		if s.closing.Load() {
			client.Close(true)
			return
		}
		if ok {
			socket.beginConnectionAnnouncement()
			if socket.ReadyState() != "open" {
				socket.finishConnectionAnnouncement()
				return
			}
		}
		s.Emit("connection", client)
		if ok {
			socket.finishConnectionAnnouncement()
		}
		return
	}

	state := &delayedConnection{socket: socket}
	state.listener = func(values ...any) {
		packet, _ := values[0].(*enginepacket.Packet)
		wirePacket, err := packetToClusterPacket(packet)
		if err != nil {
			s.reportClusterError(err)
			return
		}
		s.delayedMu.Lock()
		if current := s.delayed[socket.Id()]; current == state {
			current.packets = append(current.packets, wirePacket)
		}
		s.delayedMu.Unlock()
	}
	s.delayedMu.Lock()
	if s.closing.Load() {
		s.delayedMu.Unlock()
		socket.Close(true)
		return
	}
	_ = socket.On("packet", state.listener)
	s.delayed[socket.Id()] = state
	state.timer = utils.SetTimeout(func() { s.doConnect(socket.Id(), socket) }, s.options.DelayedConnectionTimeout)
	s.delayedMu.Unlock()
}

func (s *clusterServer) doConnect(sid string, expected *socket) {
	expected.packetMu.Lock()
	defer expected.packetMu.Unlock()
	s.delayedMu.Lock()
	state := s.delayed[sid]
	if state == nil || state.socket != expected {
		s.delayedMu.Unlock()
		return
	}
	delete(s.delayed, sid)
	s.delayedMu.Unlock()

	utils.ClearTimeout(state.timer)
	state.socket.RemoveListener("packet", state.listener)
	state.socket.beginConnectionAnnouncement()
	if state.socket.ReadyState() != "open" {
		state.socket.finishConnectionAnnouncement()
		return
	}
	if s.closing.Load() {
		state.socket.finishConnectionAnnouncement()
		state.socket.Close(true)
		return
	}
	s.Emit("connection", Socket(state.socket))
	state.socket.finishConnectionAnnouncement()
	if state.socket.ReadyState() != "open" {
		return
	}
	for _, wirePacket := range state.packets {
		state.socket.onPacketLocked(clusterPacketToPacket(wirePacket))
	}
	if state.socket.Upgraded() {
		upgradeAnnouncement := state.socket.beginUpgradeAnnouncement()
		if state.socket.ReadyState() == "open" && state.socket.Upgraded() {
			state.socket.Emit("upgrade")
		}
		state.socket.finishUpgradeAnnouncement(upgradeAnnouncement)
	}
}

func (s *clusterServer) takeDelayedPackets(
	sid string,
	expected *socket,
	upgradeToken uint64,
	// packetLocked is true when a delayed socket was consumed. The caller keeps
	// packetMu through the terminal cluster response and the subsequent local
	// upgrade/close events, preserving the original replay ordering without
	// carrying a publication permit through user callbacks.
) (packets []ClusterPacket, takeOver, committed, packetLocked bool) {
	expected.packetMu.Lock()
	s.delayedMu.Lock()
	state := s.delayed[sid]
	if state == nil || state.socket != expected {
		s.delayedMu.Unlock()
		expected.packetMu.Unlock()
		return nil, false, false, false
	}
	delete(s.delayed, sid)
	packets = append([]ClusterPacket(nil), state.packets...)
	s.delayedMu.Unlock()

	utils.ClearTimeout(state.timer)
	state.socket.RemoveListener("packet", state.listener)
	if !state.socket.finishUpgrade(upgradeToken, true) {
		return packets, true, false, true
	}
	return packets, true, true, true
}

func packetToClusterPacket(source *enginepacket.Packet) (ClusterPacket, error) {
	if source == nil {
		return ClusterPacket{}, errors.New("engine: nil cluster packet")
	}
	result := ClusterPacket{Type: source.Type.String()}
	if source.Options != nil && source.Options.Compress != nil {
		result.HasCompress = true
		result.Compress = *source.Options.Compress
	}
	if source.Data == nil {
		return result, nil
	}
	result.HasData = true
	switch source.Data.(type) {
	case *types.StringBuffer, *strings.Reader:
		// These are the two text reader types recognized by the Engine.IO
		// parsers. Every other non-nil io.Reader is encoded as binary.
	default:
		result.Binary = true
	}
	clone := clonePacketForEvent(source)
	if clone == nil || clone.Data == nil {
		return result, nil
	}
	data, err := io.ReadAll(clone.Data)
	if err != nil {
		return ClusterPacket{}, fmt.Errorf("engine: read cluster packet: %w", err)
	}
	result.Data = data
	return result, nil
}

func clusterPacketToPacket(source ClusterPacket) *enginepacket.Packet {
	packet := &enginepacket.Packet{Type: enginepacket.Type(source.Type)}
	if source.HasData {
		if source.Binary {
			packet.Data = types.NewBytesBuffer(append([]byte(nil), source.Data...))
		} else {
			packet.Data = types.NewStringBuffer(append([]byte(nil), source.Data...))
		}
	}
	if source.HasCompress {
		compress := source.Compress
		packet.Options = &enginepacket.Options{Compress: &compress}
	}
	return packet
}

func clusterPacketsToPackets(source []ClusterPacket) []*enginepacket.Packet {
	packets := make([]*enginepacket.Packet, 0, len(source))
	for _, wirePacket := range source {
		packets = append(packets, clusterPacketToPacket(wirePacket))
	}
	return packets
}

func packetDataString(packet *enginepacket.Packet) string {
	if packet == nil || packet.Data == nil {
		return ""
	}
	clone := clonePacketForEvent(packet)
	if clone == nil || clone.Data == nil {
		return ""
	}
	data, _ := io.ReadAll(clone.Data)
	return string(data)
}

// Cleanup is invoked by BaseServer.Close after local clients have closed.
func (s *clusterServer) Cleanup() {
	s.beginClose()
	s.closeOnce.Do(func() {
		// BaseServer.Close took its first client snapshot before Cleanup. Wait for
		// any takeover that entered its short registration critical section, then
		// close the resulting late client as well.
		s.stateMu.Lock()
		lateClients := make([]Socket, 0)
		forwarders := make(map[*forwardingTransport]struct{})
		s.Clients().Range(func(_ string, client Socket) bool {
			lateClients = append(lateClients, client)
			if socket, ok := client.(*socket); ok {
				if forwarder, ok := socket.Transport().(*forwardingTransport); ok {
					forwarders[forwarder] = struct{}{}
				}
			}
			return true
		})
		orphanRequests := make([]remoteRequest, 0)
		s.remoteRequests.Range(func(key, value any) bool {
			if loaded, found := s.remoteRequests.LoadAndDelete(key); found {
				orphanRequests = append(orphanRequests, loaded.(remoteRequest))
			}
			return true
		})
		s.stateMu.Unlock()

		// All shutdown publications share one absolute budget. Each remote
		// session/request gets its own worker (and preserves its own message
		// order), so cleanup latency is O(ResponseTimeout), never O(session
		// count). Local transports are detached before any bus call.
		shutdownBudget := 2 * s.options.ResponseTimeout
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownBudget)
		defer cancelShutdown()
		s.beginTransportPublishShutdown(shutdownCtx)
		notificationBatches := make([][]*ClusterMessage, 0, len(orphanRequests))
		for _, request := range orphanRequests {
			if request.stopCleanup != nil {
				request.stopCleanup()
			}
			if request.expectsDrain {
				s.releaseEarlyDrain(request.sid)
			}
			if message := grantedLockReleaseMessage(
				request.ownerID,
				request.sid,
				request.transport,
				request.lock,
				"transport close",
			); message != nil {
				notificationBatches = append(notificationBatches, []*ClusterMessage{message})
			}
		}

		s.remoteMu.Lock()
		remotes := make([]*remoteTransport, 0, len(s.remoteTransports))
		for _, remote := range s.remoteTransports {
			remotes = append(remotes, remote)
		}
		clear(s.remoteTransports)
		s.remoteMu.Unlock()
		for _, remote := range remotes {
			batch := make([]*ClusterMessage, 0, 2)
			if remote.upgradePending.Load() {
				if message := grantedLockReleaseMessage(
					remote.ownerID,
					remote.sid,
					remote.transport.Name(),
					ClusterReadLock,
					"transport close",
				); message != nil {
					batch = append(batch, message)
				}
			}
			batch = append(batch, &ClusterMessage{
				RecipientID: remote.ownerID,
				Type:        ClusterMessageClose,
				SID:         remote.sid,
				Reason:      "transport close",
			})
			remote.stop()
			notificationBatches = append(notificationBatches, batch)
		}

		for _, client := range lateClients {
			client.Close(true)
		}

		var shutdownWorkers sync.WaitGroup
		for _, batch := range notificationBatches {
			shutdownWorkers.Go(func() {
				for _, message := range batch {
					if err := s.publishTransportMessageContext(shutdownCtx, message); err != nil {
						s.reportClusterError(err)
						return
					}
				}
			})
		}
		for forwarder := range forwarders {
			shutdownWorkers.Go(func() {
				select {
				case <-forwarder.done:
				case <-shutdownCtx.Done():
				}
			})
		}
		shutdownWorkers.Go(func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for s.activeTransportPublishes() > 0 {
				select {
				case <-shutdownCtx.Done():
					return
				case <-ticker.C:
				}
			}
		})
		shutdownDone := make(chan struct{})
		go func() {
			shutdownWorkers.Wait()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
		case <-shutdownCtx.Done():
		}
		if shutdownCtx.Err() != nil {
			s.cancelTransportPublishes()
			for forwarder := range forwarders {
				select {
				case <-forwarder.done:
				default:
					forwarder.forceStop(context.DeadlineExceeded)
				}
			}
			// A context-aware bus returns immediately after cancellation. Give
			// those goroutines a small bounded scheduling window to unregister;
			// a bus which ignores Context still cannot extend Cleanup by another
			// per-publication timeout.
			quiesceBudget := s.options.ResponseTimeout / 4
			if quiesceBudget <= 0 || quiesceBudget > 25*time.Millisecond {
				quiesceBudget = 25 * time.Millisecond
			}
			quiesceTimer := time.NewTimer(quiesceBudget)
			quiesceTicker := time.NewTicker(time.Millisecond)
		quiesce:
			for s.activeTransportPublishes() > 0 {
				select {
				case <-quiesceTimer.C:
					break quiesce
				case <-quiesceTicker.C:
				}
			}
			if !quiesceTimer.Stop() {
				select {
				case <-quiesceTimer.C:
				default:
				}
			}
			quiesceTicker.Stop()
		}

		s.probeMu.Lock()
		probes := make([]transports.Transport, 0, len(s.probes))
		for probe := range s.probes {
			probes = append(probes, probe)
		}
		clear(s.probes)
		s.probeMu.Unlock()
		for _, probe := range probes {
			probe.Discard()
			probe.Close()
		}

		s.upgradeMu.Lock()
		grants := make([]clusterUpgradeGrant, 0, len(s.upgradeGrants))
		for _, grant := range s.upgradeGrants {
			grant.socket.RemoveListener("close", grant.closeListener)
			grants = append(grants, grant)
		}
		for _, timer := range s.upgradeNoopTimers {
			utils.ClearTimeout(timer)
		}
		for _, timer := range s.upgradeLockTimers {
			utils.ClearTimeout(timer)
		}
		clear(s.upgradeNoopTimers)
		clear(s.upgradeLockTimers)
		clear(s.upgradeGrants)
		s.upgradeMu.Unlock()
		for _, grant := range grants {
			grant.socket.finishUpgrade(grant.token, false)
		}

		s.writeGrantMu.Lock()
		for _, timer := range s.writeGrants {
			utils.ClearTimeout(timer)
		}
		clear(s.writeGrants)
		s.writeGrantMu.Unlock()

		s.readGrantMu.Lock()
		for _, timer := range s.readGrants {
			utils.ClearTimeout(timer)
		}
		clear(s.readGrants)
		s.readGrantMu.Unlock()

		s.delayedMu.Lock()
		for _, state := range s.delayed {
			utils.ClearTimeout(state.timer)
			state.socket.RemoveListener("packet", state.listener)
		}
		clear(s.delayed)
		s.delayedMu.Unlock()

		// Keep the subscription alive for the remainder of the shared shutdown
		// interval so a lock
		// response racing request/server cancellation can still be matched and its
		// owner grant explicitly released.
	requestGrace:
		for s.PendingClusterRequests() > 0 {
			select {
			case <-shutdownCtx.Done():
				break requestGrace
			case <-time.After(time.Millisecond):
			}
		}
		s.requestsMu.Lock()
		clear(s.requests)
		s.requestsMu.Unlock()
		if s.unsubscribe != nil {
			s.unsubscribe()
		}

		s.earlyDrainMu.Lock()
		clear(s.expectedDrains)
		clear(s.earlyDrains)
		s.earlyDrainMu.Unlock()
	})
}
