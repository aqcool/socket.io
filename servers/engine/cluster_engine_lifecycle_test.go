package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/gorilla/websocket"
)

type hookedClusterBus struct {
	base ClusterBus
	hook func(context.Context, *ClusterMessage, func() error) error
}

type stalledCleanupClusterBus struct {
	publishes atomic.Int64
	active    atomic.Int64
}

type gatedCleanupClusterBus struct {
	stalledCleanupClusterBus
	reached atomic.Int64
	gate    chan struct{}
}

func (b *gatedCleanupClusterBus) Publish(ctx context.Context, message *ClusterMessage) error {
	b.publishes.Add(1)
	b.reached.Add(1)
	select {
	case <-b.gate:
		return b.stalledCleanupClusterBus.Publish(ctx, message)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*stalledCleanupClusterBus) Subscribe(string, func(*ClusterMessage)) (func(), error) {
	return func() {}, nil
}

func (b *stalledCleanupClusterBus) Publish(ctx context.Context, _ *ClusterMessage) error {
	b.publishes.Add(1)
	b.active.Add(1)
	defer b.active.Add(-1)
	<-ctx.Done()
	return ctx.Err()
}

type lifecycleTestTransport struct {
	transports.Transport
	name string
}

type snapshotLifecycleTransport struct {
	*lifecycleTestTransport
	snapshot chan struct{}
	release  chan struct{}
	once     sync.Once
}

type blockingSendLifecycleTransport struct {
	*lifecycleTestTransport
	sendStarted chan struct{}
	releaseSend chan struct{}
	startOnce   sync.Once
	packetsMu   sync.Mutex
	packets     []*enginepacket.Packet
}

func (t *blockingSendLifecycleTransport) Send(packets []*enginepacket.Packet) {
	t.SetWritable(false)
	t.packetsMu.Lock()
	t.packets = append(t.packets, packets...)
	t.packetsMu.Unlock()
	t.startOnce.Do(func() { close(t.sendStarted) })
	<-t.releaseSend
}

func (t *snapshotLifecycleTransport) Emit(event types.EventName, values ...any) {
	if event != "close" {
		t.Transport.Emit(event, values...)
		return
	}
	listeners := t.Listeners(event)
	blocked := false
	t.once.Do(func() {
		blocked = true
		close(t.snapshot)
	})
	if blocked {
		<-t.release
	}
	for _, listener := range listeners {
		listener(values...)
	}
}

func (t *lifecycleTestTransport) Name() string { return t.name }

func (t *lifecycleTestTransport) Close(callbacks ...types.Callable) {
	if t.ReadyState() == "closed" {
		return
	}
	t.SetReadyState("closed")
	t.Emit("close")
	for _, callback := range callbacks {
		if callback != nil {
			callback()
		}
	}
}

func (b *hookedClusterBus) Subscribe(nodeID string, listener func(*ClusterMessage)) (func(), error) {
	return b.base.Subscribe(nodeID, listener)
}

func (b *hookedClusterBus) Publish(ctx context.Context, message *ClusterMessage) error {
	next := func() error { return b.base.Publish(ctx, message) }
	if b.hook == nil {
		return next()
	}
	return b.hook(ctx, message, next)
}

func lifecycleClusterOwner(
	t *testing.T,
	bus ClusterBus,
	options *config.ServerOptions,
	clusterOptions *ClusterOptions,
) (*clusterServer, *httptest.Server, string) {
	t.Helper()
	server, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	sid := clusterHandshake(t, httpServer.URL)
	t.Cleanup(func() {
		server.Close()
		httpServer.CloseClientConnections()
		httpServer.Close()
	})
	return server.(*clusterServer), httpServer, sid
}

func clusterSocketForSID(t *testing.T, server *clusterServer, sid string) *socket {
	t.Helper()
	client, found := server.Clients().Load(sid)
	if !found {
		t.Fatalf("owner does not contain SID %s", sid)
	}
	socket, ok := client.(*socket)
	if !ok {
		t.Fatalf("client %s has type %T", sid, client)
	}
	return socket
}

func TestClusterUpgradeGrantLeaseStartsAfterPublish(t *testing.T) {
	memory := NewMemoryClusterBus()
	responseStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	var responseOnce sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type != ClusterMessageAcquireLockResponse || message.Transport != "" {
			return next()
		}
		err := next() // deliver first, but do not let the owner commit the publish yet
		responseOnce.Do(func() { close(responseStarted) })
		select {
		case <-releaseResponse:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	options := config.DefaultServerOptions()
	options.SetUpgradeTimeout(80 * time.Millisecond)
	owner, _, sid := lifecycleClusterOwner(t, bus, options, &ClusterOptions{
		ResponseTimeout:     500 * time.Millisecond,
		NoopUpgradeInterval: 200 * time.Millisecond,
	})
	edgeServer, err := NewClusterServer(bus, options, &ClusterOptions{ResponseTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	edge := edgeServer.(*clusterServer)
	t.Cleanup(func() {
		edge.Close()
		_ = memory.Close()
	})

	type result struct {
		ownerID string
		success bool
	}
	resultCh := make(chan result, 1)
	go func() {
		ownerID, success := edge.acquireLock(context.Background(), sid, transports.WEBSOCKET, ClusterReadLock)
		resultCh <- result{ownerID: ownerID, success: success}
	}()
	select {
	case <-responseStarted:
	case <-time.After(time.Second):
		t.Fatal("upgrade lock response was not delivered")
	}

	socket := clusterSocketForSID(t, owner, sid)
	time.Sleep(2 * options.UpgradeTimeout())
	owner.upgradeMu.Lock()
	grant, grantFound := owner.upgradeGrants[sid]
	_, noopArmed := owner.upgradeNoopTimers[sid]
	_, leaseArmed := owner.upgradeLockTimers[sid]
	owner.upgradeMu.Unlock()
	if !grantFound || grant.senderID != edge.NodeID() || !socket.hasActiveUpgrade(grant.token) {
		t.Fatal("slow response publish lost its pending upgrade generation")
	}
	if noopArmed || leaseArmed {
		t.Fatal("upgrade lease started before response Publish completed")
	}

	close(releaseResponse)
	select {
	case got := <-resultCh:
		if !got.success || got.ownerID != owner.NodeID() {
			t.Fatalf("acquire result = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("upgrade acquisition did not finish")
	}
	waitClusterCondition(t, func() bool {
		owner.upgradeMu.Lock()
		defer owner.upgradeMu.Unlock()
		return owner.upgradeNoopTimers[sid] != nil && owner.upgradeLockTimers[sid] != nil
	})
	owner.onRemoteUpgrade(&ClusterMessage{SID: sid, SenderID: edge.NodeID(), Success: false})
	if socket.Upgrading() {
		t.Fatal("failed upgrade did not release active generation")
	}
}

func TestClusterUpgradeGrantGenerationAndListenerIsolation(t *testing.T) {
	memory := NewMemoryClusterBus()
	options := config.DefaultServerOptions()
	owner, _, sid := lifecycleClusterOwner(t, memory, options, &ClusterOptions{ResponseTimeout: 100 * time.Millisecond})
	socket := clusterSocketForSID(t, owner, sid)
	baselineCloseListeners := socket.ListenerCount("close")

	for generation := range 20 {
		token, ok := socket.beginUpgrade()
		if !ok {
			t.Fatalf("begin generation %d failed", generation)
		}
		sender := fmt.Sprintf("edge-%d", generation)
		owner.registerUpgradeAttempt(socket, sender, token)
		owner.armUpgradeAttempt(sid, sender, socket, token)
		if !owner.clearUpgradeAttempt(sid, sender, socket, token) {
			t.Fatalf("clear generation %d failed", generation)
		}
		if got := socket.ListenerCount("close"); got != baselineCloseListeners {
			t.Fatalf("close listener count after generation %d = %d, want %d", generation, got, baselineCloseListeners)
		}
	}

	oldToken, ok := socket.beginUpgrade()
	if !ok {
		t.Fatal("begin old generation failed")
	}
	owner.registerUpgradeAttempt(socket, "old-edge", oldToken)
	owner.armUpgradeAttempt(sid, "old-edge", socket, oldToken)
	owner.clearUpgradeAttempt(sid, "old-edge", socket, oldToken)
	newToken, ok := socket.beginUpgrade()
	if !ok {
		t.Fatal("begin new generation failed")
	}
	owner.registerUpgradeAttempt(socket, "new-edge", newToken)
	owner.armUpgradeAttempt(sid, "new-edge", socket, newToken)
	if owner.clearUpgradeAttempt(sid, "old-edge", socket, oldToken) {
		t.Fatal("stale generation cleared the current grant")
	}
	if token, consumed := owner.consumeUpgradeGrant(sid, "new-edge", socket); !consumed || token != newToken {
		t.Fatalf("current generation consume = token %d, %t", token, consumed)
	}
	socket.finishUpgrade(newToken, false)
	_ = memory.Close()
}

func TestClusterPollingDrainPrecedesPersistentUpgrade(t *testing.T) {
	memory := NewMemoryClusterBus()
	drainStarted := make(chan struct{})
	releaseDrain := make(chan struct{})
	var gateOnce sync.Once
	var upgradeResponseSeen atomic.Bool
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageUpgradeResponse {
			upgradeResponseSeen.Store(true)
		}
		if message.Type == ClusterMessageDrain {
			blocked := false
			gateOnce.Do(func() {
				blocked = true
				close(drainStarted)
			})
			if blocked {
				select {
				case <-releaseDrain:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return next()
	}

	options := config.DefaultServerOptions()
	owner, _, sid := lifecycleClusterOwner(t, bus, options, &ClusterOptions{ResponseTimeout: time.Second})
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)
	physical := socket.Transport()
	oneShot := newForwardingTransport(owner, socket, physical, "poll-edge", true)
	socket.flushMu.Lock()
	expected := socket.transport.Load()
	if !socket.reserveClusterTransportCAS(oneShot, expected) {
		socket.flushMu.Unlock()
		t.Fatal("could not install one-shot forwarder")
	}
	socket.flushMu.Unlock()
	oneShot.Send([]*enginepacket.Packet{{Type: enginepacket.MESSAGE, Data: strings.NewReader("before-upgrade")}})
	select {
	case <-drainStarted:
	case <-time.After(time.Second):
		t.Fatal("one-shot DRAIN did not reach barrier")
	}

	upgradeToken, ok := socket.beginUpgrade()
	if !ok {
		t.Fatal("could not begin remote upgrade generation")
	}
	owner.registerUpgradeAttempt(socket, "ws-edge", upgradeToken)
	owner.armUpgradeAttempt(sid, "ws-edge", socket, upgradeToken)
	upgradeDone := make(chan struct{})
	go func() {
		owner.onRemoteUpgrade(&ClusterMessage{
			SID:       sid,
			SenderID:  "ws-edge",
			RequestID: 91,
			Success:   true,
		})
		close(upgradeDone)
	}()
	select {
	case <-upgradeDone:
		t.Fatal("persistent forwarder overtook the polling DRAIN")
	case <-time.After(30 * time.Millisecond):
	}
	if !socket.Upgrading() || socket.Upgraded() {
		t.Fatalf("upgrade state at DRAIN barrier = upgrading %t upgraded %t", socket.Upgrading(), socket.Upgraded())
	}
	if upgradeResponseSeen.Load() {
		t.Fatal("UPGRADE_RESPONSE overtook the polling DRAIN")
	}
	if socket.Transport() != oneShot {
		t.Fatalf("transport at barrier = %T, want original one-shot", socket.Transport())
	}
	close(releaseDrain)
	select {
	case <-upgradeDone:
	case <-time.After(time.Second):
		t.Fatal("persistent install did not finish after DRAIN")
	}
	persistent, ok := socket.Transport().(*forwardingTransport)
	if !ok || persistent.oneShot || persistent.targetID != "ws-edge" {
		t.Fatalf("current transport = %T, want persistent ws-edge forwarder", socket.Transport())
	}
	if socket.Transport() != persistent {
		t.Fatalf("current transport = %T, want persistent forwarder", socket.Transport())
	}
	if _, nested := persistent.Transport.(*forwardingTransport); nested {
		t.Fatal("persistent forwarder wraps the completed one-shot forwarder")
	}
	if persistent.previous == oneShot.slot {
		t.Fatal("persistent forwarder would restore the completed one-shot slot")
	}
	if !socket.Upgraded() || socket.Upgrading() || !upgradeResponseSeen.Load() {
		t.Fatalf("final upgrade state = upgrading %t upgraded %t response %t", socket.Upgrading(), socket.Upgraded(), upgradeResponseSeen.Load())
	}
	owner.onRemoteClose(&ClusterMessage{SID: sid, SenderID: "poll-edge", Reason: "late close"})
	if socket.ReadyState() != "open" || socket.Transport() != persistent {
		t.Fatal("stale polling CLOSE killed the newer active upgrade edge")
	}
	persistent.deactivateAndRestore()
	_ = memory.Close()
}

func TestClusterPollingNextReadWaitsForDeliveredDrainPublish(t *testing.T) {
	t.Run("remote owner lookup", func(t *testing.T) {
		testClusterPollingNextReadWaitsForDeliveredDrainPublish(t, false, false)
	})
	t.Run("local owner request", func(t *testing.T) {
		testClusterPollingNextReadWaitsForDeliveredDrainPublish(t, true, false)
	})
	t.Run("local owner shutdown", func(t *testing.T) {
		testClusterPollingNextReadWaitsForDeliveredDrainPublish(t, true, true)
	})
}

func testClusterPollingNextReadWaitsForDeliveredDrainPublish(
	t *testing.T,
	nextReadOnOwner bool,
	shutdownBeforeRelease bool,
) {
	memory := NewMemoryClusterBus()
	drainDelivered := make(chan struct{})
	releasePublish := make(chan struct{})
	var drainGate, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releasePublish) }) }
	t.Cleanup(release)

	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type != ClusterMessageDrain {
			return next()
		}
		blocked := false
		drainGate.Do(func() { blocked = true })
		if !blocked {
			return next()
		}
		err := next()
		close(drainDelivered)
		select {
		case <-releasePublish:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{ResponseTimeout: time.Second}
	owner, ownerHTTP, sid := lifecycleClusterOwner(t, bus, options, clusterOptions)
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edge := edgeServer.(*clusterServer)
	edgeHTTP := httptest.NewServer(edge)
	t.Cleanup(func() {
		edge.Close()
		edgeHTTP.CloseClientConnections()
		edgeHTTP.Close()
		_ = memory.Close()
	})

	type pollResult struct {
		status int
		body   string
		err    error
	}
	poll := func(result chan<- pollResult) {
		request, requestErr := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			clusterPollingURL(edgeHTTP.URL, sid),
			nil,
		)
		if requestErr != nil {
			result <- pollResult{err: requestErr}
			return
		}
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			result <- pollResult{err: requestErr}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		result <- pollResult{status: response.StatusCode, body: string(body), err: readErr}
	}

	ownerSocket := clusterSocketForSID(t, owner, sid)
	firstResult := make(chan pollResult, 1)
	go poll(firstResult)
	var firstForwarder *forwardingTransport
	waitClusterCondition(t, func() bool {
		current, ok := ownerSocket.Transport().(*forwardingTransport)
		if ok && current.oneShot {
			firstForwarder = current
		}
		return ok && current.oneShot
	})
	ownerSocket.Send(strings.NewReader("first"), nil, nil)
	select {
	case <-drainDelivered:
	case <-time.After(time.Second):
		t.Fatal("first DRAIN was not delivered while Publish was blocked")
	}
	select {
	case result := <-firstResult:
		if result.err != nil || result.status != http.StatusOK || result.body != "4first" {
			t.Fatalf("first poll = status %d body %q error %v", result.status, result.body, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivered DRAIN did not complete the first polling GET")
	}

	if shutdownBeforeRelease {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(
			http.MethodGet,
			clusterPollingURL(ownerHTTP.URL, sid),
			nil,
		)
		requestContext := types.NewHttpContext(recorder, request)
		requestDone := make(chan struct{})

		// Hold sendMu until onRequest owns requestMu. This deterministically puts
		// the GET into the releasePending wait instead of relying on scheduler timing.
		firstForwarder.sendMu.Lock()
		go func() {
			ownerSocket.onRequest(requestContext)
			close(requestDone)
		}()
		entered := false
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if ownerSocket.requestMu.TryLock() {
				ownerSocket.requestMu.Unlock()
				time.Sleep(time.Millisecond)
				continue
			}
			entered = true
			break
		}
		firstForwarder.sendMu.Unlock()
		if !entered {
			t.Fatal("local polling GET did not enter the release-pending check")
		}
		waitClusterCondition(t, func() bool {
			if !ownerSocket.requestMu.TryLock() {
				return false
			}
			ownerSocket.requestMu.Unlock()
			return true
		})

		owner.beginClose()
		select {
		case <-requestDone:
		case <-time.After(time.Second):
			t.Fatal("owner shutdown left the waiting polling GET unfinished")
		}
		if recorder.Code != http.StatusBadRequest || recorder.Body.Len() != 0 || !requestContext.IsDone() {
			t.Fatalf(
				"shutdown poll = status %d body %q done %t",
				recorder.Code,
				recorder.Body.String(),
				requestContext.IsDone(),
			)
		}
		release()
		return
	}

	secondResult := make(chan pollResult, 1)
	secondURL := edgeHTTP.URL
	if nextReadOnOwner {
		secondURL = ownerHTTP.URL
	}
	pollNext := func(result chan<- pollResult) {
		request, requestErr := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			clusterPollingURL(secondURL, sid),
			nil,
		)
		if requestErr != nil {
			result <- pollResult{err: requestErr}
			return
		}
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			result <- pollResult{err: requestErr}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		result <- pollResult{status: response.StatusCode, body: string(body), err: readErr}
	}
	go pollNext(secondResult)
	select {
	case result := <-secondResult:
		t.Fatalf("next polling GET completed before owner restoration: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}

	release()
	waitClusterCondition(t, func() bool {
		current := ownerSocket.Transport()
		return current != nil && current != firstForwarder && current.Writable()
	})
	ownerSocket.Send(strings.NewReader("second"), nil, nil)
	select {
	case result := <-secondResult:
		if result.err != nil || result.status != http.StatusOK || result.body != "4second" {
			t.Fatalf("second poll = status %d body %q error %v", result.status, result.body, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("next polling GET did not resume after owner restoration")
	}
}

func TestClusterCleanupWaitsForForwarderClosePublication(t *testing.T) {
	memory := NewMemoryClusterBus()
	firstDrainStarted := make(chan struct{})
	releaseFirstDrain := make(chan struct{})
	var firstDrain sync.Once
	var messagesMu sync.Mutex
	var messages []ClusterMessage
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageDrain {
			messagesMu.Lock()
			messages = append(messages, cloneClusterMessage(message))
			messagesMu.Unlock()
			blocked := false
			firstDrain.Do(func() {
				blocked = true
				close(firstDrainStarted)
			})
			if blocked {
				select {
				case <-releaseFirstDrain:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return next()
	}

	options := config.DefaultServerOptions()
	owner, httpServer, sid := lifecycleClusterOwner(t, bus, options, &ClusterOptions{ResponseTimeout: time.Second})
	socket := clusterSocketForSID(t, owner, sid)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("persistent forwarder install failed")
	}
	socket.Send(strings.NewReader("payload"), nil, nil)
	select {
	case <-firstDrainStarted:
	case <-time.After(time.Second):
		t.Fatal("data DRAIN did not reach barrier")
	}

	closed := make(chan struct{})
	go func() {
		owner.Close()
		close(closed)
	}()
	waitClusterCondition(t, func() bool { return forwarder.closing.Load() })
	select {
	case <-closed:
		t.Fatal("ClusterServer.Close returned before queued DRAIN/CLOSE publication")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFirstDrain)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("ClusterServer.Close did not finish after publication barrier")
	}
	select {
	case <-forwarder.done:
	default:
		t.Fatal("forwarder worker was not done when ClusterServer.Close returned")
	}

	messagesMu.Lock()
	got := append([]ClusterMessage(nil), messages...)
	messagesMu.Unlock()
	if len(got) < 2 || len(got[0].Packets) != 1 || got[0].Packets[0].Type != enginepacket.MESSAGE.String() {
		t.Fatalf("first forwarded batch = %+v", got)
	}
	last := got[len(got)-1]
	if len(last.Packets) != 1 || last.Packets[0].Type != enginepacket.CLOSE.String() {
		t.Fatalf("last forwarded batch = %+v", last)
	}
	httpServer.CloseClientConnections()
	_ = memory.Close()
}

func TestClusterCleanupPublishesQueuedCloseAfterDrainFailure(t *testing.T) {
	memory := NewMemoryClusterBus()
	dataDrainStarted := make(chan struct{})
	failDataDrain := make(chan struct{})
	closePublished := make(chan struct{})
	publishFailure := errors.New("forced data DRAIN failure")
	var dataOnce, closeOnce sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageDrain && len(message.Packets) == 1 &&
			message.Packets[0].Type == enginepacket.MESSAGE.String() {
			blocked := false
			dataOnce.Do(func() {
				blocked = true
				close(dataDrainStarted)
			})
			if blocked {
				select {
				case <-failDataDrain:
					return publishFailure
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if message.Type == ClusterMessageClose {
			closeOnce.Do(func() { close(closePublished) })
		}
		return next()
	}

	const responseTimeout = 200 * time.Millisecond
	owner, _, sid := lifecycleClusterOwner(
		t,
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: responseTimeout},
	)
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("persistent forwarder install failed")
	}
	socket.Send(strings.NewReader("payload"), nil, nil)
	select {
	case <-dataDrainStarted:
	case <-time.After(time.Second):
		t.Fatal("data DRAIN did not reach failure barrier")
	}

	closed := make(chan struct{})
	go func() {
		owner.Close()
		close(closed)
	}()
	waitClusterCondition(t, func() bool {
		forwarder.sendMu.Lock()
		defer forwarder.sendMu.Unlock()
		for _, batch := range forwarder.queue {
			if batch.close && batch.permit != nil {
				return true
			}
		}
		return false
	})
	failedAt := time.Now()
	close(failDataDrain)
	select {
	case <-closePublished:
	case <-time.After(responseTimeout):
		t.Fatal("queued shutdown CLOSE was not published after the data DRAIN failed")
	}
	select {
	case <-closed:
	case <-time.After(responseTimeout):
		t.Fatal("Cleanup waited for its deadline after publishing the queued CLOSE")
	}
	if elapsed := time.Since(failedAt); elapsed >= responseTimeout {
		t.Fatalf("queued CLOSE recovery took %s, want less than one response timeout", elapsed)
	}
	select {
	case <-forwarder.done:
	default:
		t.Fatal("forwarder did not finish after the queued CLOSE publication")
	}
	_ = memory.Close()
}

func TestClusterReservedPublicationTimeoutStartsAtPublish(t *testing.T) {
	memory := NewMemoryClusterBus()
	const responseTimeout = 25 * time.Millisecond
	server, err := NewClusterServer(
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: responseTimeout},
	)
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	received := make(chan ClusterMessage, 1)
	unsubscribe, err := memory.Subscribe("late-edge", func(message *ClusterMessage) {
		if message.Type == ClusterMessageClose {
			received <- cloneClusterMessage(message)
		}
	})
	if err != nil {
		cluster.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unsubscribe()
		cluster.Close()
		_ = memory.Close()
	})

	permit, err := cluster.reserveTimedTransportPublish(false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * responseTimeout)
	if err := permit.publish(&ClusterMessage{
		RecipientID: "late-edge",
		Type:        ClusterMessageClose,
		SID:         "late-publication",
		Reason:      "transport close",
	}); err != nil {
		permit.release()
		t.Fatalf("publication reserved before timeout failed when it began later: %v", err)
	}
	permit.release()
	select {
	case message := <-received:
		if message.SID != "late-publication" {
			t.Fatalf("late publication SID = %q", message.SID)
		}
	case <-time.After(time.Second):
		t.Fatal("late reserved publication was not delivered")
	}
	if active := cluster.activeTransportPublishes(); active != 0 {
		t.Fatalf("tracked publications after release = %d", active)
	}
}

func TestClusterAcquireTimeoutLateSuccessPublishesRelease(t *testing.T) {
	memory := NewMemoryClusterBus()
	requestSeen := make(chan ClusterMessage, 1)
	releaseSeen := make(chan ClusterMessage, 1)
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		switch message.Type {
		case ClusterMessageAcquireLock:
			select {
			case requestSeen <- cloneClusterMessage(message):
			default:
			}
		case ClusterMessageClose:
			select {
			case releaseSeen <- cloneClusterMessage(message):
			default:
			}
		}
		return next()
	}
	const responseTimeout = 60 * time.Millisecond
	server, err := NewClusterServer(
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: responseTimeout},
	)
	if err != nil {
		t.Fatal(err)
	}
	requester := server.(*clusterServer)
	t.Cleanup(func() {
		requester.Close()
		_ = memory.Close()
	})

	type acquireResult struct {
		ownerID string
		success bool
	}
	result := make(chan acquireResult, 1)
	go func() {
		ownerID, success := requester.acquireLock(
			context.Background(),
			"01234567890123456789",
			transports.POLLING,
			ClusterReadLock,
		)
		result <- acquireResult{ownerID: ownerID, success: success}
	}()
	var acquire ClusterMessage
	select {
	case acquire = <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("ACQUIRE_LOCK was not published")
	}
	select {
	case got := <-result:
		if got.success || got.ownerID != "" {
			t.Fatalf("timed-out acquire result = %+v", got)
		}
	case <-time.After(3 * responseTimeout):
		t.Fatal("ACQUIRE_LOCK did not time out")
	}

	if err := memory.Publish(context.Background(), &ClusterMessage{
		Source:      clusterMessageSource,
		SenderID:    "late-owner",
		RecipientID: requester.NodeID(),
		RequestID:   acquire.RequestID,
		Type:        ClusterMessageAcquireLockResponse,
		Success:     true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case release := <-releaseSeen:
		if release.RecipientID != "late-owner" || release.SID != "01234567890123456789" {
			t.Fatalf("late grant release = %+v", release)
		}
	case <-time.After(responseTimeout):
		t.Fatal("late successful grant was not explicitly released")
	}
	waitClusterCondition(t, func() bool {
		return requester.PendingClusterRequests() == 0 && requester.activeTransportPublishes() == 0
	})
}

func TestClusterTerminalPublishErrorAllowsReentrantClose(t *testing.T) {
	memory := NewMemoryClusterBus()
	publishFailure := errors.New("forced terminal publication failure")
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageClose {
			return publishFailure
		}
		return next()
	}
	const responseTimeout = 100 * time.Millisecond
	server, err := NewClusterServer(
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: responseTimeout},
	)
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	t.Cleanup(func() {
		cluster.Close()
		_ = memory.Close()
	})

	listenerReturned := make(chan struct{})
	var listenerOnce sync.Once
	_ = cluster.On("cluster_error", func(values ...any) {
		if clusterErr, _ := values[0].(error); !errors.Is(clusterErr, publishFailure) {
			return
		}
		cluster.Close()
		listenerOnce.Do(func() { close(listenerReturned) })
	})
	permit, err := cluster.reserveTimedTransportPublish(false)
	if err != nil {
		t.Fatal(err)
	}
	callReturned := make(chan struct{})
	go func() {
		cluster.releaseRemoteRequestWithPermit(&remoteRequest{
			sid:       "reentrant-terminal",
			ownerID:   "owner",
			transport: transports.POLLING,
			lock:      ClusterReadLock,
		}, "transport close", permit)
		close(callReturned)
	}()
	select {
	case <-listenerReturned:
	case <-time.After(responseTimeout):
		t.Fatal("cluster_error listener deadlocked while re-entering Close")
	}
	select {
	case <-callReturned:
	case <-time.After(responseTimeout):
		t.Fatal("terminal failure path did not return after reentrant Close")
	}
	if active := cluster.activeTransportPublishes(); active != 0 {
		t.Fatalf("terminal permit remained active during cluster_error = %d", active)
	}
}

func TestClusterSocketCloseListenerCanReenterServerClose(t *testing.T) {
	memory := NewMemoryClusterBus()
	server, err := NewClusterServer(
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: 100 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	t.Cleanup(func() {
		cluster.Close()
		_ = memory.Close()
	})

	transport := &lifecycleTestTransport{
		Transport: transports.MakeTransport(),
		name:      transports.WEBSOCKET,
	}
	client := makeSocket()
	client.server = cluster
	client.id = "reentrant-close"
	client.protocol = 4
	client.setTransport(transport)
	client.readyState.Store("open")
	client.markConnectionReady()
	cluster.Clients().Store(client.id, client)
	reentered := make(chan struct{})
	_ = client.Once("close", func(...any) {
		cluster.Close()
		close(reentered)
	})

	closed := make(chan struct{})
	go func() {
		cluster.Close()
		close(closed)
	}()
	select {
	case <-reentered:
	case <-time.After(time.Second):
		t.Fatal("socket close listener could not re-enter ClusterServer.Close")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("outer ClusterServer.Close deadlocked after listener re-entry")
	}
}

func TestClusterCleanupForceStopCloseListenerCanReenterServerClose(t *testing.T) {
	const responseTimeout = 40 * time.Millisecond
	bus := &stalledCleanupClusterBus{}
	owner, _, sid := lifecycleClusterOwner(
		t,
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{
			ResponseTimeout:          responseTimeout,
			DelayedConnectionTimeout: time.Hour,
		},
	)
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "stalled-edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("persistent forwarder install failed")
	}
	reentered := make(chan struct{})
	_ = socket.Once("close", func(...any) {
		owner.Close()
		close(reentered)
	})

	closed := make(chan struct{})
	go func() {
		owner.Close()
		close(closed)
	}()
	select {
	case <-reentered:
	case <-time.After(5 * responseTimeout):
		t.Fatal("forceStop close listener deadlocked while re-entering ClusterServer.Close")
	}
	select {
	case <-closed:
	case <-time.After(5 * responseTimeout):
		t.Fatal("Cleanup did not return after forceStop close listener re-entry")
	}
	select {
	case <-forwarder.done:
	default:
		t.Fatal("force-stopped forwarder was not done when Cleanup returned")
	}
	if active := owner.activeTransportPublishes(); active != 0 {
		t.Fatalf("active publications after forceStop Cleanup = %d", active)
	}
}

func TestClusterUpgradeCallbacksRunAfterTerminalPermitRelease(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		name := "persistent-forwarder"
		if delayed {
			name = "delayed-takeover"
		}
		t.Run(name, func(t *testing.T) {
			memory := NewMemoryClusterBus()
			const responseTimeout = 150 * time.Millisecond
			owner, _, sid := lifecycleClusterOwner(
				t,
				memory,
				config.DefaultServerOptions(),
				&ClusterOptions{
					ResponseTimeout:          responseTimeout,
					DelayedConnectionTimeout: time.Hour,
				},
			)
			socket := clusterSocketForSID(t, owner, sid)
			if !delayed {
				owner.doConnect(sid, socket)
			}
			upgradeToken, ok := socket.beginUpgrade()
			if !ok {
				t.Fatal("could not begin remote upgrade")
			}
			owner.registerUpgradeAttempt(socket, "edge", upgradeToken)
			owner.armUpgradeAttempt(sid, "edge", socket, upgradeToken)

			callbackResult := make(chan error, 1)
			_ = socket.Once("upgrade", func(...any) {
				if active := owner.activeTransportPublishes(); active != 0 {
					callbackResult <- fmt.Errorf("active terminal publications in upgrade callback = %d", active)
					return
				}
				started := time.Now()
				owner.Close()
				if elapsed := time.Since(started); elapsed >= responseTimeout {
					callbackResult <- fmt.Errorf("reentrant Close took %s", elapsed)
					return
				}
				callbackResult <- nil
			})
			upgradeReturned := make(chan struct{})
			go func() {
				owner.onRemoteUpgrade(&ClusterMessage{
					SID:       sid,
					SenderID:  "edge",
					RequestID: 17,
					Success:   true,
				})
				close(upgradeReturned)
			}()
			select {
			case callbackErr := <-callbackResult:
				if callbackErr != nil {
					t.Fatal(callbackErr)
				}
			case <-time.After(time.Second):
				t.Fatal("upgrade callback deadlocked while closing its server")
			}
			select {
			case <-upgradeReturned:
			case <-time.After(time.Second):
				t.Fatal("remote upgrade did not return after callback closed the server")
			}
			_ = memory.Close()
		})
	}
}

func TestClusterTakeoverCallbacksRunAfterTerminalPermitRelease(t *testing.T) {
	for _, callbackEvent := range []string{"connection", "upgrade"} {
		t.Run(callbackEvent, func(t *testing.T) {
			memory := NewMemoryClusterBus()
			const responseTimeout = 150 * time.Millisecond
			server, err := NewClusterServer(
				memory,
				config.DefaultServerOptions(),
				&ClusterOptions{ResponseTimeout: responseTimeout},
			)
			if err != nil {
				t.Fatal(err)
			}
			cluster := server.(*clusterServer)
			t.Cleanup(func() {
				cluster.Close()
				_ = memory.Close()
			})

			transport := &lifecycleTestTransport{
				Transport: transports.MakeTransport(),
				name:      transports.WEBSOCKET,
			}
			sid := "01234567890123456789"
			remote := cluster.hookRemoteTransport(sid, "owner", transport, true)
			if !cluster.storeRemoteTransport(remote) {
				t.Fatal("could not store takeover remote")
			}
			permit, err := cluster.reserveTimedTransportPublish(false)
			if err != nil {
				t.Fatal(err)
			}
			callbackResult := make(chan error, 1)
			closeFromCallback := func() {
				if active := cluster.activeTransportPublishes(); active != 0 {
					callbackResult <- fmt.Errorf("active terminal publications in %s callback = %d", callbackEvent, active)
					return
				}
				started := time.Now()
				cluster.Close()
				if elapsed := time.Since(started); elapsed >= responseTimeout {
					callbackResult <- fmt.Errorf("reentrant Close took %s", elapsed)
					return
				}
				callbackResult <- nil
			}
			_ = cluster.Once("connection", func(values ...any) {
				client, _ := values[0].(*socket)
				if callbackEvent == "connection" {
					closeFromCallback()
					return
				}
				if client == nil {
					callbackResult <- errors.New("takeover connection did not provide a socket")
					return
				}
				_ = client.Once("upgrade", func(...any) { closeFromCallback() })
			})
			request := httptest.NewRequest(
				http.MethodGet,
				"/engine.io/?EIO=4&transport=websocket&sid="+sid,
				nil,
			)
			request.RemoteAddr = "127.0.0.1:12345"
			httpContext := types.NewHttpContext(httptest.NewRecorder(), request)
			takeoverReturned := make(chan struct{})
			go func() {
				cluster.takeOverSocketWithPermit(remote, httpContext, nil, permit)
				close(takeoverReturned)
			}()
			select {
			case callbackErr := <-callbackResult:
				if callbackErr != nil {
					t.Fatal(callbackErr)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s callback deadlocked while closing its server", callbackEvent)
			}
			select {
			case <-takeoverReturned:
			case <-time.After(time.Second):
				t.Fatal("takeover did not return after callback closed the server")
			}
		})
	}
}

func TestClusterCleanupUsesOneSharedPublicationDeadline(t *testing.T) {
	const responseTimeout = 60 * time.Millisecond
	bus := &stalledCleanupClusterBus{}
	owner, httpServer, firstSID := lifecycleClusterOwner(
		t,
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{
			ResponseTimeout:          responseTimeout,
			DelayedConnectionTimeout: time.Hour,
		},
	)
	secondSID := clusterHandshake(t, httpServer.URL)

	forwarders := make([]*forwardingTransport, 0, 2)
	for index, sid := range []string{firstSID, secondSID} {
		socket := clusterSocketForSID(t, owner, sid)
		owner.doConnect(sid, socket)
		forwarder := newForwardingTransport(
			owner,
			socket,
			socket.Transport(),
			fmt.Sprintf("forward-edge-%d", index),
			false,
		)
		if !socket.installClusterTransport(forwarder) {
			t.Fatalf("could not install forwarder %d", index)
		}
		forwarders = append(forwarders, forwarder)
	}

	for index := range 2 {
		owner.remoteRequests.Store(new(int), remoteRequest{
			sid:       fmt.Sprintf("orphan-%d", index),
			ownerID:   fmt.Sprintf("orphan-owner-%d", index),
			transport: transports.POLLING,
			lock:      ClusterReadLock,
			stopCleanup: func() bool {
				return true
			},
		})
	}
	remoteTransports := make([]*lifecycleTestTransport, 0, 2)
	for index := range 2 {
		transport := &lifecycleTestTransport{
			Transport: transports.MakeTransport(),
			name:      transports.WEBSOCKET,
		}
		remote := owner.hookRemoteTransport(
			fmt.Sprintf("remote-%d", index),
			fmt.Sprintf("remote-owner-%d", index),
			transport,
			index == 0,
		)
		if !owner.storeRemoteTransport(remote) {
			t.Fatalf("could not store remote transport %d", index)
		}
		remoteTransports = append(remoteTransports, transport)
	}

	started := time.Now()
	owner.Close()
	elapsed := time.Since(started)
	if elapsed > 4*responseTimeout {
		t.Fatalf("Cleanup took %s for parallel shutdown sets, want one shared deadline near %s", elapsed, 2*responseTimeout)
	}
	if got := bus.publishes.Load(); got < 6 {
		t.Fatalf("shutdown publish attempts = %d, want all forwarder/orphan/remote groups", got)
	}
	waitClusterCondition(t, func() bool { return bus.active.Load() == 0 })
	publishCount := bus.publishes.Load()
	time.Sleep(2 * responseTimeout)
	if got := bus.publishes.Load(); got != publishCount {
		t.Fatalf("shutdown worker started %d late publications after Cleanup returned", got-publishCount)
	}
	for index, forwarder := range forwarders {
		select {
		case <-forwarder.done:
		default:
			t.Fatalf("forwarder %d was not locally stopped at the shared deadline", index)
		}
	}
	for index, transport := range remoteTransports {
		if transport.ReadyState() != "closed" {
			t.Fatalf("remote transport %d state = %s, want closed", index, transport.ReadyState())
		}
	}
	if owner.RemoteTransportCount() != 0 {
		t.Fatalf("remote transport count after Cleanup = %d", owner.RemoteTransportCount())
	}
}

func TestClusterCleanupTracksDetachedInFlightPublications(t *testing.T) {
	const responseTimeout = 80 * time.Millisecond
	bus := &gatedCleanupClusterBus{gate: make(chan struct{})}
	owner, _, sid := lifecycleClusterOwner(
		t,
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{
			ResponseTimeout:          responseTimeout,
			DelayedConnectionTimeout: time.Hour,
		},
	)
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "failed-edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("could not install failure-path forwarder")
	}

	remoteTransport := &lifecycleTestTransport{
		Transport: transports.MakeTransport(),
		name:      transports.WEBSOCKET,
	}
	remote := owner.hookRemoteTransport("detached-remote", "remote-owner", remoteTransport, false)
	if !owner.storeRemoteTransport(remote) {
		t.Fatal("could not store failure-path remote")
	}
	request := httptest.NewRequest(http.MethodGet, "/engine.io/?EIO=4&transport=polling&sid=orphan", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	requestContext := types.NewHttpContext(httptest.NewRecorder(), request)
	owner.remoteRequests.Store(requestContext, remoteRequest{
		sid:       "detached-request",
		ownerID:   "request-owner",
		transport: transports.POLLING,
		lock:      ClusterReadLock,
	})

	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		forwarder.fail(errors.New("forced forwarder failure"))
	}()
	go func() {
		defer workers.Done()
		remote.notifyClose("transport close")
	}()
	go func() {
		defer workers.Done()
		owner.forgetRemoteRequest(requestContext)
	}()
	waitClusterCondition(t, func() bool {
		_, requestFound := owner.remoteRequests.Load(requestContext)
		return bus.reached.Load() == 3 &&
			socket.Transport() != forwarder &&
			owner.loadRemoteTransport(remote.sid) == nil &&
			!requestFound
	})
	if active := owner.activeTransportPublishes(); active != 3 {
		t.Fatalf("tracked publications before bus admission = %d, want 3", active)
	}
	if active := bus.active.Load(); active != 0 {
		t.Fatalf("publications passed the admission gate early: active = %d", active)
	}

	started := time.Now()
	owner.Close()
	elapsed := time.Since(started)
	if elapsed < responseTimeout/2 {
		t.Fatalf("Cleanup returned in %s while detached publications were still active", elapsed)
	}
	if elapsed > 3*responseTimeout {
		t.Fatalf("Cleanup took %s, want one bounded publication interval", elapsed)
	}
	if active := bus.active.Load(); active != 0 {
		t.Fatalf("active detached publications after Cleanup = %d", active)
	}
	if tracked := owner.activeTransportPublishes(); tracked != 0 {
		t.Fatalf("tracked detached publications after Cleanup = %d", tracked)
	}
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(time.Second):
		t.Fatal("detached publication workers survived Cleanup")
	}
}

func TestClusterClosingRejectsDetachedLatePublications(t *testing.T) {
	const responseTimeout = 80 * time.Millisecond
	bus := &stalledCleanupClusterBus{}
	owner, _, sid := lifecycleClusterOwner(
		t,
		bus,
		config.DefaultServerOptions(),
		&ClusterOptions{
			ResponseTimeout:          responseTimeout,
			DelayedConnectionTimeout: time.Hour,
		},
	)
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "late-forward-edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("could not install late-path forwarder")
	}
	remoteTransport := &lifecycleTestTransport{
		Transport: transports.MakeTransport(),
		name:      transports.WEBSOCKET,
	}
	remote := owner.hookRemoteTransport("late-remote", "late-remote-owner", remoteTransport, false)
	if !owner.storeRemoteTransport(remote) {
		t.Fatal("could not store late-path remote")
	}
	request := httptest.NewRequest(http.MethodGet, "/engine.io/?EIO=4&transport=polling&sid=late", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	requestContext := types.NewHttpContext(httptest.NewRecorder(), request)
	owner.remoteRequests.Store(requestContext, remoteRequest{
		sid:       "late-request",
		ownerID:   "late-request-owner",
		transport: transports.POLLING,
		lock:      ClusterReadLock,
	})

	owner.beginClose()
	started := time.Now()
	forwarder.fail(errors.New("late forwarder failure"))
	remote.notifyClose("transport close")
	owner.forgetRemoteRequest(requestContext)
	if elapsed := time.Since(started); elapsed > responseTimeout/2 {
		t.Fatalf("late shutdown publications blocked for %s after beginClose", elapsed)
	}
	if publishes := bus.publishes.Load(); publishes != 0 {
		t.Fatalf("late shutdown paths reached bus Publish %d times", publishes)
	}
	if active := owner.activeTransportPublishes(); active != 0 {
		t.Fatalf("late shutdown paths registered %d publications", active)
	}
	if socket.Transport() != forwarder {
		t.Fatal("late forwarder failure detached after shutdown admission closed")
	}
	if owner.loadRemoteTransport(remote.sid) != remote {
		t.Fatal("late remote close disappeared before Cleanup could snapshot it")
	}
	if _, found := owner.remoteRequests.Load(requestContext); !found {
		t.Fatal("late request disappeared before Cleanup could release its owner grant")
	}

	cleanupStarted := time.Now()
	owner.Close()
	if elapsed := time.Since(cleanupStarted); elapsed > 3*responseTimeout {
		t.Fatalf("Cleanup took %s after preserving late terminal state", elapsed)
	}
	if publishes := bus.publishes.Load(); publishes < 3 {
		t.Fatalf("Cleanup publication attempts = %d, want forwarder/request/remote CLOSE", publishes)
	}
	if active := owner.activeTransportPublishes(); active != 0 {
		t.Fatalf("tracked publications after Cleanup = %d", active)
	}
	if _, found := owner.remoteRequests.Load(requestContext); found {
		t.Fatal("Cleanup retained the late remote request")
	}
	if owner.loadRemoteTransport(remote.sid) != nil {
		t.Fatal("Cleanup retained the late remote transport")
	}
}

func TestClusterDiscardCloseRejectsNewAcquireWhilePublishBlocked(t *testing.T) {
	memory := NewMemoryClusterBus()
	closePublishStarted := make(chan struct{})
	releaseClosePublish := make(chan struct{})
	var closeGate sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(ctx context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageDrain && len(message.Packets) == 1 &&
			message.Packets[0].Type == enginepacket.CLOSE.String() {
			blocked := false
			closeGate.Do(func() {
				blocked = true
				close(closePublishStarted)
			})
			if blocked {
				select {
				case <-releaseClosePublish:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return next()
	}

	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{ResponseTimeout: 300 * time.Millisecond}
	owner, _, sid := lifecycleClusterOwner(t, bus, options, clusterOptions)
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edge := edgeServer.(*clusterServer)
	t.Cleanup(func() {
		edge.Close()
		_ = memory.Close()
	})
	socket := clusterSocketForSID(t, owner, sid)
	forwarder := newForwardingTransport(owner, socket, socket.Transport(), "active-edge", false)
	if !socket.installClusterTransport(forwarder) {
		t.Fatal("persistent forwarder install failed")
	}
	go socket.Close(true)
	select {
	case <-closePublishStarted:
	case <-time.After(time.Second):
		t.Fatal("forwarded CLOSE did not reach publish barrier")
	}
	if socket.ReadyState() != "closing" {
		t.Fatalf("socket state while CLOSE publish blocked = %q", socket.ReadyState())
	}
	if ownerID, success := edge.acquireLock(context.Background(), sid, transports.WEBSOCKET, ClusterReadLock); success {
		t.Fatalf("closing socket granted upgrade lock to %s", ownerID)
	}
	if ownerID, success := edge.acquireLock(context.Background(), sid, transports.POLLING, ClusterWriteLock); success {
		t.Fatalf("closing socket granted write lock to %s", ownerID)
	}
	close(releaseClosePublish)
	waitClusterCondition(t, func() bool { return socket.ReadyState() == "closed" })
}

func TestClusterWriteGrantCoversHTTPIdleTimeout(t *testing.T) {
	memory := NewMemoryClusterBus()
	options := config.DefaultServerOptions()
	options.SetPingTimeout(5 * time.Millisecond)
	options.SetIdleTimeout(80 * time.Millisecond)
	server, err := NewClusterServer(memory, options, &ClusterOptions{ResponseTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	owner := server.(*clusterServer)
	t.Cleanup(func() {
		owner.Close()
		_ = memory.Close()
	})
	owner.recordWriteGrant("sid", "edge")
	time.Sleep(4 * options.PingTimeout())
	if !owner.clearWriteGrant("sid", "edge") {
		t.Fatal("write grant expired at PingTimeout instead of covering IdleTimeout")
	}
}

func TestClusterSlowRemotePostAbortReleasesOwner(t *testing.T) {
	memory := NewMemoryClusterBus()
	options := config.DefaultServerOptions()
	options.SetPingInterval(10 * time.Second)
	options.SetPingTimeout(10 * time.Millisecond)
	options.SetIdleTimeout(500 * time.Millisecond)
	clusterOptions := &ClusterOptions{ResponseTimeout: 150 * time.Millisecond}
	ownerServer, err := NewClusterServer(memory, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(memory, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(ownerServer)
	edgeHTTP := httptest.NewServer(edgeServer)
	t.Cleanup(func() {
		edgeServer.Close()
		ownerServer.Close()
		edgeHTTP.CloseClientConnections()
		ownerHTTP.CloseClientConnections()
		edgeHTTP.Close()
		ownerHTTP.Close()
		_ = memory.Close()
	})
	owner := ownerServer.(*clusterServer)
	edge := edgeServer.(*clusterServer)
	sid := clusterHandshake(t, ownerHTTP.URL)
	ownerClosed := make(chan string, 1)
	_ = clusterSocketForSID(t, owner, sid).Once("close", func(values ...any) {
		reason, _ := values[0].(string)
		ownerClosed <- reason
	})

	reader, writer := io.Pipe()
	requestContext, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodPost,
		clusterPollingURL(edgeHTTP.URL, sid),
		reader,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()

	key := clusterWriteGrantKey{sid: sid, senderID: edge.NodeID()}
	waitClusterCondition(t, func() bool {
		owner.writeGrantMu.Lock()
		_, found := owner.writeGrants[key]
		owner.writeGrantMu.Unlock()
		return found
	})
	time.Sleep(4 * options.PingTimeout())
	cancel()
	_ = writer.CloseWithError(context.Canceled)
	select {
	case reason := <-ownerClosed:
		if reason != "transport close" && reason != "transport error" {
			t.Fatalf("owner close reason = %q", reason)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("slow POST cancellation did not close owner through write-grant release")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("slow POST did not stop after cancellation")
	}
	waitClusterCondition(t, func() bool {
		_, found := owner.Clients().Load(sid)
		return !found
	})
}

func TestClusterPacketPublishFailureClosesBothHalves(t *testing.T) {
	memory := NewMemoryClusterBus()
	sentinel := errors.New("packet publish failed")
	packetFailed := make(chan struct{})
	var failOnce sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessagePacket {
			failOnce.Do(func() { close(packetFailed) })
			return sentinel
		}
		return next()
	}
	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{ResponseTimeout: 150 * time.Millisecond}
	ownerServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(ownerServer)
	edgeHTTP := httptest.NewServer(edgeServer)
	t.Cleanup(func() {
		edgeServer.Close()
		ownerServer.Close()
		edgeHTTP.CloseClientConnections()
		ownerHTTP.CloseClientConnections()
		edgeHTTP.Close()
		ownerHTTP.Close()
		_ = memory.Close()
	})
	sid := clusterHandshake(t, ownerHTTP.URL)
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		_, _ = clusterRequestWithoutTesting(http.MethodPost, edgeHTTP.URL, sid, "4lost")
	}()
	select {
	case <-packetFailed:
	case <-time.After(time.Second):
		t.Fatal("remote packet did not reach failing bus")
	}
	waitClusterCondition(t, func() bool {
		_, found := ownerServer.Clients().Load(sid)
		return !found
	})
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("failed remote POST did not terminate")
	}
}

func TestClusterReadResponseDeliverThenErrorCompletesPoll(t *testing.T) {
	memory := NewMemoryClusterBus()
	sentinel := errors.New("response failed after delivery")
	var failResponse atomic.Bool
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		err := next()
		if message.Type == ClusterMessageAcquireLockResponse && failResponse.CompareAndSwap(false, true) {
			return sentinel
		}
		return err
	}
	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{ResponseTimeout: 150 * time.Millisecond}
	ownerServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(ownerServer)
	edgeHTTP := httptest.NewServer(edgeServer)
	t.Cleanup(func() {
		edgeServer.Close()
		ownerServer.Close()
		edgeHTTP.CloseClientConnections()
		ownerHTTP.CloseClientConnections()
		edgeHTTP.Close()
		ownerHTTP.Close()
		_ = memory.Close()
	})
	sid := clusterHandshake(t, ownerHTTP.URL)
	owner := ownerServer.(*clusterServer)
	clusterSocketForSID(t, owner, sid).Send(strings.NewReader("kept"), nil, nil)

	result := make(chan struct {
		status int
		body   string
	}, 1)
	go func() {
		status, body, _ := clusterRequestRaw(http.MethodGet, edgeHTTP.URL, sid, "")
		result <- struct {
			status int
			body   string
		}{status: status, body: body}
	}()
	select {
	case got := <-result:
		if got.status != http.StatusOK || got.body != "1" {
			t.Fatalf("ambiguous remote poll = status %d body %q", got.status, got.body)
		}
	case <-time.After(time.Second):
		t.Fatal("ambiguous remote poll did not receive rollback CLOSE")
	}
	status, body, err := clusterRequestRaw(http.MethodGet, ownerHTTP.URL, sid, "")
	if err != nil || status != http.StatusOK || body != "4kept" {
		t.Fatalf("owner packet after rollback = status %d body %q error %v", status, body, err)
	}
	waitClusterCondition(t, func() bool { return clusterRequestStateClean(edgeServer.(*clusterServer)) })
}

func TestClusterDrainDeliverThenErrorClosesOwnerAndEdge(t *testing.T) {
	memory := NewMemoryClusterBus()
	sentinel := errors.New("drain failed after delivery")
	var failEnabled atomic.Bool
	failed := make(chan struct{})
	var failOnce sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		err := next()
		if message.Type == ClusterMessageDrain && failEnabled.Load() {
			failOnce.Do(func() { close(failed) })
			return sentinel
		}
		return err
	}
	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{
		ResponseTimeout:          200 * time.Millisecond,
		DelayedConnectionTimeout: 20 * time.Millisecond,
	}
	ownerServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(ownerServer)
	edgeHTTP := httptest.NewServer(edgeServer)
	t.Cleanup(func() {
		edgeServer.Close()
		ownerServer.Close()
		edgeHTTP.CloseClientConnections()
		ownerHTTP.CloseClientConnections()
		edgeHTTP.Close()
		ownerHTTP.Close()
		_ = memory.Close()
	})
	owner := ownerServer.(*clusterServer)
	edge := edgeServer.(*clusterServer)
	sid := clusterHandshake(t, ownerHTTP.URL)
	waitClusterCondition(t, func() bool {
		owner.delayedMu.Lock()
		_, delayed := owner.delayed[sid]
		owner.delayedMu.Unlock()
		return !delayed
	})
	ownerSocket := clusterSocketForSID(t, owner, sid)
	wsURL := "ws" + strings.TrimPrefix(edgeHTTP.URL, "http") +
		"/engine.io/?EIO=4&transport=websocket&sid=" + sid
	connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("WebSocket dial status %d: %v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, probe, err := connection.ReadMessage()
	if err != nil || string(probe) != "3probe" {
		t.Fatalf("probe response = %q, %v", probe, err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("5")); err != nil {
		t.Fatal(err)
	}
	waitClusterCondition(t, func() bool {
		forwarder, ok := ownerSocket.Transport().(*forwardingTransport)
		return ok && !forwarder.oneShot && ownerSocket.Upgraded() && edge.RemoteTransportCount() == 1
	})

	failEnabled.Store(true)
	ownerSocket.Send(strings.NewReader("payload"), nil, nil)
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("forwarded DRAIN did not reach deliver-then-error hook")
	}
	waitClusterCondition(t, func() bool {
		_, ownerAlive := owner.Clients().Load(sid)
		return !ownerAlive && edge.RemoteTransportCount() == 0
	})
}

func TestClusterRemoteCloseWhileUpgradePendingCancelsOwnerGrant(t *testing.T) {
	memory := NewMemoryClusterBus()
	upgradeQueued := make(chan struct{})
	releaseUpgrade := make(chan struct{})
	var queueOnce sync.Once
	upgradeCanceled := make(chan struct{})
	var cancelOnce sync.Once
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		if message.Type == ClusterMessageUpgrade && !message.Success {
			err := next()
			cancelOnce.Do(func() { close(upgradeCanceled) })
			return err
		}
		if message.Type != ClusterMessageUpgrade || !message.Success {
			return next()
		}
		queued := false
		queueOnce.Do(func() {
			queued = true
			close(upgradeQueued)
		})
		if !queued {
			return next()
		}
		copy := cloneClusterMessage(message)
		go func() {
			<-releaseUpgrade
			_ = memory.Publish(context.Background(), &copy)
		}()
		return nil
	}
	options := config.DefaultServerOptions()
	clusterOptions := &ClusterOptions{ResponseTimeout: 120 * time.Millisecond}
	ownerServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := NewClusterServer(bus, options, clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(ownerServer)
	edgeHTTP := httptest.NewServer(edgeServer)
	t.Cleanup(func() {
		edgeServer.Close()
		ownerServer.Close()
		edgeHTTP.CloseClientConnections()
		ownerHTTP.CloseClientConnections()
		edgeHTTP.Close()
		ownerHTTP.Close()
		_ = memory.Close()
	})
	owner := ownerServer.(*clusterServer)
	edge := edgeServer.(*clusterServer)
	sid := clusterHandshake(t, ownerHTTP.URL)
	ownerSocket := clusterSocketForSID(t, owner, sid)
	wsURL := "ws" + strings.TrimPrefix(edgeHTTP.URL, "http") +
		"/engine.io/?EIO=4&transport=websocket&sid=" + sid
	connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("WebSocket dial status %d: %v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, probe, err := connection.ReadMessage()
	if err != nil || string(probe) != "3probe" {
		t.Fatalf("probe response = %q, %v", probe, err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("5")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-upgradeQueued:
	case <-time.After(time.Second):
		t.Fatal("successful UPGRADE did not reach delayed-delivery hook")
	}
	_ = connection.Close()
	waitClusterCondition(t, func() bool { return edge.RemoteTransportCount() == 0 })
	select {
	case <-upgradeCanceled:
	case <-time.After(time.Second):
		t.Fatal("pending edge close did not publish UPGRADE(false)")
	}
	close(releaseUpgrade)
	time.Sleep(2 * clusterOptions.ResponseTimeout)
	if ownerSocket.Upgraded() || ownerSocket.Upgrading() {
		t.Fatalf("closed pending edge left owner upgrade state = upgrading %t upgraded %t", ownerSocket.Upgrading(), ownerSocket.Upgraded())
	}
	if forwarder, ok := ownerSocket.Transport().(*forwardingTransport); ok && forwarder.active.Load() {
		t.Fatal("closed pending edge installed an active owner forwarder")
	}
	if _, found := edge.Clients().Load(sid); found {
		t.Fatal("closed pending edge was taken over into a local Socket")
	}
	waitClusterCondition(t, func() bool { return clusterRequestStateClean(edge) })
}

func TestClusterRemoteErrorBeforeStoreClosesTransportAndNotifiesOwner(t *testing.T) {
	memory := NewMemoryClusterBus()
	var messagesMu sync.Mutex
	var messages []ClusterMessage
	bus := &hookedClusterBus{base: memory}
	bus.hook = func(_ context.Context, message *ClusterMessage, next func() error) error {
		messagesMu.Lock()
		messages = append(messages, cloneClusterMessage(message))
		messagesMu.Unlock()
		return next()
	}
	server, err := NewClusterServer(bus, config.DefaultServerOptions(), &ClusterOptions{ResponseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	t.Cleanup(func() {
		cluster.Close()
		_ = memory.Close()
	})
	transport := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.WEBSOCKET}
	remote := cluster.hookRemoteTransport("01234567890123456789", "owner", transport, true)
	transport.Emit("error", errors.New("pre-store failure"))
	if transport.ReadyState() != "closed" || !transport.Discarded() || !remote.closed.Load() {
		t.Fatalf("failed remote state = ready %q discarded %t closed %t", transport.ReadyState(), transport.Discarded(), remote.closed.Load())
	}
	if cluster.storeRemoteTransport(remote) {
		t.Fatal("closed pre-store remote was resurrected")
	}
	if cluster.RemoteTransportCount() != 0 {
		t.Fatal("closed pre-store remote remained registered")
	}
	messagesMu.Lock()
	got := append([]ClusterMessage(nil), messages...)
	messagesMu.Unlock()
	if len(got) < 2 || got[0].Type != ClusterMessageUpgrade || got[0].Success || got[1].Type != ClusterMessageClose {
		t.Fatalf("pre-store failure notifications = %+v", got)
	}
}

func TestClusterTakeoverForwardsPreHandoffCloseSnapshot(t *testing.T) {
	memory := NewMemoryClusterBus()
	server, err := NewClusterServer(memory, config.DefaultServerOptions(), &ClusterOptions{ResponseTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	t.Cleanup(func() {
		cluster.Close()
		_ = memory.Close()
	})
	transport := &snapshotLifecycleTransport{
		lifecycleTestTransport: &lifecycleTestTransport{
			Transport: transports.MakeTransport(),
			name:      transports.WEBSOCKET,
		},
		snapshot: make(chan struct{}),
		release:  make(chan struct{}),
	}
	sid := "01234567890123456789"
	remote := cluster.hookRemoteTransport(sid, "owner", transport, true)
	if !cluster.storeRemoteTransport(remote) {
		t.Fatal("could not store takeover remote")
	}
	closeDelivered := make(chan struct{})
	go func() {
		transport.Emit("close")
		close(closeDelivered)
	}()
	select {
	case <-transport.snapshot:
	case <-time.After(time.Second):
		t.Fatal("old remote listener snapshot was not captured")
	}
	request := httptest.NewRequest(http.MethodGet, "/engine.io/?EIO=4&transport=websocket&sid="+sid, nil)
	request.RemoteAddr = "127.0.0.1:12345"
	httpContext := types.NewHttpContext(httptest.NewRecorder(), request)
	cluster.takeOverSocket(remote, httpContext, nil)
	if _, found := cluster.Clients().Load(sid); !found {
		t.Fatal("takeover did not publish its permanent Socket before old callback resumed")
	}
	close(transport.release)
	select {
	case <-closeDelivered:
	case <-time.After(time.Second):
		t.Fatal("snapshotted remote close callback did not resume")
	}
	waitClusterCondition(t, func() bool {
		_, found := cluster.Clients().Load(sid)
		return !found && cluster.ClientsCount() == 0
	})
}

func TestClusterTakeoverDrainCallbackUsesActualTransport(t *testing.T) {
	memory := NewMemoryClusterBus()
	server, err := NewClusterServer(
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: 100 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	cluster := server.(*clusterServer)
	t.Cleanup(func() {
		cluster.Close()
		_ = memory.Close()
	})
	actual := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.WEBSOCKET}
	actual.SetWritable(true)
	sid := "98765432109876543210"
	remote := cluster.hookRemoteTransport(sid, "owner", actual, true)
	if !cluster.storeRemoteTransport(remote) {
		t.Fatal("could not store takeover remote")
	}
	request := httptest.NewRequest(http.MethodGet, "/engine.io/?EIO=4&transport=websocket&sid="+sid, nil)
	request.RemoteAddr = "127.0.0.1:12345"
	cluster.takeOverSocket(remote, types.NewHttpContext(httptest.NewRecorder(), request), nil)
	loaded, found := cluster.Clients().Load(sid)
	if !found {
		t.Fatal("takeover client was not registered")
	}
	client := loaded.(*socket)
	callbackDone := make(chan transports.Transport, 1)
	client.Send(strings.NewReader("after-takeover"), nil, func(source transports.Transport) {
		callbackDone <- source
	})
	actual.Emit("drain")
	select {
	case source := <-callbackDone:
		if source != actual {
			t.Fatalf("takeover callback transport = %T, want actual edge transport", source)
		}
	case <-time.After(time.Second):
		t.Fatal("takeover send callback did not run")
	}
}

func TestLocalUpgradeCloseRaceCannotLeaveDeadTransportOpen(t *testing.T) {
	memory := NewMemoryClusterBus()
	owner, _, sid := lifecycleClusterOwner(
		t,
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: 100 * time.Millisecond},
	)
	socket := clusterSocketForSID(t, owner, sid)
	candidate := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.WEBSOCKET}
	socket.MaybeUpgrade(candidate)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		candidate.Emit("packet", &enginepacket.Packet{Type: enginepacket.UPGRADE})
	}()
	go func() {
		defer group.Done()
		<-start
		candidate.Close()
	}()
	close(start)
	group.Wait()
	waitClusterCondition(t, func() bool { return !socket.Upgrading() })
	if socket.Transport() == candidate && candidate.ReadyState() == "closed" && socket.ReadyState() != "closed" {
		t.Fatal("upgrade race left an open Socket bound to a closed candidate transport")
	}
	_ = memory.Close()
}

func TestClusterDelayedConnectionSerializesLocalUpgradeEvent(t *testing.T) {
	memory := NewMemoryClusterBus()
	owner, _, sid := lifecycleClusterOwner(
		t,
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{
			ResponseTimeout:          100 * time.Millisecond,
			DelayedConnectionTimeout: time.Hour,
		},
	)
	t.Cleanup(func() { _ = memory.Close() })

	connectionEntered := make(chan struct{})
	releaseConnection := make(chan struct{})
	var upgradeEvents atomic.Int64
	_ = owner.On("connection", func(values ...any) {
		if len(values) == 0 {
			return
		}
		connected, ok := values[0].(Socket)
		if !ok || connected == nil || connected.Id() != sid {
			return
		}
		_ = connected.On("upgrade", func(...any) { upgradeEvents.Add(1) })
		close(connectionEntered)
		<-releaseConnection
	})

	socket := clusterSocketForSID(t, owner, sid)
	candidate := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.WEBSOCKET}
	socket.MaybeUpgrade(candidate)
	connectDone := make(chan struct{})
	go func() {
		owner.doConnect(sid, socket)
		close(connectDone)
	}()
	select {
	case <-connectionEntered:
	case <-time.After(time.Second):
		t.Fatal("delayed connection handler did not reach barrier")
	}

	upgradeDone := make(chan struct{})
	go func() {
		candidate.Emit("packet", &enginepacket.Packet{Type: enginepacket.UPGRADE})
		close(upgradeDone)
	}()
	select {
	case <-upgradeDone:
		t.Fatal("local upgrade overtook the in-flight connection announcement")
	case <-time.After(30 * time.Millisecond):
	}
	if socket.Upgraded() {
		t.Fatal("upgrade committed while connection listeners were still being announced")
	}

	close(releaseConnection)
	select {
	case <-connectDone:
	case <-time.After(time.Second):
		t.Fatal("delayed connection handler did not finish")
	}
	select {
	case <-upgradeDone:
	case <-time.After(time.Second):
		t.Fatal("local upgrade did not resume after connection announcement")
	}
	if !socket.Upgraded() || socket.Transport() != candidate {
		t.Fatal("local upgrade did not commit after the connection barrier")
	}
	if got := upgradeEvents.Load(); got != 1 {
		t.Fatalf("application observed %d upgrade events, want 1", got)
	}
}

func TestLocalUpgradeWaitsForPreviousTransportDrain(t *testing.T) {
	memory := NewMemoryClusterBus()
	owner, _, sid := lifecycleClusterOwner(
		t,
		memory,
		config.DefaultServerOptions(),
		&ClusterOptions{ResponseTimeout: 100 * time.Millisecond},
	)
	t.Cleanup(func() { _ = memory.Close() })
	socket := clusterSocketForSID(t, owner, sid)
	owner.doConnect(sid, socket)

	previous := socket.Transport()
	oldTransport := &blockingSendLifecycleTransport{
		lifecycleTestTransport: &lifecycleTestTransport{
			Transport: transports.MakeTransport(),
			name:      transports.POLLING,
		},
		sendStarted: make(chan struct{}),
		releaseSend: make(chan struct{}),
	}
	oldTransport.SetWritable(true)
	socket.flushMu.Lock()
	if socket.sentCallbackFn.Len() != 0 {
		socket.flushMu.Unlock()
		t.Fatal("handshake transport still has an in-flight batch")
	}
	socket.cleanupTransportListeners()
	socket.setTransport(oldTransport)
	socket.flushMu.Unlock()
	previous.Discard()
	previous.Close()

	var eventsMu sync.Mutex
	events := make([]string, 0, 3)
	appendEvent := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	var oldCallbacks atomic.Int64
	var oldCallbackTransport atomic.Pointer[transports.Transport]
	oldCallbackEntered := make(chan struct{})
	releaseOldCallback := make(chan struct{})
	sendDone := make(chan struct{})
	go func() {
		socket.Send(strings.NewReader("old"), nil, func(transport transports.Transport) {
			oldCallbacks.Add(1)
			value := transport
			oldCallbackTransport.Store(&value)
			close(oldCallbackEntered)
			<-releaseOldCallback
			appendEvent("old-callback")
		})
		close(sendDone)
	}()
	select {
	case <-oldTransport.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("old transport Send did not reach barrier")
	}

	candidate := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.WEBSOCKET}
	candidate.SetWritable(true)
	var newCallbacks atomic.Int64
	newSendQueued := make(chan struct{})
	_ = socket.On("upgrade", func(...any) {
		appendEvent("upgrade")
		socket.Send(strings.NewReader("new"), nil, func(transport transports.Transport) {
			if transport != candidate {
				t.Errorf("new callback transport = %T, want candidate", transport)
			}
			newCallbacks.Add(1)
			appendEvent("new-callback")
		})
		close(newSendQueued)
	})
	socket.MaybeUpgrade(candidate)
	upgradePacketDone := make(chan struct{})
	go func() {
		candidate.Emit("packet", &enginepacket.Packet{Type: enginepacket.UPGRADE})
		close(upgradePacketDone)
	}()
	select {
	case <-upgradePacketDone:
		t.Fatal("upgrade packet overtook the blocked old Send")
	case <-time.After(30 * time.Millisecond):
	}

	close(oldTransport.releaseSend)
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("old Send did not return")
	}
	select {
	case <-upgradePacketDone:
	case <-time.After(time.Second):
		t.Fatal("upgrade packet did not reach the old-drain barrier")
	}
	if socket.Upgraded() {
		t.Fatal("upgrade committed before the old transport drain")
	}
	if oldCallbacks.Load() != 0 {
		t.Fatal("old batch callback ran before its transport drain")
	}

	drainDone := make(chan struct{})
	go func() {
		oldTransport.Emit("drain")
		close(drainDone)
	}()
	select {
	case <-oldCallbackEntered:
	case <-time.After(time.Second):
		t.Fatal("old transport drain did not enter its callback barrier")
	}
	if socket.Upgraded() {
		t.Fatal("upgrade overtook the old transport drain callback")
	}
	if got := oldCallbacks.Load(); got != 1 {
		t.Fatalf("old batch callback count = %d, want 1", got)
	}
	if callbackTransport := oldCallbackTransport.Load(); callbackTransport == nil || *callbackTransport != oldTransport {
		t.Fatalf("old callback transport = %v, want old polling transport", callbackTransport)
	}
	if newCallbacks.Load() != 0 {
		t.Fatal("new batch callback ran before the candidate drain")
	}
	close(releaseOldCallback)
	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("old transport drain callback did not finish")
	}
	waitClusterCondition(t, func() bool {
		return socket.Upgraded() && socket.Transport() == candidate
	})
	select {
	case <-newSendQueued:
	case <-time.After(time.Second):
		t.Fatal("upgrade handler did not queue the candidate batch")
	}
	candidate.Emit("drain")
	if got := newCallbacks.Load(); got != 1 {
		t.Fatalf("new batch callback count = %d, want 1", got)
	}
	eventsMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	wantEvents := []string{"old-callback", "upgrade", "new-callback"}
	if fmt.Sprint(gotEvents) != fmt.Sprint(wantEvents) {
		t.Fatalf("event order = %v, want %v", gotEvents, wantEvents)
	}
}

func clusterRequestWithoutTesting(method, baseURL, sid, body string) (int, error) {
	status, _, err := clusterRequestRaw(method, baseURL, sid, body)
	return status, err
}

func clusterRequestRaw(method, baseURL, sid, body string) (int, string, error) {
	request, err := http.NewRequest(method, clusterPollingURL(baseURL, sid), strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	return response.StatusCode, string(payload), err
}

func TestSocketCloseIsSingleWinner(t *testing.T) {
	memory := NewMemoryClusterBus()
	options := config.DefaultServerOptions()
	owner, _, sid := lifecycleClusterOwner(t, memory, options, &ClusterOptions{ResponseTimeout: 100 * time.Millisecond})
	socket := clusterSocketForSID(t, owner, sid)
	var closeEvents atomic.Int64
	_ = socket.On("close", func(...any) { closeEvents.Add(1) })

	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range 100 {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			if index%2 == 0 {
				socket.Close(false)
				return
			}
			socket.OnClose("concurrent close")
		}(index)
	}
	close(start)
	group.Wait()
	waitClusterCondition(t, func() bool { return socket.ReadyState() == "closed" })
	if got := closeEvents.Load(); got != 1 {
		t.Fatalf("close event count = %d, want 1", got)
	}
	waitClusterCondition(t, func() bool { return owner.ClientsCount() == 0 })
	if socket.ReadyState() != "closed" {
		t.Fatalf("final ready state = %q", socket.ReadyState())
	}
	_ = memory.Close()
}

func TestConnectionHandlerSynchronousCloseIsOrdered(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.CloseClientConnections()
		httpServer.Close()
	})
	var orderMu sync.Mutex
	var order []string
	closed := make(chan struct{})
	_ = server.On("connection", func(values ...any) {
		socket := values[0].(Socket)
		orderMu.Lock()
		order = append(order, "connection")
		orderMu.Unlock()
		_ = socket.Once("close", func(...any) {
			orderMu.Lock()
			order = append(order, "close")
			orderMu.Unlock()
			close(closed)
		})
		socket.Close(true)
	})
	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling") //nolint:gosec,noctx
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close event deadlocked inside connection handler")
	}
	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	if len(got) != 2 || got[0] != "connection" || got[1] != "close" {
		t.Fatalf("connection/close order = %v", got)
	}
}

func TestConnectionAnnouncementDefersCloseAcrossCheckEmitGap(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	transport := &lifecycleTestTransport{Transport: transports.MakeTransport(), name: transports.POLLING}
	socket := makeSocket()
	socket.server = server
	socket.id = "announcement-gap"
	socket.setTransport(transport)
	socket.readyState.Store("open")

	socket.beginConnectionAnnouncement()
	if socket.ReadyState() != "open" {
		t.Fatal("socket was not open at announcement check")
	}
	// This is the exact check -> Emit gap: resource cleanup wins now, but the
	// user-visible close event must wait until the connection handler can attach.
	socket.OnClose("gap close")
	order := []string{"connection"}
	closed := make(chan struct{})
	_ = socket.Once("close", func(...any) {
		order = append(order, "close")
		close(closed)
	})
	socket.finishConnectionAnnouncement()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("deferred close event was not released after connection announcement")
	}
	if len(order) != 2 || order[0] != "connection" || order[1] != "close" {
		t.Fatalf("check/emit gap order = %v", order)
	}
}
