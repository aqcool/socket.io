package engine

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

// forwardingTransport keeps the authoritative Socket on its owner node while
// replacing only its I/O edge. A polling edge is consumed once; a WebSocket
// edge remains installed until the session closes or is taken over.
type forwardingTransport struct {
	transports.Transport

	server   *clusterServer
	socket   *socket
	sid      string
	targetID string
	oneShot  bool
	slot     *transports.Transport
	previous *transports.Transport

	active          atomic.Bool
	writable        atomic.Bool
	closing         atomic.Bool
	shutdownAllowed atomic.Bool
	sendMu          sync.Mutex
	sending         bool
	releasePending  bool
	queue           []forwardingBatch
	done            chan struct{}
	doneOnce        sync.Once

	publishMu     sync.Mutex
	publishCancel context.CancelFunc
	forced        bool
}

type forwardingBatch struct {
	packets   []ClusterPacket
	close     bool
	failure   error
	callbacks []types.Callable
	permit    *transportPublishPermit
}

func newForwardingTransport(
	server *clusterServer,
	socket *socket,
	base transports.Transport,
	targetID string,
	oneShot bool,
) *forwardingTransport {
	transport := &forwardingTransport{
		Transport: base,
		server:    server,
		socket:    socket,
		sid:       socket.Id(),
		targetID:  targetID,
		oneShot:   oneShot,
		done:      make(chan struct{}),
	}
	transport.active.Store(true)
	transport.writable.Store(true)
	return transport
}

func (t *forwardingTransport) Writable() bool {
	return t.active.Load() && t.writable.Load()
}

func (t *forwardingTransport) SetWritable(value bool) {
	t.writable.Store(value)
}

func (t *forwardingTransport) oneShotReleasePending() bool {
	t.sendMu.Lock()
	pending := t.oneShot && t.releasePending
	t.sendMu.Unlock()
	return pending
}

func (t *forwardingTransport) Send(packets []*enginepacket.Packet) {
	batch := forwardingBatch{packets: make([]ClusterPacket, 0, len(packets))}
	for _, packet := range packets {
		wirePacket, err := packetToClusterPacket(packet)
		if err != nil {
			t.closeOnConversionError(err)
			return
		}
		batch.packets = append(batch.packets, wirePacket)
	}
	t.enqueue(&batch, false)
}

// closeOnConversionError terminates the remote edge with an ordered Engine.IO
// CLOSE packet. Treating a failed conversion as an empty successful batch would
// restore a one-shot owner transport while leaving the remote polling GET open.
func (t *forwardingTransport) closeOnConversionError(err error) {
	if !t.closing.CompareAndSwap(false, true) {
		return
	}

	t.sendMu.Lock()
	if !t.active.Load() {
		t.sendMu.Unlock()
		go func() {
			socketClosed := t.waitForSocketFlush()
			t.server.reportClusterError(err)
			if !socketClosed {
				t.socket.OnClose("transport error", err)
			}
			t.Discard()
			t.Transport.Close()
			t.signalDone()
		}()
		return
	}
	t.queue = append(t.queue, forwardingBatch{
		packets: []ClusterPacket{{Type: enginepacket.CLOSE.String()}},
		close:   true,
		failure: err,
	})
	t.writable.Store(false)
	if !t.sending {
		t.sending = true
		t.sendMu.Unlock()
		go t.drainQueue()
		return
	}
	t.sendMu.Unlock()
}

func (t *forwardingTransport) enqueue(batch *forwardingBatch, allowClosing bool) {
	t.sendMu.Lock()
	if !t.active.Load() || (t.closing.Load() && !allowClosing) {
		t.sendMu.Unlock()
		return
	}
	t.queue = append(t.queue, *batch)
	t.writable.Store(false)
	if t.sending {
		t.sendMu.Unlock()
		return
	}
	t.sending = true
	t.sendMu.Unlock()
	go t.drainQueue()
}

func (t *forwardingTransport) drainQueue() {
	for {
		t.sendMu.Lock()
		if len(t.queue) == 0 {
			t.sending = false
			active := t.active.Load() && !t.closing.Load()
			if active {
				t.writable.Store(true)
			}
			t.sendMu.Unlock()
			if active {
				t.Emit("ready")
			}
			return
		}
		batch := t.queue[0]
		t.queue = t.queue[1:]
		// A DRAIN may be delivered by an asynchronous bus before Publish returns.
		// Mark that narrow phase while holding sendMu so a causally subsequent
		// polling read can wait for this one-shot edge to restore the owner transport.
		if t.oneShot && len(batch.packets) > 0 {
			t.releasePending = true
		}
		t.sendMu.Unlock()

		var publishErr error
		if len(batch.packets) > 0 {
			message := &ClusterMessage{
				RecipientID: t.targetID,
				Type:        ClusterMessageDrain,
				SID:         t.sid,
				Packets:     batch.packets,
			}
			if batch.permit != nil {
				publishErr = batch.permit.publish(message)
				batch.permit.release()
				batch.permit = nil
			} else {
				publishErr = t.publishTransportMessage(message, t.shutdownAllowed.Load())
			}
		} else if batch.permit != nil {
			batch.permit.release()
			batch.permit = nil
		}
		if publishErr != nil {
			// Send is called while Socket.flush holds flushMu. Wait until it has
			// returned before emitting user-visible errors or close events, so a
			// listener may safely re-enter Send or Close.
			_ = t.waitForSocketFlush()
			if batch.failure != nil {
				t.server.reportClusterError(batch.failure)
			}
			t.fail(publishErr)
			t.server.reportClusterError(publishErr)
			return
		}
		socketClosed := false
		if batch.failure != nil {
			socketClosed = t.waitForSocketFlush()
			t.server.reportClusterError(batch.failure)
		}

		// A single worker preserves transport drain order even when a drain callback
		// synchronously queues CLOSE or another application batch.
		// A conversion failure must not acknowledge the failed application batch.
		// Socket.OnClose clears its pending callbacks after the ordered CLOSE has
		// reached the remote edge.
		if batch.failure == nil {
			t.Emit("drain")
		}
		if batch.close {
			t.active.Store(false)
			t.writable.Store(false)
			if batch.failure != nil && !socketClosed {
				t.socket.OnClose("transport error", batch.failure)
			}
			t.Discard()
			t.Transport.Close(batch.callbacks...)
			t.signalDone()
			return
		}
		if t.oneShot {
			t.sendMu.Lock()
			if !t.closing.Load() && len(t.queue) == 0 {
				t.server.recordReadGrant(t.sid, t.targetID)
				t.sending = false
				t.active.Store(false)
				t.writable.Store(false)
				t.sendMu.Unlock()
				t.socket.restoreClusterTransport(t)
				t.signalDone()
				return
			}
			t.sendMu.Unlock()
		}
	}
}

func (t *forwardingTransport) waitForSocketFlush() bool {
	t.socket.flushMu.Lock()
	closed := t.socket.ReadyState() == "closed"
	t.socket.flushMu.Unlock()
	return closed
}

func (t *forwardingTransport) fail(err error) {
	// BaseServer.Close may already have queued a CLOSE with a shutdown permit
	// behind the batch which just failed. Consume that permit first: attempting
	// a normal reservation after beginClose would be rejected and strand the
	// queued CLOSE with no worker.
	var closePermit *transportPublishPermit
	t.sendMu.Lock()
	for index := range t.queue {
		if t.queue[index].close && t.queue[index].permit != nil {
			closePermit = t.queue[index].permit
			t.queue[index].permit = nil
			break
		}
	}
	t.sendMu.Unlock()

	if closePermit == nil {
		t.publishMu.Lock()
		forced := t.forced
		t.publishMu.Unlock()
		if forced || !t.active.Load() {
			return
		}
		closePermit, _ = t.server.reserveTimedTransportPublish(false)
	}
	if closePermit == nil {
		// The admission barrier won before this failure detached. Re-check for a
		// shutdown CLOSE queued concurrently with the failed reservation. If none
		// exists, leave the forwarder installed so BaseServer.Close can enqueue
		// one later and restart the ordered worker.
		t.sendMu.Lock()
		for index := range t.queue {
			if t.queue[index].close && t.queue[index].permit != nil {
				closePermit = t.queue[index].permit
				t.queue[index].permit = nil
				break
			}
		}
		if closePermit == nil {
			t.sending = false
			t.writable.Store(false)
			t.sendMu.Unlock()
			return
		}
		t.sendMu.Unlock()
	}

	t.sendMu.Lock()
	if !t.active.Load() {
		queued := t.queue
		t.queue = nil
		t.sending = false
		t.sendMu.Unlock()
		closePermit.release()
		for _, batch := range queued {
			if batch.permit != nil {
				batch.permit.release()
			}
		}
		return
	}
	queued := t.queue
	t.queue = nil
	t.sending = false
	t.active.Store(false)
	t.writable.Store(false)
	t.sendMu.Unlock()
	for index := range queued {
		if queued[index].permit != nil {
			queued[index].permit.release()
		}
	}
	t.socket.restoreClusterTransport(t)
	closeErr := closePermit.publish(&ClusterMessage{
		RecipientID: t.targetID,
		Type:        ClusterMessageClose,
		SID:         t.sid,
		Reason:      "transport error",
	})
	closePermit.release()
	t.server.reportClusterError(closeErr)
	t.socket.OnClose("transport error", err)
	t.Discard()
	t.Transport.Close()
	t.signalDone()
}

// forceStop is the shutdown deadline fallback. The ordered worker has already
// had the shared grace interval to publish CLOSE, so this path performs local
// teardown only and must not start another full publish timeout.
func (t *forwardingTransport) forceStop(err error) {
	t.publishMu.Lock()
	t.forced = true
	cancelPublish := t.publishCancel
	t.publishMu.Unlock()
	if cancelPublish != nil {
		cancelPublish()
	}
	t.sendMu.Lock()
	queued := t.queue
	t.queue = nil
	t.sending = false
	t.active.Store(false)
	t.writable.Store(false)
	t.sendMu.Unlock()
	for _, batch := range queued {
		if batch.permit != nil {
			batch.permit.release()
		}
	}
	t.socket.restoreClusterTransport(t)
	t.socket.OnClose("transport error", err)
	t.Discard()
	t.Transport.Close()
	t.signalDone()
}

func (t *forwardingTransport) publishTransportMessage(
	message *ClusterMessage,
	allowClosing bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.server.options.ResponseTimeout)
	t.publishMu.Lock()
	if t.forced {
		t.publishMu.Unlock()
		cancel()
		return context.Canceled
	}
	t.publishCancel = cancel
	t.publishMu.Unlock()

	err := t.server.publishTransportMessageTracked(ctx, message, allowClosing)
	t.publishMu.Lock()
	t.publishCancel = nil
	t.publishMu.Unlock()
	cancel()
	return err
}

func (t *forwardingTransport) OnRequest(ctx *types.HttpContext) {
	// The official polling transport marks the owner's request slot while a
	// remote read owns it. socket.onRequest waits out the narrow post-DRAIN
	// restoration phase; every GET that still reaches this one-shot is either a
	// true overlap or a terminal stale edge and must receive the same 400.
	if t.oneShot && ctx.Method() == http.MethodGet {
		_ = ctx.SetStatusCode(http.StatusBadRequest)
		_, _ = ctx.Write(nil)
		return
	}
	t.Transport.OnRequest(ctx)
}

func (t *forwardingTransport) Close(callbacks ...types.Callable) {
	// clusterServer.Close marks closing before BaseServer closes its clients.
	// Reserve a shutdown publication permit synchronously, before the ordered
	// worker can be delayed behind an existing application batch. Cleanup tracks
	// this forwarder and bounds every permitted publication with its shared
	// deadline; detached failure paths never receive this permit.
	if t.server.closing.Load() {
		t.shutdownAllowed.Store(true)
	}
	permit, _ := t.server.reserveTimedTransportPublish(t.shutdownAllowed.Load())
	if !t.closing.CompareAndSwap(false, true) {
		attached := false
		if permit != nil {
			t.sendMu.Lock()
			for index := range t.queue {
				if t.queue[index].close && t.queue[index].permit == nil {
					t.queue[index].permit = permit
					attached = true
					break
				}
			}
			t.sendMu.Unlock()
			if !attached {
				permit.release()
			}
		}
		return
	}
	t.sendMu.Lock()
	if !t.active.Load() {
		t.sendMu.Unlock()
		if permit != nil {
			permit.release()
		}
		t.Discard()
		t.Transport.Close(callbacks...)
		t.signalDone()
		return
	}
	t.queue = append(t.queue, forwardingBatch{
		packets:   []ClusterPacket{{Type: enginepacket.CLOSE.String()}},
		close:     true,
		callbacks: callbacks,
		permit:    permit,
	})
	t.writable.Store(false)
	if !t.sending {
		t.sending = true
		t.sendMu.Unlock()
		go t.drainQueue()
		return
	}
	t.sendMu.Unlock()
}

func (t *forwardingTransport) deactivateAndRestore() {
	t.sendMu.Lock()
	if !t.active.Swap(false) {
		t.sendMu.Unlock()
		return
	}
	t.writable.Store(false)
	t.sendMu.Unlock()
	t.socket.restoreClusterTransport(t)
	t.signalDone()
}

// finishOneShotForUpgrade completes an outstanding polling read before a
// persistent upgraded edge is installed. If no application packet is already
// in flight, a NOOP consumes the polling request just like the official fast
// upgrade path. Waiting on done also establishes DRAIN-before-UPGRADE ordering.
func (t *forwardingTransport) finishOneShotForUpgrade() {
	if !t.oneShot {
		return
	}
	t.sendMu.Lock()
	if t.active.Load() && !t.closing.Load() && !t.sending && len(t.queue) == 0 {
		t.queue = append(t.queue, forwardingBatch{packets: []ClusterPacket{{
			Type: enginepacket.NOOP.String(),
		}}})
		t.writable.Store(false)
		t.sending = true
		t.sendMu.Unlock()
		go t.drainQueue()
		return
	}
	t.sendMu.Unlock()
}

func (t *forwardingTransport) signalDone() {
	t.doneOnce.Do(func() { close(t.done) })
}

func (s *socket) installClusterTransport(transport *forwardingTransport) bool {
	for {
		if s.ReadyState() != "open" || transport.server.closing.Load() {
			transport.stopWithoutSocket()
			return false
		}
		expected := s.transport.Load()
		if expected == nil {
			transport.stopWithoutSocket()
			return false
		}
		if current, ok := (*expected).(*forwardingTransport); ok && current.oneShot {
			current.finishOneShotForUpgrade()
			select {
			case <-current.done:
				continue
			case <-transport.server.closed:
				transport.stopWithoutSocket()
				return false
			}
		}

		s.flushMu.Lock()
		expected = s.transport.Load()
		if expected == nil || s.ReadyState() != "open" || transport.server.closing.Load() {
			s.flushMu.Unlock()
			transport.stopWithoutSocket()
			return false
		}
		base := *expected
		if current, ok := base.(*forwardingTransport); ok && current.oneShot {
			s.flushMu.Unlock()
			continue
		}
		transport.Transport = base
		if s.reserveClusterTransportCAS(transport, expected) {
			s.flushMu.Unlock()
			s.flush()
			return true
		}
		s.flushMu.Unlock()
	}
}

func (t *forwardingTransport) stopWithoutSocket() {
	t.active.Store(false)
	t.writable.Store(false)
	t.signalDone()
}

func (s *socket) reserveClusterTransportCAS(
	transport *forwardingTransport,
	expected *transports.Transport,
) bool {
	if expected == nil {
		return false
	}
	transport.previous = expected
	value := transports.Transport(transport)
	transport.slot = &value
	return s.transport.CompareAndSwap(expected, transport.slot)
}

func (s *socket) restoreClusterTransport(current *forwardingTransport) {
	if current.slot == nil || current.previous == nil {
		return
	}
	// A newer polling read or WebSocket commit may already have replaced this
	// forwarder. CAS prevents an older one-shot drain from overwriting that edge.
	s.transport.CompareAndSwap(current.slot, current.previous)
}

// suppressSendTransport lets takeover initialize a regular Engine.IO Socket
// (including heartbeat timers) without flushing a second OPEN packet.
type suppressSendTransport struct {
	transports.Transport
}

func (*suppressSendTransport) Send([]*enginepacket.Packet) {}
func (*suppressSendTransport) Writable() bool              { return false }
func (*suppressSendTransport) SetWritable(bool)            {}
