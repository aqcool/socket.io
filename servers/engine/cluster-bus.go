package engine

import (
	"context"
	"errors"
	"sync"
)

const clusterMessageSource = "_eio"

// ClusterMessageType is the wire-level operation used by ClusterServer.
// The values intentionally match @socket.io/cluster-engine@0.1.0.
type ClusterMessageType uint8

const (
	ClusterMessageAcquireLock ClusterMessageType = iota
	ClusterMessageAcquireLockResponse
	ClusterMessageDrain
	ClusterMessagePacket
	ClusterMessageUpgrade
	ClusterMessageUpgradeResponse
	ClusterMessageClose
)

// ClusterLockType identifies which half of a polling exchange is being routed.
type ClusterLockType string

const (
	ClusterReadLock  ClusterLockType = "read"
	ClusterWriteLock ClusterLockType = "write"
)

// ClusterPacket is the transport-independent representation of an Engine.IO
// packet. Data is copied before publication so buses may deliver asynchronously.
type ClusterPacket struct {
	Type    string `json:"type" msgpack:"type"`
	Data    []byte `json:"data,omitempty" msgpack:"data,omitempty"`
	HasData bool   `json:"hasData,omitempty" msgpack:"hasData,omitempty"`
	Binary  bool   `json:"binary,omitempty" msgpack:"binary,omitempty"`
	// HasCompress distinguishes an absent packet option from explicit false.
	HasCompress bool `json:"hasCompress,omitempty" msgpack:"hasCompress,omitempty"`
	Compress    bool `json:"compress,omitempty" msgpack:"compress,omitempty"`
}

// ClusterMessage is the complete cluster-engine wire envelope. RecipientID is
// empty for a broadcast and set for a point-to-point response or data frame.
type ClusterMessage struct {
	Source      string             `json:"source" msgpack:"source"`
	SenderID    string             `json:"senderId" msgpack:"senderId"`
	RecipientID string             `json:"recipientId,omitempty" msgpack:"recipientId,omitempty"`
	RequestID   uint64             `json:"requestId,omitempty" msgpack:"requestId,omitempty"`
	Type        ClusterMessageType `json:"type" msgpack:"type"`

	SID       string          `json:"sid,omitempty" msgpack:"sid,omitempty"`
	Transport string          `json:"transport,omitempty" msgpack:"transport,omitempty"`
	LockType  ClusterLockType `json:"lockType,omitempty" msgpack:"lockType,omitempty"`
	Success   bool            `json:"success,omitempty" msgpack:"success,omitempty"`
	TakeOver  bool            `json:"takeOver,omitempty" msgpack:"takeOver,omitempty"`
	Reason    string          `json:"reason,omitempty" msgpack:"reason,omitempty"`

	Packet  *ClusterPacket  `json:"packet,omitempty" msgpack:"packet,omitempty"`
	Packets []ClusterPacket `json:"packets,omitempty" msgpack:"packets,omitempty"`
}

// ClusterBus transports messages between ClusterServer instances. Subscribe
// must deliver broadcasts and messages addressed to nodeID. The returned
// function removes that subscription without closing a shared bus. Publish
// implementations must observe ctx.Done() and return promptly. They must also
// enqueue delivery instead of invoking subscription listeners synchronously,
// while preserving FIFO order per subscription for ordered Publish calls. A
// listener is allowed to close any ClusterServer, including the publisher,
// without re-entering its in-flight publication barrier. Shutdown can cancel
// an in-flight publish, but Go cannot forcibly stop a bus implementation which
// ignores its Context.
type ClusterBus interface {
	Subscribe(nodeID string, listener func(*ClusterMessage)) (unsubscribe func(), err error)
	Publish(context.Context, *ClusterMessage) error
}

var ErrClusterBusClosed = errors.New("engine: cluster bus is closed")

// MemoryClusterBus is a concurrency-safe, process-local implementation used
// for multi-listener deployments and deterministic tests. It still exercises
// the full cluster protocol; it does not perform sticky owner routing.
type MemoryClusterBus struct {
	mu          sync.RWMutex
	closed      bool
	nextID      uint64
	subscribers map[uint64]*memoryClusterSubscriber
}

type memoryClusterSubscriber struct {
	nodeID   string
	listener func(*ClusterMessage)

	mu      sync.Mutex
	cond    *sync.Cond
	pending []*ClusterMessage
	stopped bool
	done    chan struct{}
}

func NewMemoryClusterBus() *MemoryClusterBus {
	return &MemoryClusterBus{subscribers: make(map[uint64]*memoryClusterSubscriber)}
}

func newMemoryClusterSubscriber(
	nodeID string,
	listener func(*ClusterMessage),
) *memoryClusterSubscriber {
	subscriber := &memoryClusterSubscriber{
		nodeID:   nodeID,
		listener: listener,
		pending:  make([]*ClusterMessage, 0),
		done:     make(chan struct{}),
	}
	subscriber.cond = sync.NewCond(&subscriber.mu)
	go subscriber.run()
	return subscriber
}

func (s *memoryClusterSubscriber) enqueue(message *ClusterMessage) bool {
	if message == nil {
		return false
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.pending = append(s.pending, message)
	s.cond.Signal()
	s.mu.Unlock()
	return true
}

func (s *memoryClusterSubscriber) stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		clear(s.pending)
		s.pending = nil
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *memoryClusterSubscriber) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for len(s.pending) == 0 && !s.stopped {
			s.cond.Wait()
		}
		if s.stopped {
			clear(s.pending)
			s.pending = nil
			s.mu.Unlock()
			return
		}
		message := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		if len(s.pending) == 0 {
			s.pending = nil
		}
		s.mu.Unlock()

		s.deliver(message)
	}
}

func (s *memoryClusterSubscriber) deliver(message *ClusterMessage) {
	defer func() {
		if recovered := recover(); recovered != nil {
			serverLog.Errorf("cluster listener panic recovered: %v", recovered)
		}
	}()
	s.listener(message)
}

func (b *MemoryClusterBus) Subscribe(nodeID string, listener func(*ClusterMessage)) (func(), error) {
	if listener == nil {
		return nil, errors.New("engine: cluster listener is nil")
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClusterBusClosed
	}
	b.nextID++
	id := b.nextID
	subscriber := newMemoryClusterSubscriber(nodeID, listener)
	b.subscribers[id] = subscriber
	b.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			current := b.subscribers[id]
			if current == subscriber {
				delete(b.subscribers, id)
			}
			b.mu.Unlock()
			subscriber.stop()
		})
	}, nil
}

func (b *MemoryClusterBus) Publish(ctx context.Context, message *ClusterMessage) error {
	if message == nil {
		return errors.New("engine: cluster message is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrClusterBusClosed
	}
	subscribers := make([]*memoryClusterSubscriber, 0, len(b.subscribers))
	for _, subscriber := range b.subscribers {
		if message.RecipientID == "" || message.RecipientID == subscriber.nodeID {
			subscribers = append(subscribers, subscriber)
		}
	}
	b.mu.RUnlock()

	cloned := cloneClusterMessage(message)
	for _, subscriber := range subscribers {
		if err := ctx.Err(); err != nil {
			return err
		}
		delivered := cloneClusterMessage(&cloned)
		subscriber.enqueue(&delivered)
	}
	return nil
}

// Close prevents new publications and removes every subscription.
func (b *MemoryClusterBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subscribers := make([]*memoryClusterSubscriber, 0, len(b.subscribers))
	for _, subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	clear(b.subscribers)
	b.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.stop()
	}
	return nil
}

func cloneClusterMessage(source *ClusterMessage) ClusterMessage {
	message := *source
	if message.Packet != nil {
		packetCopy := *message.Packet
		packetCopy.Data = append([]byte(nil), message.Packet.Data...)
		message.Packet = &packetCopy
	}
	if message.Packets != nil {
		message.Packets = append([]ClusterPacket(nil), message.Packets...)
		for index := range message.Packets {
			message.Packets[index].Data = append([]byte(nil), message.Packets[index].Data...)
		}
	}
	return message
}
