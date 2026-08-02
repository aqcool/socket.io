package broker

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
)

// MemoryBroker is a deterministic broker for tests and single-process use.
type MemoryBroker struct {
	sequence atomic.Uint64
	mu       sync.RWMutex
	handlers map[string]map[uint64]Handler
}

func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{handlers: make(map[string]map[uint64]Handler)}
}

func (b *MemoryBroker) Publish(ctx context.Context, subject string, payload []byte) (string, error) {
	id := b.sequence.Add(1)
	b.mu.RLock()
	handlers := make([]Handler, 0, len(b.handlers[subject]))
	for _, handler := range b.handlers[subject] {
		handlers = append(handlers, handler)
	}
	b.mu.RUnlock()
	message := Message{ID: strconv.FormatUint(id, 10), Data: append([]byte(nil), payload...)}
	for _, handler := range handlers {
		current := handler
		go func() { _ = current(ctx, message) }()
	}
	return message.ID, nil
}

func (b *MemoryBroker) Subscribe(_ context.Context, subject string, handler Handler) (Subscription, error) {
	id := b.sequence.Add(1)
	b.mu.Lock()
	if b.handlers[subject] == nil {
		b.handlers[subject] = make(map[uint64]Handler)
	}
	b.handlers[subject][id] = handler
	b.mu.Unlock()
	return &memorySubscription{close: func() {
		b.mu.Lock()
		delete(b.handlers[subject], id)
		b.mu.Unlock()
	}}, nil
}

type memorySubscription struct {
	once  sync.Once
	close func()
}

func (s *memorySubscription) Close() error {
	s.once.Do(s.close)
	return nil
}
