package socketio

import (
	"context"
	"log/slog"
	"sync"
)

type listenerEntry struct {
	id   uint64
	fn   Listener
	once bool
}

type eventHub struct {
	mu        sync.Mutex
	nextID    uint64
	listeners map[string][]*listenerEntry
	attached  map[string]bool
	attach    func(string, func(...any)) error
	ctx       func() context.Context
	transform func(string, []any) []any
	logger    *slog.Logger
}

func newEventHub(
	attach func(string, func(...any)) error,
	ctx func() context.Context,
	transform func(string, []any) []any,
	logger *slog.Logger,
) *eventHub {
	if logger == nil {
		logger = slog.Default()
	}
	return &eventHub{
		listeners: make(map[string][]*listenerEntry),
		attached:  make(map[string]bool),
		attach:    attach,
		ctx:       ctx,
		transform: transform,
		logger:    logger,
	}
}

func (h *eventHub) On(event string, fn Listener) Subscription {
	return h.add(event, fn, false)
}

func (h *eventHub) Once(event string, fn Listener) Subscription {
	return h.add(event, fn, true)
}

func (h *eventHub) add(event string, fn Listener, once bool) Subscription {
	if h == nil || event == "" || fn == nil || h.attach == nil {
		return closedSubscription{}
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.attached[event] {
		if err := h.attach(event, func(args ...any) {
			h.dispatch(event, args)
		}); err != nil {
			h.logger.Error("socket.io v4: attach listener bridge", "event", event, "error", err)
			return closedSubscription{}
		}
		h.attached[event] = true
	}

	h.nextID++
	entry := &listenerEntry{id: h.nextID, fn: fn, once: once}
	h.listeners[event] = append(h.listeners[event], entry)
	return &subscription{hub: h, event: event, id: entry.id}
}

func (h *eventHub) dispatch(event string, args []any) {
	h.mu.Lock()
	entries := append([]*listenerEntry(nil), h.listeners[event]...)
	h.mu.Unlock()

	if h.transform != nil {
		args = h.transform(event, args)
	}

	ctx := context.Background()
	if h.ctx != nil {
		if current := h.ctx(); current != nil {
			ctx = current
		}
	}

	for _, entry := range entries {
		if entry == nil || entry.fn == nil {
			continue
		}
		if entry.once {
			h.remove(event, entry.id)
		}
		if err := entry.fn(ctx, args...); err != nil {
			h.logger.Error("socket.io v4: event listener returned error", "event", event, "error", err)
		}
	}
}

func (h *eventHub) remove(event string, id uint64) {
	if h == nil || id == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	entries := h.listeners[event]
	for i, entry := range entries {
		if entry != nil && entry.id == id {
			copy(entries[i:], entries[i+1:])
			entries[len(entries)-1] = nil
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(h.listeners, event)
	} else {
		h.listeners[event] = entries
	}
}

func (h *eventHub) RemoveAll(event string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if event == "" {
		clear(h.listeners)
		return
	}
	delete(h.listeners, event)
}

type subscription struct {
	once  sync.Once
	hub   *eventHub
	event string
	id    uint64
}

func (s *subscription) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.hub != nil {
			s.hub.remove(s.event, s.id)
		}
	})
	return nil
}

type closedSubscription struct{}

func (closedSubscription) Close() error { return nil }
