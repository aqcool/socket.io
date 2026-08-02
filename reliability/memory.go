package reliability

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type MemoryStore struct {
	mu      sync.RWMutex
	seq     atomic.Uint64
	events  []Event
	acks    map[string]Offset
	inbound map[string]time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{acks: make(map[string]Offset), inbound: make(map[string]time.Time)}
}

func (s *MemoryStore) Append(_ context.Context, target Target, event *Event) (Offset, error) {
	offset := Offset(strconv.FormatUint(s.seq.Add(1), 10))
	event.Target, event.Offset = target, offset
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}
	s.mu.Lock()
	s.events = append(s.events, *event)
	s.mu.Unlock()
	return offset, nil
}

func (s *MemoryStore) Replay(_ context.Context, target Target, after Offset, limit int) ([]Event, error) {
	afterValue, _ := strconv.ParseUint(string(after), 10, 64)
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Event, 0)
	for i := range s.events {
		event := &s.events[i]
		value, _ := strconv.ParseUint(string(event.Offset), 10, 64)
		if value <= afterValue || event.Target != target ||
			(!event.ExpiresAt.IsZero() && !event.ExpiresAt.After(now)) {
			continue
		}
		result = append(result, *event)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryStore) Ack(_ context.Context, clientID string, offset Offset) error {
	s.mu.Lock()
	current, _ := strconv.ParseUint(string(s.acks[clientID]), 10, 64)
	next, _ := strconv.ParseUint(string(offset), 10, 64)
	if next > current {
		s.acks[clientID] = offset
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) LastAck(_ context.Context, clientID string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.acks[clientID], nil
}

func (s *MemoryStore) MarkInbound(_ context.Context, clientID, eventID string, expiresAt time.Time) (bool, error) {
	key, now := clientID+"\x00"+eventID, time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if expiry, ok := s.inbound[key]; ok && (expiry.IsZero() || expiry.After(now)) {
		return false, nil
	}
	s.inbound[key] = expiresAt
	return true, nil
}

func (s *MemoryStore) Prune(_ context.Context, limits Limits) error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered, bytes := s.events[:0], int64(0)
	for i := range s.events {
		event := &s.events[i]
		expired := !event.ExpiresAt.IsZero() && !event.ExpiresAt.After(now)
		if limits.Retention > 0 && event.CreatedAt.Add(limits.Retention).Before(now) {
			expired = true
		}
		if !expired {
			filtered, bytes = append(filtered, *event), bytes+event.Size
		}
	}
	s.events = filtered
	for key, expiry := range s.inbound {
		if !expiry.IsZero() && !expiry.After(now) {
			delete(s.inbound, key)
		}
	}
	for (limits.MaxEvents > 0 && int64(len(s.events)) > limits.MaxEvents) ||
		(limits.MaxBytes > 0 && bytes > limits.MaxBytes && len(s.events) > 0) {
		bytes -= s.events[0].Size
		s.events = s.events[1:]
	}
	return nil
}
