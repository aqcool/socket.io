package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	socketredis "github.com/aqcool/socket.io/adapters/redis/v3"
	"github.com/aqcool/socket.io/reliability/v3"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	client *socketredis.RedisClient
	prefix string
}

func New(client *socketredis.RedisClient, prefix string) (*Store, error) {
	if client == nil || client.Client == nil {
		return nil, errors.New("redis eventstore: client is required")
	}
	if prefix == "" {
		prefix = "socket.io:reliability"
	}
	return &Store{client: client, prefix: prefix}, nil
}

func (s *Store) Append(ctx context.Context, target reliability.Target, event *reliability.Event) (reliability.Offset, error) {
	if ctx == nil {
		ctx = s.client.Context
	}
	event.Target = target
	payload, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	id, err := s.client.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.eventsKey(),
		Values: map[string]any{"event": payload},
	}).Result()
	if err != nil {
		return "", err
	}
	event.Offset = reliability.Offset(id)
	return event.Offset, nil
}

func (s *Store) Replay(ctx context.Context, target reliability.Target, after reliability.Offset, limit int) ([]reliability.Event, error) {
	if ctx == nil {
		ctx = s.client.Context
	}
	start := "-"
	if after != "" {
		start = "(" + string(after)
	}
	batchSize := int64(500)
	if limit > 0 && int64(limit) > batchSize {
		batchSize = int64(limit)
	}
	result := make([]reliability.Event, 0)
	for {
		messages, err := s.client.Sub().XRangeN(ctx, s.eventsKey(), start, "+", batchSize).Result()
		if err != nil {
			return nil, err
		}
		if len(messages) == 0 {
			return result, nil
		}
		for i := range messages {
			event, decodeErr := decode(messages[i])
			if decodeErr != nil {
				return nil, decodeErr
			}
			if event.Target == target && (event.ExpiresAt.IsZero() || event.ExpiresAt.After(time.Now())) {
				result = append(result, event)
				if limit > 0 && len(result) >= limit {
					return result, nil
				}
			}
		}
		start = "(" + messages[len(messages)-1].ID
		if int64(len(messages)) < batchSize {
			return result, nil
		}
	}
}

func (s *Store) Ack(ctx context.Context, clientID string, offset reliability.Offset) error {
	_, err := s.client.Client.HSet(ctx, s.acksKey(), clientID, string(offset)).Result()
	return err
}

func (s *Store) LastAck(ctx context.Context, clientID string) (reliability.Offset, error) {
	value, err := s.client.Sub().HGet(ctx, s.acksKey(), clientID).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return reliability.Offset(value), err
}

func (s *Store) MarkInbound(ctx context.Context, clientID, eventID string, expiresAt time.Time) (bool, error) {
	ttl := time.Duration(0)
	if !expiresAt.IsZero() {
		ttl = time.Until(expiresAt)
		if ttl <= 0 {
			return false, nil
		}
	}
	return s.client.Client.SetNX(ctx, s.dedupKey(clientID, eventID), "1", ttl).Result()
}

func (s *Store) Prune(ctx context.Context, limits reliability.Limits) error {
	if limits.Retention > 0 {
		minID := strconv.FormatInt(time.Now().Add(-limits.Retention).UnixMilli(), 10) + "-0"
		if err := s.client.Client.XTrimMinID(ctx, s.eventsKey(), minID).Err(); err != nil {
			return err
		}
	}
	if limits.MaxEvents > 0 {
		if err := s.client.Client.XTrimMaxLen(ctx, s.eventsKey(), limits.MaxEvents).Err(); err != nil {
			return err
		}
	}
	if limits.MaxBytes <= 0 {
		return nil
	}
	messages, err := s.client.Sub().XRange(ctx, s.eventsKey(), "-", "+").Result()
	if err != nil {
		return err
	}
	var total int64
	for i := range messages {
		total += valueSize(messages[i].Values["event"])
	}
	for i := range messages {
		if total <= limits.MaxBytes {
			break
		}
		total -= valueSize(messages[i].Values["event"])
		if err = s.client.Client.XDel(ctx, s.eventsKey(), messages[i].ID).Err(); err != nil {
			return err
		}
	}
	return nil
}

func decode(message redis.XMessage) (reliability.Event, error) {
	raw, ok := message.Values["event"]
	if !ok {
		return reliability.Event{}, errors.New("redis eventstore: event field is missing")
	}
	var payload []byte
	switch value := raw.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return reliability.Event{}, fmt.Errorf("redis eventstore: invalid event field %T", raw)
	}
	var event reliability.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return reliability.Event{}, err
	}
	event.Offset = reliability.Offset(message.ID)
	return event, nil
}

func valueSize(value any) int64 {
	switch typed := value.(type) {
	case string:
		return int64(len(typed))
	case []byte:
		return int64(len(typed))
	default:
		return 0
	}
}

func (s *Store) eventsKey() string { return s.prefix + ":events" }
func (s *Store) acksKey() string   { return s.prefix + ":acks" }
func (s *Store) dedupKey(clientID, eventID string) string {
	return s.prefix + ":dedup:" + clientID + ":" + eventID
}

var _ reliability.EventStore = (*Store)(nil)
