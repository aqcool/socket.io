package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	socketpostgres "github.com/aqcool/socket.io/adapters/postgres/v3"
	"github.com/aqcool/socket.io/reliability/v3"
	"github.com/jackc/pgx/v5"
)

type Store struct {
	client *socketpostgres.PostgresClient
	events string
	acks   string
	dedup  string
	prefix string
}

func New(ctx context.Context, client *socketpostgres.PostgresClient, prefix string) (*Store, error) {
	if client == nil || client.Pool == nil {
		return nil, errors.New("postgres eventstore: client is required")
	}
	if prefix == "" {
		prefix = "socket_io_reliability"
	}
	store := &Store{
		client: client,
		events: pgx.Identifier{prefix + "_events"}.Sanitize(),
		acks:   pgx.Identifier{prefix + "_acks"}.Sanitize(),
		dedup:  pgx.Identifier{prefix + "_dedup"}.Sanitize(),
		prefix: prefix,
	}
	if err := store.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id BIGSERIAL PRIMARY KEY,
  target_kind TEXT NOT NULL,
  namespace TEXT NOT NULL,
  target_id TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL,
  size_bytes BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS %s ON %s (target_kind, namespace, target_id, id);
CREATE INDEX IF NOT EXISTS %s ON %s (expires_at);
CREATE TABLE IF NOT EXISTS %s (
  client_id TEXT PRIMARY KEY,
  offset_value BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS %s (
  client_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  PRIMARY KEY (client_id, event_id)
);`,
		s.events,
		pgx.Identifier{s.raw("events") + "_target_idx"}.Sanitize(), s.events,
		pgx.Identifier{s.raw("events") + "_expires_idx"}.Sanitize(), s.events,
		s.acks, s.dedup)
	_, err := s.client.Pool.Exec(ctx, query)
	return err
}

func (s *Store) Append(ctx context.Context, target reliability.Target, event *reliability.Event) (reliability.Offset, error) {
	event.Target = target
	payload, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	var expiresAt any
	if !event.ExpiresAt.IsZero() {
		expiresAt = event.ExpiresAt
	}
	query := fmt.Sprintf(`INSERT INTO %s
(target_kind, namespace, target_id, payload, size_bytes, created_at, expires_at)
VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`, s.events)
	var id int64
	err = s.client.Pool.QueryRow(ctx, query, target.Kind, target.Namespace, target.ID, payload,
		event.Size, event.CreatedAt, expiresAt).Scan(&id)
	event.Offset = reliability.Offset(strconv.FormatInt(id, 10))
	return event.Offset, err
}

func (s *Store) Replay(ctx context.Context, target reliability.Target, after reliability.Offset, limit int) ([]reliability.Event, error) {
	afterID, err := parseOffset(after)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 500
	}
	query := fmt.Sprintf(`SELECT id,payload FROM %s
WHERE target_kind=$1 AND namespace=$2 AND target_id=$3 AND id>$4
AND (expires_at IS NULL OR expires_at>NOW()) ORDER BY id LIMIT $5`, s.events)
	rows, err := s.client.Pool.Query(ctx, query, target.Kind, target.Namespace, target.ID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]reliability.Event, 0)
	for rows.Next() {
		var id int64
		var payload []byte
		if err = rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		var event reliability.Event
		if err = json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		event.Offset = reliability.Offset(strconv.FormatInt(id, 10))
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) Ack(ctx context.Context, clientID string, offset reliability.Offset) error {
	value, err := parseOffset(offset)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO %s (client_id,offset_value) VALUES ($1,$2)
ON CONFLICT (client_id) DO UPDATE SET offset_value=GREATEST(%s.offset_value,EXCLUDED.offset_value)`, s.acks, s.acks)
	_, err = s.client.Pool.Exec(ctx, query, clientID, value)
	return err
}

func (s *Store) LastAck(ctx context.Context, clientID string) (reliability.Offset, error) {
	query := fmt.Sprintf(`SELECT offset_value FROM %s WHERE client_id=$1`, s.acks)
	var value int64
	err := s.client.Pool.QueryRow(ctx, query, clientID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return reliability.Offset(strconv.FormatInt(value, 10)), err
}

func (s *Store) MarkInbound(ctx context.Context, clientID, eventID string, expiresAt time.Time) (bool, error) {
	var expiry any
	if !expiresAt.IsZero() {
		expiry = expiresAt
	}
	query := fmt.Sprintf(`INSERT INTO %s (client_id,event_id,expires_at) VALUES ($1,$2,$3)
ON CONFLICT DO NOTHING`, s.dedup)
	tag, err := s.client.Pool.Exec(ctx, query, clientID, eventID, expiry)
	return tag.RowsAffected() == 1, err
}

func (s *Store) Prune(ctx context.Context, limits reliability.Limits) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE
(expires_at IS NOT NULL AND expires_at<=NOW()) OR ($1::bigint>0 AND created_at < NOW()-($1*INTERVAL '1 millisecond'));
DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at<=NOW()`, s.events, s.dedup)
	if _, err := s.client.Pool.Exec(ctx, query, limits.Retention.Milliseconds()); err != nil {
		return err
	}
	if limits.MaxEvents > 0 {
		query = fmt.Sprintf(`DELETE FROM %s WHERE id IN
(SELECT id FROM %s ORDER BY id DESC OFFSET $1)`, s.events, s.events)
		if _, err := s.client.Pool.Exec(ctx, query, limits.MaxEvents); err != nil {
			return err
		}
	}
	if limits.MaxBytes > 0 {
		query = fmt.Sprintf(`WITH ranked AS (
SELECT id,SUM(size_bytes) OVER (ORDER BY id DESC) AS kept FROM %s)
DELETE FROM %s WHERE id IN (SELECT id FROM ranked WHERE kept>$1)`, s.events, s.events)
		_, err := s.client.Pool.Exec(ctx, query, limits.MaxBytes)
		return err
	}
	return nil
}

func parseOffset(offset reliability.Offset) (int64, error) {
	if offset == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(string(offset), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("postgres eventstore: invalid offset %q: %w", offset, err)
	}
	return value, nil
}

func (s *Store) raw(suffix string) string {
	return s.prefix + "_" + suffix
}

var _ reliability.EventStore = (*Store)(nil)
