// Package postgres provides PostgreSQL client wrapper for Socket.IO PostgreSQL adapter.
// This package offers a unified interface for PostgreSQL operations with event handling support
// using LISTEN/NOTIFY for pub/sub communication.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresClient wraps a pgxpool.Pool and provides context management
// and event emitting capabilities for the Socket.IO PostgreSQL adapter.
//
// The client supports a separate listener connection for LISTEN/NOTIFY operations.
// The Pool is used for write operations (pg_notify, INSERT, DELETE, etc.)
// and the Listener connection is used for LISTEN operations.
//
// The client supports error event emission, which allows higher-level components
// to handle PostgreSQL-related errors gracefully.
type PostgresClient struct {
	types.EventEmitter

	// Pool is the connection pool used for write operations
	// (pg_notify, INSERT, DELETE, SELECT, etc.).
	Pool *pgxpool.Pool

	// Context is the context used for PostgreSQL operations.
	// This context controls the lifecycle of subscriptions and operations.
	Context context.Context

	// listenerConn is a dedicated connection for LISTEN operations.
	// It is lazily acquired from the pool.
	listenerConn *pgx.Conn
	listenerMu   sync.Mutex
	listenerOpMu sync.Mutex
}

// NewPostgresClient creates a new PostgresClient with the given context and connection pool.
//
// Parameters:
//   - ctx: The context that controls the lifecycle of PostgreSQL operations.
//     When canceled, all subscriptions and pending operations will be terminated.
//   - pool: A pgxpool.Pool instance that handles the actual PostgreSQL communication.
//
// Returns:
//   - A pointer to the initialized PostgresClient instance.
//
// Example:
//
//	pool, _ := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/db")
//	pgClient := NewPostgresClient(context.Background(), pool)
func NewPostgresClient(ctx context.Context, pool *pgxpool.Pool) *PostgresClient {
	if ctx == nil {
		ctx = context.Background()
	}

	return &PostgresClient{
		EventEmitter: types.NewEventEmitter(),
		Pool:         pool,
		Context:      ctx,
	}
}

// getListenerConn returns a dedicated connection for LISTEN/NOTIFY operations.
// The connection is lazily acquired from the pool on first call and reused thereafter.
// This is thread-safe.
func (c *PostgresClient) getListenerConn() (*pgx.Conn, error) {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()

	if c.listenerConn != nil {
		return c.listenerConn, nil
	}

	conn, err := pgx.Connect(c.Context, c.Pool.Config().ConnConfig.ConnString())
	if err != nil {
		return nil, fmt.Errorf("failed to acquire listener connection: %w", err)
	}

	c.listenerConn = conn
	return conn, nil
}

// Listen subscribes to the specified PostgreSQL notification channels using LISTEN.
// A dedicated connection is used to ensure notifications are not lost.
//
// Parameters:
//   - ctx: The context for the LISTEN operation.
//   - channels: One or more channel names to listen on.
func (c *PostgresClient) Listen(ctx context.Context, channels ...string) error {
	c.listenerOpMu.Lock()
	defer c.listenerOpMu.Unlock()

	conn, err := c.getListenerConn()
	if err != nil {
		return err
	}

	for _, channel := range channels {
		if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanitize())); err != nil {
			return fmt.Errorf("failed to LISTEN on channel %q: %w", channel, err)
		}
	}

	return nil
}

// Unlisten unsubscribes from the specified PostgreSQL notification channels using UNLISTEN.
//
// Parameters:
//   - ctx: The context for the UNLISTEN operation.
//   - channels: One or more channel names to unlisten from.
func (c *PostgresClient) Unlisten(ctx context.Context, channels ...string) error {
	c.listenerOpMu.Lock()
	defer c.listenerOpMu.Unlock()

	c.listenerMu.Lock()
	conn := c.listenerConn
	c.listenerMu.Unlock()

	if conn == nil {
		return nil
	}

	for _, channel := range channels {
		if _, err := conn.Exec(ctx, fmt.Sprintf("UNLISTEN %s", pgx.Identifier{channel}.Sanitize())); err != nil {
			return fmt.Errorf("failed to UNLISTEN on channel %q: %w", channel, err)
		}
	}

	return nil
}

// WaitForNotification waits for a notification on the listener connection.
// This method blocks until a notification is received or the context is canceled.
//
// Returns the received notification or an error if the wait was interrupted.
func (c *PostgresClient) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	conn, err := c.getListenerConn()
	if err != nil {
		return nil, err
	}

	for {
		c.listenerOpMu.Lock()
		waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		notification, waitErr := conn.WaitForNotification(waitCtx)
		cancel()
		c.listenerOpMu.Unlock()

		if errors.Is(waitErr, context.DeadlineExceeded) && ctx.Err() == nil {
			continue
		}
		return notification, waitErr
	}
}

// Notify sends a NOTIFY on the specified channel with the given payload.
// Uses pg_notify() to send the notification through the connection pool.
//
// Parameters:
//   - ctx: The context for the notification operation.
//   - channel: The notification channel name.
//   - payload: The notification payload string.
func (c *PostgresClient) Notify(ctx context.Context, channel, payload string) error {
	_, err := c.Pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	return err
}

// EnsureTable creates the attachment table if it does not exist.
// This table is used to store large payloads that exceed the pg_notify limit.
//
// Parameters:
//   - ctx: The context for the operation.
//   - tableName: The name of the table to create.
func (c *PostgresClient) EnsureTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id bigserial UNIQUE, created_at timestamptz DEFAULT NOW(), payload bytea)",
		pgx.Identifier{tableName}.Sanitize(),
	)
	_, err := c.Pool.Exec(ctx, query)
	return err
}

// InsertAttachment inserts a payload into the attachment table and returns its generated ID.
//
// Parameters:
//   - ctx: The context for the operation.
//   - tableName: The name of the attachment table.
//   - payload: The binary payload to store.
func (c *PostgresClient) InsertAttachment(ctx context.Context, tableName string, payload []byte) (int64, error) {
	var id int64
	query := fmt.Sprintf("INSERT INTO %s (payload) VALUES ($1) RETURNING id", pgx.Identifier{tableName}.Sanitize())
	err := c.Pool.QueryRow(ctx, query, payload).Scan(&id)
	return id, err
}

// GetAttachment retrieves a payload from the attachment table by ID.
//
// Parameters:
//   - ctx: The context for the operation.
//   - tableName: The name of the attachment table.
//   - id: The attachment ID.
func (c *PostgresClient) GetAttachment(ctx context.Context, tableName string, id int64) ([]byte, error) {
	var payload []byte
	query := fmt.Sprintf("SELECT payload FROM %s WHERE id = $1", pgx.Identifier{tableName}.Sanitize())
	err := c.Pool.QueryRow(ctx, query, id).Scan(&payload)
	return payload, err
}

// CleanupAttachments deletes attachments older than the specified interval.
//
// Parameters:
//   - ctx: The context for the operation.
//   - tableName: The name of the attachment table.
//   - cleanupIntervalMs: The age threshold in milliseconds; attachments older than this are deleted.
func (c *PostgresClient) CleanupAttachments(ctx context.Context, tableName string, cleanupIntervalMs int64) error {
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE created_at < now() - interval '%d milliseconds'",
		pgx.Identifier{tableName}.Sanitize(),
		cleanupIntervalMs,
	)
	_, err := c.Pool.Exec(ctx, query)
	return err
}

// EnsureRecoveryTables creates the durable event log and session store used
// for connection state recovery.
func (c *PostgresClient) EnsureRecoveryTables(ctx context.Context, eventTable, sessionTable string) error {
	eventIdentifier := pgx.Identifier{eventTable}.Sanitize()
	sessionIdentifier := pgx.Identifier{sessionTable}.Sanitize()
	queries := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigserial PRIMARY KEY, namespace text NOT NULL, payload bytea NOT NULL, expires_at timestamptz NOT NULL)", eventIdentifier),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (namespace, id)", pgx.Identifier{eventTable + "_namespace_id_idx"}.Sanitize(), eventIdentifier),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (expires_at)", pgx.Identifier{eventTable + "_expires_at_idx"}.Sanitize(), eventIdentifier),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (namespace text NOT NULL, pid text NOT NULL, payload bytea NOT NULL, expires_at timestamptz NOT NULL, PRIMARY KEY (namespace, pid))", sessionIdentifier),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (expires_at)", pgx.Identifier{sessionTable + "_expires_at_idx"}.Sanitize(), sessionIdentifier),
	}
	for _, query := range queries {
		if _, err := c.Pool.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func (c *PostgresClient) InsertRecoveryEvent(ctx context.Context, tableName, namespace string, payload []byte, expiresAt time.Time) (int64, error) {
	var id int64
	query := fmt.Sprintf("INSERT INTO %s (namespace, payload, expires_at) VALUES ($1, $2, $3) RETURNING id", pgx.Identifier{tableName}.Sanitize())
	err := c.Pool.QueryRow(ctx, query, namespace, payload, expiresAt).Scan(&id)
	return id, err
}

func (c *PostgresClient) DeleteRecoveryEvent(ctx context.Context, tableName string, id int64) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", pgx.Identifier{tableName}.Sanitize())
	_, err := c.Pool.Exec(ctx, query, id)
	return err
}

func (c *PostgresClient) RecoveryOffsetExists(ctx context.Context, tableName, namespace string, id int64) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1 AND namespace = $2 AND expires_at > now())", pgx.Identifier{tableName}.Sanitize())
	err := c.Pool.QueryRow(ctx, query, id, namespace).Scan(&exists)
	return exists, err
}

func (c *PostgresClient) RecoveryEventsAfter(ctx context.Context, tableName, namespace string, id int64) ([]RecoveryEvent, error) {
	query := fmt.Sprintf("SELECT id, payload FROM %s WHERE namespace = $1 AND id > $2 AND expires_at > now() ORDER BY id", pgx.Identifier{tableName}.Sanitize())
	rows, err := c.Pool.Query(ctx, query, namespace, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]RecoveryEvent, 0)
	for rows.Next() {
		var event RecoveryEvent
		if err := rows.Scan(&event.ID, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (c *PostgresClient) UpsertRecoverySession(ctx context.Context, tableName, namespace, pid string, payload []byte, expiresAt time.Time) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (namespace, pid, payload, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (namespace, pid) DO UPDATE SET payload = EXCLUDED.payload, expires_at = EXCLUDED.expires_at",
		pgx.Identifier{tableName}.Sanitize(),
	)
	_, err := c.Pool.Exec(ctx, query, namespace, pid, payload, expiresAt)
	return err
}

func (c *PostgresClient) DeleteRecoverySession(ctx context.Context, tableName, namespace, pid string) ([]byte, error) {
	var payload []byte
	query := fmt.Sprintf("DELETE FROM %s WHERE namespace = $1 AND pid = $2 AND expires_at > now() RETURNING payload", pgx.Identifier{tableName}.Sanitize())
	err := c.Pool.QueryRow(ctx, query, namespace, pid).Scan(&payload)
	return payload, err
}

func (c *PostgresClient) CleanupRecovery(ctx context.Context, eventTable, sessionTable string) error {
	for _, tableName := range []string{eventTable, sessionTable} {
		query := fmt.Sprintf("DELETE FROM %s WHERE expires_at <= now()", pgx.Identifier{tableName}.Sanitize())
		if _, err := c.Pool.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the listener connection if it was acquired.
func (c *PostgresClient) Close() {
	c.listenerOpMu.Lock()
	defer c.listenerOpMu.Unlock()

	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()

	if c.listenerConn != nil {
		_ = c.listenerConn.Close(c.Context)
		c.listenerConn = nil
	}
}
