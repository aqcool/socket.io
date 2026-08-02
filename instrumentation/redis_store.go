package instrumentation

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultRedisStorePrefix matches @socket.io/admin-ui@0.5.1.
	DefaultRedisStorePrefix = "socket.io-admin"
	// DefaultRedisStoreSessionDuration matches the official 86400-second TTL.
	DefaultRedisStoreSessionDuration = 24 * time.Hour
)

var ErrInvalidRedisStoreOptions = errors.New("instrumentation: invalid Redis store options")

// RedisStoreOptions configures the Admin UI authentication session store.
type RedisStoreOptions struct {
	Prefix          string
	SessionDuration time.Duration
	Context         context.Context
}

// RedisStore is the Go equivalent of the RedisStore exported by
// @socket.io/admin-ui. It supports standalone, Sentinel and Cluster clients
// through the go-redis Cmdable interface.
type RedisStore struct {
	client          redis.Cmdable
	context         context.Context
	prefix          string
	sessionDuration time.Duration
}

// NewRedisStore creates a Redis-backed Admin UI session store.
func NewRedisStore(client redis.Cmdable, options *RedisStoreOptions) (*RedisStore, error) {
	if client == nil || isNilRedisClient(client) {
		return nil, ErrInvalidRedisStoreOptions
	}

	prefix := DefaultRedisStorePrefix
	sessionDuration := DefaultRedisStoreSessionDuration
	ctx := context.Background()
	if options != nil {
		if options.Prefix != "" {
			prefix = options.Prefix
		}
		if options.SessionDuration < 0 {
			return nil, ErrInvalidRedisStoreOptions
		}
		if options.SessionDuration > 0 {
			sessionDuration = options.SessionDuration
		}
		if options.Context != nil {
			ctx = options.Context
		}
	}

	return &RedisStore{
		client:          client,
		context:         ctx,
		prefix:          prefix,
		sessionDuration: sessionDuration,
	}, nil
}

func isNilRedisClient(client redis.Cmdable) bool {
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *RedisStore) key(sessionID string) string {
	return s.prefix + "#" + sessionID
}

// DoesSessionExist reports whether the official Admin UI session key exists.
func (s *RedisStore) DoesSessionExist(sessionID string) (bool, error) {
	count, err := s.client.Exists(s.context, s.key(sessionID)).Result()
	return count == 1, err
}

// SaveSession stores a session atomically with its expiry.
func (s *RedisStore) SaveSession(sessionID string) error {
	return s.client.Set(s.context, s.key(sessionID), true, s.sessionDuration).Err()
}
