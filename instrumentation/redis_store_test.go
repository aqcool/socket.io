package instrumentation

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestOfficialAdminUI051InMemoryStore(t *testing.T) {
	store := NewInMemorySessionStore()

	exists, err := store.DoesSessionExist("123")
	if err != nil || exists {
		t.Fatalf("new store lookup = %t, %v; want false, nil", exists, err)
	}
	if err = store.SaveSession("123"); err != nil {
		t.Fatal(err)
	}
	exists, err = store.DoesSessionExist("123")
	if err != nil || !exists {
		t.Fatalf("saved store lookup = %t, %v; want true, nil", exists, err)
	}
	exists, err = store.DoesSessionExist("456")
	if err != nil || exists {
		t.Fatalf("unknown store lookup = %t, %v; want false, nil", exists, err)
	}
}

func TestRedisStoreOptionsAndValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	store, err := NewRedisStore(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	if store.prefix != DefaultRedisStorePrefix {
		t.Fatalf("prefix = %q, want %q", store.prefix, DefaultRedisStorePrefix)
	}
	if store.sessionDuration != DefaultRedisStoreSessionDuration {
		t.Fatalf("duration = %s, want %s", store.sessionDuration, DefaultRedisStoreSessionDuration)
	}
	if key := store.key("abc"); key != "socket.io-admin#abc" {
		t.Fatalf("key = %q", key)
	}

	custom, err := NewRedisStore(client, &RedisStoreOptions{
		Prefix:          "custom-admin",
		SessionDuration: 90 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if custom.prefix != "custom-admin" || custom.sessionDuration != 90*time.Second ||
		custom.key("abc") != "custom-admin#abc" {
		t.Fatalf("unexpected custom store: %#v", custom)
	}

	if _, err = NewRedisStore(nil, nil); !errors.Is(err, ErrInvalidRedisStoreOptions) {
		t.Fatalf("nil client error = %v", err)
	}
	var typedNil *redis.Client
	if _, err = NewRedisStore(typedNil, nil); !errors.Is(err, ErrInvalidRedisStoreOptions) {
		t.Fatalf("typed nil client error = %v", err)
	}
	if _, err = NewRedisStore(client, &RedisStoreOptions{SessionDuration: -time.Second}); !errors.Is(err, ErrInvalidRedisStoreOptions) {
		t.Fatalf("negative duration error = %v", err)
	}
}

func TestOfficialAdminUI051RedisStore(t *testing.T) {
	if os.Getenv(adminUIOfficialInteropEnv) != "1" {
		t.Skip("set SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP=1 with a dedicated Redis instance")
	}
	address := os.Getenv("SOCKET_IO_ADMIN_UI_REDIS_ADDR")
	if address == "" {
		address = os.Getenv("SOCKET_IO_REDIS_TEST_ADDR")
	}
	if address == "" {
		t.Skip("SOCKET_IO_ADMIN_UI_REDIS_ADDR is not set")
	}
	password := os.Getenv("SOCKET_IO_ADMIN_UI_REDIS_PASSWORD")
	if password == "" {
		password = os.Getenv("SOCKET_IO_REDIS_TEST_PASSWORD")
	}

	client := redis.NewClient(&redis.Options{Addr: address, Password: password})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("Redis at %s is unavailable: %v", address, err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	defaultStore, err := NewRedisStore(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	defaultID := "official-default-" + suffix
	defaultKey := defaultStore.key(defaultID)
	t.Cleanup(func() { _ = client.Del(t.Context(), defaultKey).Err() })
	assertRedisSessionStore(t, defaultStore, defaultID)
	defaultTTL, err := client.TTL(t.Context(), defaultKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if defaultTTL <= 23*time.Hour || defaultTTL > DefaultRedisStoreSessionDuration {
		t.Fatalf("default TTL = %s, want approximately 24h", defaultTTL)
	}

	customStore, err := NewRedisStore(client, &RedisStoreOptions{
		Prefix:          "socket.io-admin-test-" + suffix,
		SessionDuration: 90 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	customID := "official-custom"
	customKey := customStore.key(customID)
	t.Cleanup(func() { _ = client.Del(t.Context(), customKey).Err() })
	assertRedisSessionStore(t, customStore, customID)
	customTTL, err := client.TTL(t.Context(), customKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if customTTL <= 0 || customTTL > 90*time.Second {
		t.Fatalf("custom TTL = %s, want (0, 90s]", customTTL)
	}
}

func assertRedisSessionStore(t *testing.T, store *RedisStore, sessionID string) {
	t.Helper()
	exists, err := store.DoesSessionExist(sessionID)
	if err != nil || exists {
		t.Fatalf("initial lookup = %t, %v; want false, nil", exists, err)
	}
	if err = store.SaveSession(sessionID); err != nil {
		t.Fatal(err)
	}
	exists, err = store.DoesSessionExist(sessionID)
	if err != nil || !exists {
		t.Fatalf("saved lookup = %t, %v; want true, nil", exists, err)
	}
}
