package eventstore

import (
	"context"
	"os"
	"testing"
	"time"

	socketredis "github.com/aqcool/socket.io/adapters/redis/v3"
	"github.com/aqcool/socket.io/reliability/v3"
	"github.com/redis/go-redis/v9"
)

func TestRedisEventStoreContract(t *testing.T) {
	address := os.Getenv("SOCKET_IO_REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("SOCKET_IO_REDIS_TEST_ADDR is not set")
	}
	ctx := context.Background()
	raw := redis.NewClient(&redis.Options{Addr: address, Password: "root"})
	t.Cleanup(func() { _ = raw.Close() })
	store, err := New(socketredis.NewRedisClient(ctx, raw), "socket.io:test:reliability:"+time.Now().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	exerciseStore(t, ctx, store)
}

func exerciseStore(t *testing.T, ctx context.Context, store reliability.EventStore) {
	t.Helper()
	target := reliability.Target{Kind: reliability.TargetUser, Namespace: "/", ID: "u1"}
	event := &reliability.Event{ID: "e1", Name: "notice", Args: []any{"hello"}, CreatedAt: time.Now(), Size: 5}
	offset, err := store.Append(ctx, target, event)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Replay(ctx, target, "", 10)
	if err != nil || len(replayed) != 1 || replayed[0].Offset != offset {
		t.Fatalf("replay=%#v err=%v", replayed, err)
	}
	if err = store.Ack(ctx, "c1", offset); err != nil {
		t.Fatal(err)
	}
	if ack, ackErr := store.LastAck(ctx, "c1"); ackErr != nil || ack != offset {
		t.Fatalf("ack=%q err=%v", ack, ackErr)
	}
	fresh, err := store.MarkInbound(ctx, "c1", "request-1", time.Now().Add(time.Minute))
	if err != nil || !fresh {
		t.Fatalf("first dedup fresh=%v err=%v", fresh, err)
	}
	fresh, err = store.MarkInbound(ctx, "c1", "request-1", time.Now().Add(time.Minute))
	if err != nil || fresh {
		t.Fatalf("duplicate fresh=%v err=%v", fresh, err)
	}
}
