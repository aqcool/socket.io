package eventstore

import (
	"context"
	"os"
	"testing"
	"time"

	socketpostgres "github.com/aqcool/socket.io/adapters/postgres/v4"
	"github.com/aqcool/socket.io/reliability/v4"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresEventStoreContract(t *testing.T) {
	uri := os.Getenv("SOCKET_IO_POSTGRES_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_POSTGRES_TEST_URI is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	prefix := "socket_io_reliability_test_" + time.Now().Format("150405000000000")
	store, err := New(ctx, socketpostgres.NewPostgresClient(ctx, pool), prefix)
	if err != nil {
		t.Fatal(err)
	}
	exerciseStore(t, ctx, store)
}

func exerciseStore(t *testing.T, ctx context.Context, store reliability.EventStore) {
	t.Helper()
	target := reliability.Target{Kind: reliability.TargetRoom, Namespace: "/chat", ID: "room-1"}
	event := &reliability.Event{ID: "e1", Name: "notice", CreatedAt: time.Now(), Size: 5}
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
