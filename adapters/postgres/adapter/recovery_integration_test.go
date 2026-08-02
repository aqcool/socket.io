package adapter

import (
	"fmt"
	"os"
	"testing"
	"time"

	clusteradapter "github.com/aqcool/socket.io/adapters/adapter/v3"
	"github.com/aqcool/socket.io/adapters/postgres/v3"
	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresCrossNodeConnectionStateRecovery(t *testing.T) {
	uri := os.Getenv("SOCKET_IO_POSTGRES_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_POSTGRES_TEST_URI is not set")
	}

	ctx := t.Context()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	tableName := fmt.Sprintf("socket_io_test_%d", time.Now().UnixNano())
	newServer := func() *socket.Server {
		client := postgres.NewPostgresClient(ctx, pool)
		adapterOptions := DefaultPostgresAdapterOptions()
		adapterOptions.SetTableName(tableName)
		serverOptions := socket.DefaultServerOptions()
		recovery := socket.DefaultConnectionStateRecovery()
		recovery.SetMaxDisconnectionDuration(30_000)
		serverOptions.SetConnectionStateRecovery(recovery)
		serverOptions.SetAdapter(&PostgresAdapterBuilder{Postgres: client, Opts: adapterOptions})
		return socket.NewServer(nil, serverOptions)
	}

	server1 := newServer()
	server2 := newServer()
	defer server1.Sockets().Adapter().Close()
	defer server2.Sockets().Adapter().Close()

	source := server1.Sockets().Adapter()
	target := server2.Sockets().Adapter()
	rooms := types.NewSet[socket.Room]("room-1")

	baseline := &parser.Packet{Type: parser.EVENT, Data: []any{"baseline"}}
	source.Broadcast(baseline, &socket.BroadcastOptions{
		Rooms:  types.NewSet[socket.Room](),
		Except: types.NewSet[socket.Room](),
	})
	baselineData := baseline.Data.([]any)
	offset, ok := baselineData[len(baselineData)-1].(clusteradapter.Offset)
	if !ok || offset == "" {
		t.Fatalf("expected PostgreSQL offset, got %#v", baselineData)
	}

	target.PersistSession(&socket.SessionToPersist{
		Sid:   "sid-1",
		Pid:   "pid-1",
		Rooms: rooms,
		Data:  map[string]any{"node": "two"},
	})

	source.Broadcast(&parser.Packet{Type: parser.EVENT, Data: []any{"missed", "included"}}, &socket.BroadcastOptions{
		Rooms:  types.NewSet[socket.Room]("room-1"),
		Except: types.NewSet[socket.Room](),
	})
	source.Broadcast(&parser.Packet{Type: parser.EVENT, Data: []any{"missed", "excluded"}}, &socket.BroadcastOptions{
		Rooms:  types.NewSet[socket.Room]("room-2"),
		Except: types.NewSet[socket.Room](),
	})

	restored, err := target.RestoreSession("pid-1", string(offset))
	if err != nil {
		t.Fatal(err)
	}
	if restored == nil {
		t.Fatal("expected session restored on the second node")
	}
	if restored.Sid != "sid-1" || restored.Pid != "pid-1" || !restored.Rooms.Has("room-1") {
		t.Fatalf("restored session state mismatch: %#v", restored.SessionToPersist)
	}
	if data, ok := restored.Data.(map[string]any); !ok || data["node"] != "two" {
		t.Fatalf("restored session data mismatch: %#v", restored.Data)
	}
	if len(restored.MissedPackets) != 1 {
		t.Fatalf("expected one matching missed packet, got %#v", restored.MissedPackets)
	}
	packetData := restored.MissedPackets[0].([]any)
	if packetData[0] != "missed" || packetData[1] != "included" {
		t.Fatalf("unexpected missed packet: %#v", packetData)
	}
	if recoveredOffset, ok := packetData[len(packetData)-1].(string); !ok || recoveredOffset == "" {
		t.Fatalf("expected recovered bigserial offset, got %#v", packetData)
	}
	restoredAgain, err := target.RestoreSession("pid-1", string(offset))
	if err != nil {
		t.Fatal(err)
	}
	if restoredAgain != nil {
		t.Fatal("expected the restored session to be consumed atomically")
	}
}
