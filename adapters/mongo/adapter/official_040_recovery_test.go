package adapter

import (
	"fmt"
	"os"
	"testing"
	"time"

	clusteradapter "github.com/aqcool/socket.io/adapters/adapter/v4"
	mongoio "github.com/aqcool/socket.io/adapters/mongo/v4"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type official040MongoRecoveryFixture struct {
	source       socket.Adapter
	target       socket.Adapter
	otherNsp     socket.Adapter
	collection   *mongod.Collection
	sourceServer *socket.Server
	targetServer *socket.Server
}

func newOfficial040MongoRecoveryFixture(
	t *testing.T,
	database *mongod.Database,
	collectionName string,
	addCreatedAt bool,
) *official040MongoRecoveryFixture {
	t.Helper()

	if addCreatedAt {
		collection := database.Collection(collectionName)
		_, err := collection.Indexes().CreateOne(t.Context(), mongod.IndexModel{
			Keys:    bson.D{{Key: "createdAt", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(3600),
		})
		if err != nil {
			t.Fatal(err)
		}
	} else if err := database.CreateCollection(
		t.Context(),
		collectionName,
		options.CreateCollection().SetCapped(true).SetSizeInBytes(1_000_000),
	); err != nil {
		t.Fatal(err)
	}

	collection := database.Collection(collectionName)
	newServer := func() *socket.Server {
		adapterOptions := DefaultMongoAdapterOptions()
		adapterOptions.SetAddCreatedAtField(addCreatedAt)
		adapterOptions.SetHeartbeatInterval(50 * time.Millisecond)
		adapterOptions.SetHeartbeatTimeout(500)

		serverOptions := socket.DefaultServerOptions()
		recovery := socket.DefaultConnectionStateRecovery()
		recovery.SetMaxDisconnectionDuration(5_000)
		serverOptions.SetConnectionStateRecovery(recovery)
		serverOptions.SetAdapter(&MongoAdapterBuilder{
			Mongo: mongoio.NewMongoClient(t.Context(), collection),
			Opts:  adapterOptions,
		})
		return socket.NewServer(nil, serverOptions)
	}

	sourceServer := newServer()
	targetServer := newServer()
	t.Cleanup(func() {
		sourceServer.Close(nil)
		targetServer.Close(nil)
	})

	return &official040MongoRecoveryFixture{
		source:       sourceServer.Sockets().Adapter(),
		target:       targetServer.Sockets().Adapter(),
		otherNsp:     sourceServer.Of("/foo", nil).Adapter(),
		collection:   collection,
		sourceServer: sourceServer,
		targetServer: targetServer,
	}
}

func (f *official040MongoRecoveryFixture) broadcast(
	t *testing.T,
	event string,
	value any,
	rooms []socket.Room,
	except []socket.Room,
) clusteradapter.Offset {
	t.Helper()

	packet := &parser.Packet{Type: parser.EVENT, Data: []any{event, value}}
	f.source.Broadcast(packet, &socket.BroadcastOptions{
		Rooms:  types.NewSet(rooms...),
		Except: types.NewSet(except...),
	})
	data := packet.Data.([]any)
	offset, ok := data[len(data)-1].(clusteradapter.Offset)
	if !ok || offset == "" {
		t.Fatalf("expected MongoDB ObjectID offset, got %#v", data)
	}
	return offset
}

func (f *official040MongoRecoveryFixture) persist(pid string, rooms ...socket.Room) {
	f.target.PersistSession(&socket.SessionToPersist{
		Sid:   socket.SocketId("sid-" + pid),
		Pid:   socket.PrivateSessionId(pid),
		Rooms: types.NewSet(rooms...),
		Data:  map[string]any{"pid": pid},
	})
}

func assertOfficial040RestoredSession(t *testing.T, restored *socket.Session, pid string) {
	t.Helper()
	if restored == nil {
		t.Fatal("expected a restored session")
	}
	if restored.Sid != socket.SocketId("sid-"+pid) || restored.Pid != socket.PrivateSessionId(pid) {
		t.Fatalf("unexpected restored identity: %#v", restored.SessionToPersist)
	}
	data, ok := restored.Data.(map[string]any)
	if !ok || data["pid"] != pid {
		t.Fatalf("unexpected restored data: %#v", restored.Data)
	}
}

// TestOfficialMongoAdapter040ConnectionStateRecovery maps the ten executable
// tests in @socket.io/mongo-adapter@0.4.0/test/connection-state-recovery.ts:
// the same five assertions are run once with a capped collection and once with
// a createdAt TTL index.
func TestOfficialMongoAdapter040ConnectionStateRecovery(t *testing.T) {
	uri := os.Getenv("SOCKET_IO_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_MONGO_TEST_URI is not set")
	}

	client, err := mongod.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(t.Context()) })
	if err = client.Ping(t.Context(), readpref.Primary()); err != nil {
		t.Fatal(err)
	}

	database := client.Database(fmt.Sprintf("socket_io_official_040_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = database.Drop(t.Context()) })

	for _, mode := range []struct {
		name         string
		collection   string
		addCreatedAt bool
	}{
		{name: "with a capped collection", collection: "events-capped"},
		{name: "with a TTL index", collection: "events-ttl", addCreatedAt: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			fixture := newOfficial040MongoRecoveryFixture(
				t,
				database,
				mode.collection,
				mode.addCreatedAt,
			)

			t.Run("should restore the session", func(t *testing.T) {
				pid := mode.collection + "-restore"
				offset := fixture.broadcast(t, "init", 1, nil, nil)
				fixture.persist(pid, socket.Room("sid-"+pid))

				restored, restoreErr := fixture.target.RestoreSession(socket.PrivateSessionId(pid), string(offset))
				if restoreErr != nil {
					t.Fatal(restoreErr)
				}
				assertOfficial040RestoredSession(t, restored, pid)
			})

			t.Run("should restore any missed packets", func(t *testing.T) {
				pid := mode.collection + "-missed"
				sidRoom := socket.Room("sid-" + pid)
				offset := fixture.broadcast(t, "init", 1, nil, nil)
				fixture.persist(pid, sidRoom, "room1")

				fixture.broadcast(t, "myEvent", 1, []socket.Room{sidRoom}, nil)
				fixture.broadcast(t, "myEvent", 2, nil, nil)
				fixture.broadcast(t, "myEvent", 3, []socket.Room{"room1"}, nil)
				fixture.broadcast(t, "myEvent", 4, []socket.Room{"room2"}, nil)
				fixture.broadcast(t, "myEvent", 5, nil, []socket.Room{"room1"})
				otherPacket := &parser.Packet{Type: parser.EVENT, Data: []any{"myEvent", 6}}
				fixture.otherNsp.Broadcast(otherPacket, &socket.BroadcastOptions{
					Rooms:  types.NewSet[socket.Room](),
					Except: types.NewSet[socket.Room](),
				})

				restored, restoreErr := fixture.target.RestoreSession(socket.PrivateSessionId(pid), string(offset))
				if restoreErr != nil {
					t.Fatal(restoreErr)
				}
				assertOfficial040RestoredSession(t, restored, pid)
				if len(restored.MissedPackets) != 3 {
					t.Fatalf("expected missed packets [1 2 3], got %#v", restored.MissedPackets)
				}
				for index, expected := range []string{"1", "2", "3"} {
					packet, ok := restored.MissedPackets[index].([]any)
					if !ok || len(packet) < 3 || fmt.Sprint(packet[1]) != expected {
						t.Fatalf("unexpected missed packet %d: %#v", index, restored.MissedPackets[index])
					}
					if recoveredOffset, ok := packet[len(packet)-1].(string); !ok || recoveredOffset == "" {
						t.Fatalf("missing recovered offset in packet %d: %#v", index, packet)
					}
				}
			})

			t.Run("should restore the session only once", func(t *testing.T) {
				pid := mode.collection + "-once"
				offset := fixture.broadcast(t, "init", 1, nil, nil)
				fixture.persist(pid, socket.Room("sid-"+pid))

				first, restoreErr := fixture.target.RestoreSession(socket.PrivateSessionId(pid), string(offset))
				if restoreErr != nil {
					t.Fatal(restoreErr)
				}
				assertOfficial040RestoredSession(t, first, pid)
				second, restoreErr := fixture.target.RestoreSession(socket.PrivateSessionId(pid), string(offset))
				if restoreErr != nil {
					t.Fatal(restoreErr)
				}
				if second != nil {
					t.Fatalf("expected the session to be consumed, got %#v", second)
				}
			})

			t.Run("should fail to restore an unknown session (invalid session ID)", func(t *testing.T) {
				offset := fixture.broadcast(t, "init", 1, nil, nil)
				restored, restoreErr := fixture.target.RestoreSession("unknown-pid", string(offset))
				if restoreErr != nil {
					t.Fatal(restoreErr)
				}
				if restored != nil {
					t.Fatalf("expected no restored session, got %#v", restored)
				}
			})

			t.Run("should fail to restore an unknown session (invalid offset)", func(t *testing.T) {
				pid := mode.collection + "-invalid-offset"
				fixture.persist(pid, socket.Room("sid-"+pid))
				if restored, restoreErr := fixture.target.RestoreSession(socket.PrivateSessionId(pid), "abc"); restoreErr == nil || restored != nil {
					t.Fatalf("expected invalid offset error, got session=%#v error=%v", restored, restoreErr)
				}
			})
		})
	}
}
