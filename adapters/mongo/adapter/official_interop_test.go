package adapter

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	mongoio "github.com/aqcool/socket.io/adapters/mongo/v4"
	mgemitter "github.com/aqcool/socket.io/adapters/mongo/v4/emitter"
	"github.com/aqcool/socket.io/servers/socket/v4"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const mongoOfficialInteropEnv = "SOCKET_IO_MONGO_OFFICIAL_INTEROP"

func TestOfficialMongoAdapterBilateralInterop(t *testing.T) {
	if os.Getenv(mongoOfficialInteropEnv) != "1" {
		t.Skip("set SOCKET_IO_MONGO_OFFICIAL_INTEROP=1 after npm ci in testdata/official-interop")
	}
	uri := os.Getenv("SOCKET_IO_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_MONGO_TEST_URI is not set")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client, err := mongod.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
	databaseName := fmt.Sprintf("socket_io_official_interop_%d", time.Now().UnixNano())
	database := client.Database(databaseName)
	t.Cleanup(func() { _ = database.Drop(context.Background()) })

	mongoClient := mongoio.NewMongoClient(ctx, database.Collection("events"))
	adapterOptions := DefaultMongoAdapterOptions()
	adapterOptions.SetAddCreatedAtField(true)
	adapterOptions.SetHeartbeatInterval(200 * time.Millisecond)
	adapterOptions.SetHeartbeatTimeout(600)
	serverOptions := socket.DefaultServerOptions()
	recovery := socket.DefaultConnectionStateRecovery()
	recovery.SetMaxDisconnectionDuration(5_000)
	serverOptions.SetConnectionStateRecovery(recovery)
	serverOptions.SetAdapter(&MongoAdapterBuilder{Mongo: mongoClient, Opts: adapterOptions})
	server := socket.NewServer(nil, serverOptions)
	emitterOptions := mgemitter.DefaultEmitterOptions()
	emitterOptions.SetAddCreatedAtField(true)
	goEmitter := mgemitter.NewEmitter(mongoClient, emitterOptions)
	registerOfficialMongoInteropHandlers(server, goEmitter)
	server.Of("/custom", nil)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})

	workingDirectory := filepath.Join("..", "testdata", "official-interop")
	commandCtx, commandCancel := context.WithTimeout(ctx, 60*time.Second)
	defer commandCancel()
	command := exec.CommandContext(commandCtx, "node", "matrix.cjs", httpServer.URL, uri, databaseName)
	command.Dir = workingDirectory
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err = command.Run(); err != nil {
		if commandCtx.Err() != nil {
			t.Fatalf("official MongoDB Adapter matrix timed out: %v", commandCtx.Err())
		}
		t.Fatalf("official MongoDB Adapter matrix failed: %v", err)
	}

	emitterCtx, emitterCancel := context.WithTimeout(ctx, 75*time.Second)
	defer emitterCancel()
	emitterCommand := exec.CommandContext(emitterCtx, "node", "emitter-matrix.cjs", httpServer.URL, uri, databaseName)
	emitterCommand.Dir = workingDirectory
	emitterCommand.Stdout = os.Stdout
	emitterCommand.Stderr = os.Stderr
	if err = emitterCommand.Run(); err != nil {
		if emitterCtx.Err() != nil {
			t.Fatalf("official MongoDB emitter matrix timed out: %v", emitterCtx.Err())
		}
		t.Fatalf("official MongoDB emitter matrix failed: %v", err)
	}
}

func registerOfficialMongoInteropHandlers(server *socket.Server, externalEmitter *mgemitter.Emitter) {
	_ = server.On("connection", func(values ...any) {
		client := values[0].(*socket.Socket)
		_ = client.On("interop-join", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			client.Join(socket.Room(fmt.Sprint(args[0])))
			ack([]any{"joined"}, nil)
		})
		_ = client.On("interop-trigger-broadcast", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.Emit("interop-from-go", "go", []byte{1, 2, 3})
			ack(nil, nil)
		})
		_ = client.On("interop-trigger-broadcast-ack", func(args ...any) {
			controlAck := args[len(args)-1].(socket.Ack)
			server.Timeout(2*time.Second).EmitWithAck("interop-ack-from-go", "go")(func(responses []any, err error) {
				if err != nil {
					controlAck([]any{err.Error(), responses}, nil)
					return
				}
				controlAck([]any{nil, responses}, nil)
			})
		})
		_ = client.On("interop-trigger-room", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			_ = server.To("shared-room").Emit("interop-room-from-go", "go-room")
			ack(nil, nil)
		})
		_ = client.On("interop-fetch", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
				if err != nil {
					ack([]any{0, err.Error()}, nil)
					return
				}
				ack([]any{len(sockets), nil}, nil)
			})
		})
		_ = client.On("interop-trigger-server-side", func(args ...any) {
			controlAck := args[len(args)-1].(socket.Ack)
			err := server.ServerSideEmitWithAck("interop-go-cluster", "value")(func(responses []any, err error) {
				if err != nil {
					controlAck([]any{err.Error(), responses}, nil)
					return
				}
				controlAck([]any{nil, responses}, nil)
			})
			if err != nil {
				controlAck([]any{err.Error(), []any{}}, nil)
			}
		})
		_ = client.On("interop-remote-join", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.In(socket.Room(fmt.Sprint(args[0]))).SocketsJoin(socket.Room(fmt.Sprint(args[1])))
			time.AfterFunc(300*time.Millisecond, func() { ack(nil, nil) })
		})
		_ = client.On("interop-trigger-remote-room", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			_ = server.To("go-remote-room").Emit("interop-go-remote-room", "joined")
			ack(nil, nil)
		})
		_ = client.On("interop-trigger-recovery-baseline", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.Emit("interop-recovery-baseline", "go")
			ack(nil, nil)
		})
		_ = client.On("interop-trigger-recovered-event", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.Emit("interop-recovered-event", "missed")
			ack(nil, nil)
		})
		_ = client.On("interop-trigger-after-roll", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			server.Emit("interop-after-roll", "go-roll")
			ack(nil, nil)
		})
		_ = client.On("interop-go-emitter-command", func(args ...any) {
			ack := args[len(args)-1].(socket.Ack)
			if len(args) < 2 {
				ack([]any{"missing emitter command"}, nil)
				return
			}
			command := fmt.Sprint(args[0])
			values := args[1 : len(args)-1]
			var err error
			switch command {
			case "broadcast":
				err = externalEmitter.Emit(fmt.Sprint(values[0]), values[1], []byte{1, 2, 3})
			case "namespace":
				err = externalEmitter.Of("/custom").Emit(fmt.Sprint(values[0]), values[1])
			case "room":
				err = externalEmitter.To(socket.Room(fmt.Sprint(values[0]))).Emit(fmt.Sprint(values[1]), values[2])
			case "except":
				err = externalEmitter.Except(socket.Room(fmt.Sprint(values[0]))).Emit(fmt.Sprint(values[1]), values[2])
			case "join":
				err = externalEmitter.In(socket.Room(fmt.Sprint(values[0]))).SocketsJoin(socket.Room(fmt.Sprint(values[1])))
			case "leave":
				err = externalEmitter.In(socket.Room(fmt.Sprint(values[0]))).SocketsLeave(socket.Room(fmt.Sprint(values[1])))
			case "disconnect":
				err = externalEmitter.In(socket.Room(fmt.Sprint(values[0]))).DisconnectSockets(false)
			case "server-side":
				err = externalEmitter.ServerSideEmit(fmt.Sprint(values[0]), values[1])
			default:
				err = fmt.Errorf("unknown emitter command %q", command)
			}
			if err != nil {
				ack([]any{err.Error()}, nil)
				return
			}
			ack([]any{nil}, nil)
		})
	})
	_ = server.On("interop-node-cluster", func(args ...any) {
		ack := args[len(args)-1].(socket.Ack)
		ack([]any{"go:" + fmt.Sprint(args[0])}, nil)
	})
	_ = server.On("official-emitter-server-side", func(args ...any) {
		_ = server.Local().Emit("official-emitter-server-side-seen", args...)
	})
}
