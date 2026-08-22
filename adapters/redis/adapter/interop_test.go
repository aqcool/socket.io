package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"testing"
	"time"

	redisbridge "github.com/aqcool/socket.io/adapters/redis/v4"
	redisemitter "github.com/aqcool/socket.io/adapters/redis/v4/emitter"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	rds "github.com/redis/go-redis/v9"
)

const redisInteropTestEnv = "SOCKET_IO_REDIS_INTEROP_TEST"

const redisInteropGoWorkerEnv = "SOCKET_IO_REDIS_INTEROP_GO_WORKER"

type officialJSONRedisParser struct{}

func (officialJSONRedisParser) Encode(value any) ([]byte, error) {
	return json.Marshal(value)
}

func (officialJSONRedisParser) Decode(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func interopAck(args []any) socket.Ack {
	if len(args) == 0 {
		return nil
	}
	ack, _ := args[len(args)-1].(socket.Ack)
	return ack
}

type redisEmitterCommands struct {
	broadcast  func(string, any) error
	namespace  func(string, any) error
	room       func(socket.Room, string, any) error
	except     func(socket.Room, string, any) error
	join       func(socket.Room, socket.Room) error
	leave      func(socket.Room, socket.Room) error
	disconnect func(socket.Room) error
	serverSide func(string, any) error
}

func newRedisEmitterCommands(mode string, client *redisbridge.RedisClient) redisEmitterCommands {
	if mode == "streams" {
		emitter := redisemitter.NewRedisStreamsEmitter(client, nil)
		return redisEmitterCommands{
			broadcast:  func(event string, value any) error { return emitter.Emit(event, value, []byte{1, 2, 3}) },
			namespace:  func(event string, value any) error { return emitter.Of("/custom").Emit(event, value) },
			room:       func(room socket.Room, event string, value any) error { return emitter.To(room).Emit(event, value) },
			except:     func(room socket.Room, event string, value any) error { return emitter.Except(room).Emit(event, value) },
			join:       func(selector, room socket.Room) error { return emitter.In(selector).SocketsJoin(room) },
			leave:      func(selector, room socket.Room) error { return emitter.In(selector).SocketsLeave(room) },
			disconnect: func(selector socket.Room) error { return emitter.In(selector).DisconnectSockets(false) },
			serverSide: func(event string, value any) error { return emitter.ServerSideEmit(event, value) },
		}
	}

	opts := redisemitter.DefaultEmitterOptions()
	if mode == "sharded" {
		opts.SetSharded(true)
		opts.SetSubscriptionMode(redisbridge.DynamicSubscriptionMode)
	}
	emitter := redisemitter.NewEmitter(client, opts)
	return redisEmitterCommands{
		broadcast:  func(event string, value any) error { return emitter.Emit(event, value, []byte{1, 2, 3}) },
		namespace:  func(event string, value any) error { return emitter.Of("/custom").Emit(event, value) },
		room:       func(room socket.Room, event string, value any) error { return emitter.To(room).Emit(event, value) },
		except:     func(room socket.Room, event string, value any) error { return emitter.Except(room).Emit(event, value) },
		join:       func(selector, room socket.Room) error { return emitter.In(selector).SocketsJoin(room) },
		leave:      func(selector, room socket.Room) error { return emitter.In(selector).SocketsLeave(room) },
		disconnect: func(selector socket.Room) error { return emitter.In(selector).DisconnectSockets(false) },
		serverSide: func(event string, value any) error { return emitter.ServerSideEmit(event, value) },
	}
}

func (c redisEmitterCommands) execute(args []any) error {
	command := fmt.Sprint(args[0])
	values := args[1 : len(args)-1]
	switch command {
	case "broadcast":
		return c.broadcast(fmt.Sprint(values[0]), values[1])
	case "namespace":
		return c.namespace(fmt.Sprint(values[0]), values[1])
	case "room":
		return c.room(socket.Room(fmt.Sprint(values[0])), fmt.Sprint(values[1]), values[2])
	case "except":
		return c.except(socket.Room(fmt.Sprint(values[0])), fmt.Sprint(values[1]), values[2])
	case "join":
		return c.join(socket.Room(fmt.Sprint(values[0])), socket.Room(fmt.Sprint(values[1])))
	case "leave":
		return c.leave(socket.Room(fmt.Sprint(values[0])), socket.Room(fmt.Sprint(values[1])))
	case "disconnect":
		return c.disconnect(socket.Room(fmt.Sprint(values[0])))
	case "server-side":
		return c.serverSide(fmt.Sprint(values[0]), values[1])
	default:
		return fmt.Errorf("unknown emitter command %q", command)
	}
}

func TestOfficialNodeRedisAdapterInterop(t *testing.T) {
	if os.Getenv(redisInteropTestEnv) != "1" {
		t.Skip("set SOCKET_IO_REDIS_INTEROP_TEST=1 after running npm ci in testdata/interop")
	}

	address := os.Getenv("SOCKET_IO_REDIS_TEST_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	password, passwordConfigured := os.LookupEnv("SOCKET_IO_REDIS_TEST_PASSWORD")
	if !passwordConfigured {
		password = "root"
	}

	for _, mode := range []string{"pubsub", "streams", "sharded"} {
		t.Run(mode, func(t *testing.T) {
			runOfficialNodeRedisAdapterInterop(t, mode, address, password)
		})
	}
}

func TestRedisInteropGoWorker(t *testing.T) {
	if os.Getenv(redisInteropGoWorkerEnv) != "1" {
		t.Skip("helper process for TestOfficialNodeRedisAdapterInterop")
	}

	address := os.Getenv("SOCKET_IO_REDIS_TEST_ADDR")
	password := os.Getenv("SOCKET_IO_REDIS_TEST_PASSWORD")
	mode := os.Getenv("SOCKET_IO_REDIS_INTEROP_GO_WORKER_MODE")
	listenAddress := os.Getenv("SOCKET_IO_REDIS_INTEROP_GO_WORKER_ADDR")
	if address == "" || mode == "" || listenAddress == "" {
		t.Fatal("missing Go interop worker configuration")
	}
	startSignal := make(chan os.Signal, 1)
	signal.Notify(startSignal, syscall.SIGUSR1)
	<-startSignal
	signal.Stop(startSignal)

	ctx := context.Background()
	pubClient := rds.NewClient(&rds.Options{Addr: address, Password: password})
	subClient := rds.NewClient(&rds.Options{Addr: address, Password: password})
	redisClient := redisbridge.NewRedisClientWithSub(ctx, pubClient, subClient)
	_ = redisClient.On("error", func(...any) {})
	serverOptions := socket.DefaultServerOptions()
	switch mode {
	case "pubsub":
		adapterOptions := DefaultRedisAdapterOptions()
		adapterOptions.SetPublishOnSpecificResponseChannel(true)
		serverOptions.SetAdapter(&RedisAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	case "streams":
		adapterOptions := DefaultRedisStreamsAdapterOptions()
		adapterOptions.SetBlockTimeInMs(100)
		serverOptions.SetAdapter(&RedisStreamsAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	case "sharded":
		adapterOptions := DefaultShardedRedisAdapterOptions()
		adapterOptions.SetSubscriptionMode(redisbridge.DynamicSubscriptionMode)
		serverOptions.SetAdapter(&ShardedRedisAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	default:
		t.Fatalf("unsupported Redis adapter mode %q", mode)
	}
	server := socket.NewServer(nil, serverOptions)
	server.Of("/", func(args ...any) {
		args[0].(*socket.Socket).Join("go-worker-room")
	})

	httpServer := &http.Server{
		Addr:              listenAddress,
		Handler:           server.ServeHandler(nil),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		t.Fatalf("Go interop worker failed: %v", err)
	}
}

func startRedisInteropGoWorker(t *testing.T, mode, redisAddress, password string) (*exec.Cmd, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate Go interop worker port: %v", err)
	}
	listenAddress := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release Go interop worker port: %v", err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestRedisInteropGoWorker$", "-test.v")
	command.Env = append(os.Environ(),
		redisInteropGoWorkerEnv+"=1",
		"SOCKET_IO_REDIS_TEST_ADDR="+redisAddress,
		"SOCKET_IO_REDIS_TEST_PASSWORD="+password,
		"SOCKET_IO_REDIS_INTEROP_GO_WORKER_MODE="+mode,
		"SOCKET_IO_REDIS_INTEROP_GO_WORKER_ADDR="+listenAddress,
	)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Go interop worker: %v", err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	// Give the helper test enough time to install its SIGUSR1 handler. The
	// actual Socket.IO server and Redis subscriptions are intentionally started
	// later by the Node fault scenario.
	time.Sleep(100 * time.Millisecond)
	return command, "http://" + listenAddress
}

func runOfficialNodeRedisAdapterInterop(t *testing.T, mode, address, password string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pubClient := rds.NewClient(&rds.Options{Addr: address, Password: password})
	subClient := rds.NewClient(&rds.Options{Addr: address, Password: password})
	adminClient := rds.NewClient(&rds.Options{Addr: address, Password: password})
	t.Cleanup(func() {
		_ = pubClient.Close()
		_ = subClient.Close()
		_ = adminClient.Close()
	})
	if err := pubClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis is not available at %s: %v", address, err)
	}

	redisClient := redisbridge.NewRedisClientWithSub(ctx, pubClient, subClient)
	_ = redisClient.On("error", func(...any) {})
	serverOptions := socket.DefaultServerOptions()
	switch mode {
	case "pubsub":
		adapterOptions := DefaultRedisAdapterOptions()
		adapterOptions.SetPublishOnSpecificResponseChannel(true)
		serverOptions.SetAdapter(&RedisAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	case "streams":
		adapterOptions := DefaultRedisStreamsAdapterOptions()
		adapterOptions.SetBlockTimeInMs(100)
		serverOptions.SetAdapter(&RedisStreamsAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
		recovery := socket.DefaultConnectionStateRecovery()
		recovery.SetMaxDisconnectionDuration(30_000)
		recovery.SetSkipMiddlewares(true)
		serverOptions.SetConnectionStateRecovery(recovery)
	case "sharded":
		adapterOptions := DefaultShardedRedisAdapterOptions()
		adapterOptions.SetSubscriptionMode(redisbridge.DynamicSubscriptionMode)
		serverOptions.SetAdapter(&ShardedRedisAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	default:
		t.Fatalf("unsupported Redis adapter mode %q", mode)
	}
	server := socket.NewServer(nil, serverOptions)
	externalEmitter := newRedisEmitterCommands(mode, redisClient)

	server.Of("/", func(args ...any) {
		client := args[0].(*socket.Socket)
		client.Join("go-room")
		_ = client.On("interop-join", func(args ...any) {
			client.Join(socket.Room(fmt.Sprint(args[0])))
			interopAck(args)([]any{"joined"}, nil)
		})
		_ = client.On("interop-set-data", func(args ...any) {
			client.SetData(args[0])
			interopAck(args)(nil, nil)
		})

		_ = client.On("control-broadcast", func(args ...any) {
			value := args[0]
			server.Emit("from-go", value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-burst-broadcast", func(args ...any) {
			count, err := strconv.Atoi(fmt.Sprint(args[0]))
			if err != nil || count < 0 || count > 1000 {
				interopAck(args)([]any{"invalid burst count"}, nil)
				return
			}
			for index := range count {
				server.Emit("go-burst", index)
			}
			interopAck(args)([]any{""}, nil)
		})
		_ = client.On("control-binary-broadcast", func(args ...any) {
			value := args[0]
			server.Emit("binary-from-go", value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-room-broadcast", func(args ...any) {
			value := args[0]
			_ = server.To("node-room").Emit("go-to-node-room", value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-local-broadcast", func(args ...any) {
			value := args[0]
			_ = server.Local().Emit("local-from-go", value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-namespace-broadcast", func(args ...any) {
			value := args[0]
			_ = server.Of("/custom", nil).Emit("namespace-from-go", value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-target-broadcast", func(args ...any) {
			target := socket.Room(fmt.Sprint(args[0]))
			event := fmt.Sprint(args[1])
			value := args[2]
			_ = server.To(target).Emit(event, value)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-fetch-sockets", func(args ...any) {
			ack := interopAck(args)
			server.FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
				if err != nil {
					ack(nil, err)
					return
				}
				ack([]any{len(sockets)}, nil)
			})
		})
		_ = client.On("control-all-rooms", func(args ...any) {
			ack := interopAck(args)
			redisAdapter, ok := server.Sockets().Adapter().(RedisAdapter)
			if !ok {
				ack(nil, errors.New("allRooms is only available on the Redis Pub/Sub adapter"))
				return
			}
			redisAdapter.AllRooms()(func(rooms *types.Set[socket.Room], err error) {
				if err != nil {
					ack(nil, err)
					return
				}
				values := make([]string, 0, rooms.Len())
				for _, room := range rooms.Keys() {
					values = append(values, string(room))
				}
				sort.Strings(values)
				ack([]any{values}, nil)
			})
		})
		_ = client.On("control-broadcast-ack", func(args ...any) {
			value := args[0]
			ack := interopAck(args)
			server.Timeout(3*time.Second).EmitWithAck("cluster-ack", value)(func(responses []any, err error) {
				ack([]any{responses}, err)
			})
		})
		_ = client.On("control-binary-broadcast-ack", func(args ...any) {
			value := args[0]
			ack := interopAck(args)
			server.Timeout(3*time.Second).EmitWithAck("cluster-binary-ack", value)(func(responses []any, err error) {
				ack([]any{responses}, err)
			})
		})
		_ = client.On("control-server-side-ack", func(args ...any) {
			value := args[0]
			ack := interopAck(args)
			if err := server.ServerSideEmitWithAck("from-go-server", value)(func(responses []any, err error) {
				ack([]any{responses}, err)
			}); err != nil {
				ack(nil, err)
			}
		})
		_ = client.On("control-sockets-join", func(args ...any) {
			targetRoom := socket.Room(fmt.Sprint(args[0]))
			roomToJoin := socket.Room(fmt.Sprint(args[1]))
			server.In(targetRoom).SocketsJoin(roomToJoin)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-sockets-leave", func(args ...any) {
			targetRoom := socket.Room(fmt.Sprint(args[0]))
			roomToLeave := socket.Room(fmt.Sprint(args[1]))
			server.In(targetRoom).SocketsLeave(roomToLeave)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-has-room", func(args ...any) {
			room := socket.Room(fmt.Sprint(args[0]))
			interopAck(args)([]any{client.Rooms().Has(room)}, nil)
		})
		_ = client.On("control-disconnect-room", func(args ...any) {
			targetRoom := socket.Room(fmt.Sprint(args[0]))
			server.In(targetRoom).DisconnectSockets(false)
			interopAck(args)(nil, nil)
		})
		_ = client.On("control-reset-redis-connections", func(args ...any) {
			ack := interopAck(args)
			normalKilled, err := adminClient.ClientKillByFilter(ctx, "TYPE", "normal", "SKIPME", "yes").Result()
			if err != nil {
				ack([]any{int64(0), int64(0), err.Error()}, nil)
				return
			}
			pubSubKilled, err := adminClient.ClientKillByFilter(ctx, "TYPE", "pubsub", "SKIPME", "yes").Result()
			if err != nil {
				ack([]any{normalKilled, int64(0), err.Error()}, nil)
				return
			}
			ack([]any{normalKilled, pubSubKilled, ""}, nil)
		})
		_ = client.On("interop-go-emitter-command", func(args ...any) {
			ack := interopAck(args)
			if len(args) < 2 {
				ack([]any{"missing emitter command"}, nil)
				return
			}
			if err := externalEmitter.execute(args); err != nil {
				ack([]any{err.Error()}, nil)
				return
			}
			ack([]any{nil}, nil)
		})
	})
	server.Of("/custom", nil)
	_ = server.Sockets().On("from-node-server", func(args ...any) {
		value := args[0]
		interopAck(args)([]any{"go-server:" + fmt.Sprint(value)}, nil)
	})
	_ = server.Sockets().On("official-matrix-server-side", func(args ...any) {
		_ = server.Local().Emit("official-matrix-server-side-seen", args...)
	})
	_ = server.Sockets().On("official-matrix-server-side-timeout", func(...any) {
		// The official timeout case intentionally leaves this request unanswered.
	})
	_ = server.On("official-emitter-server-side", func(args ...any) {
		_ = server.Local().Emit("official-emitter-server-side-seen", args...)
	})

	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	goWorkerCommand, goWorkerURL := startRedisInteropGoWorker(t, mode, address, password)

	redisURL := &url.URL{Scheme: "redis", Host: address}
	if password != "" {
		redisURL.User = url.UserPassword("", password)
	}
	workingDirectory := filepath.Join("..", "testdata", "interop")
	commandCtx, commandCancel := context.WithTimeout(ctx, 90*time.Second)
	defer commandCancel()
	command := exec.CommandContext(
		commandCtx,
		"node",
		"interop.cjs",
		httpServer.URL,
		redisURL.String(),
		mode,
		goWorkerURL,
		strconv.Itoa(goWorkerCommand.Process.Pid),
	)
	command.Dir = workingDirectory
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		if commandCtx.Err() != nil {
			t.Fatalf("Node/Go Redis adapter interop test timed out: %v", commandCtx.Err())
		}
		t.Fatalf("Node/Go Redis adapter interop test failed: %v", err)
	}

	if mode == "pubsub" || mode == "streams" {
		emitterCtx, emitterCancel := context.WithTimeout(ctx, 75*time.Second)
		defer emitterCancel()
		emitterCommand := exec.CommandContext(
			emitterCtx,
			"node",
			"emitter-matrix.cjs",
			httpServer.URL,
			redisURL.String(),
			mode,
		)
		emitterCommand.Dir = workingDirectory
		emitterCommand.Stdout = os.Stdout
		emitterCommand.Stderr = os.Stderr
		if err := emitterCommand.Run(); err != nil {
			if emitterCtx.Err() != nil {
				t.Fatalf("official Redis %s emitter matrix timed out: %v", mode, emitterCtx.Err())
			}
			t.Fatalf("official Redis %s emitter matrix failed: %v", mode, err)
		}
	}

	if mode == "pubsub" {
		runOfficialRedisCustomParserInterop(t, ctx, redisClient, redisURL.String(), workingDirectory)
	}
}

func runOfficialRedisCustomParserInterop(
	t *testing.T,
	ctx context.Context,
	redisClient *redisbridge.RedisClient,
	redisURL string,
	workingDirectory string,
) {
	t.Helper()
	key := "socket.io-json-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	parser := officialJSONRedisParser{}
	adapterOptions := DefaultRedisAdapterOptions()
	adapterOptions.SetKey(key)
	adapterOptions.SetParser(parser)
	serverOptions := socket.DefaultServerOptions()
	serverOptions.SetAdapter(&RedisAdapterBuilder{Redis: redisClient, Opts: adapterOptions})
	server := socket.NewServer(nil, serverOptions)

	emitterOptions := redisemitter.DefaultEmitterOptions()
	emitterOptions.SetKey(key)
	emitterOptions.SetParser(parser)
	goEmitter := redisemitter.NewEmitter(redisClient, emitterOptions)
	server.Of("/", func(args ...any) {
		client := args[0].(*socket.Socket)
		_ = client.On("interop-go-custom-parser-emitter", func(args ...any) {
			ack := interopAck(args)
			if err := goEmitter.Emit("go-custom-payload", 1, "2", []any{3}); err != nil {
				ack([]any{err.Error()}, nil)
				return
			}
			ack([]any{nil}, nil)
		})
	})
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	defer func() {
		server.Close(nil)
		httpServer.Close()
	}()

	commandCtx, commandCancel := context.WithTimeout(ctx, 30*time.Second)
	defer commandCancel()
	command := exec.CommandContext(
		commandCtx,
		"node",
		"custom-parser-matrix.cjs",
		httpServer.URL,
		redisURL,
		key,
	)
	command.Dir = workingDirectory
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		if commandCtx.Err() != nil {
			t.Fatalf("official Redis custom parser emitter matrix timed out: %v", commandCtx.Err())
		}
		t.Fatalf("official Redis custom parser emitter matrix failed: %v", err)
	}
}
