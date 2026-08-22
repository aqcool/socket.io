package adapter

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	unixbridge "github.com/aqcool/socket.io/adapters/unix/v4"
	engine "github.com/aqcool/socket.io/servers/engine/v4"
	engineconfig "github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/sticky/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

const (
	clusterOfficialInteropEnv = "SOCKET_IO_CLUSTER_OFFICIAL_INTEROP"
	clusterWorkerEnv          = "SOCKET_IO_UNIX_CLUSTER_WORKER"
	clusterWorkerParentFDEnv  = "SOCKET_IO_UNIX_CLUSTER_PARENT_FD"
)

func clusterAck(args []any) socket.Ack {
	if len(args) == 0 {
		return nil
	}
	ack, _ := args[len(args)-1].(socket.Ack)
	return ack
}

func replyClusterAck(args []any, values ...any) {
	if ack := clusterAck(args); ack != nil {
		ack(values, nil)
	}
}

func registerClusterSocketHandlers(server *socket.Server, client *socket.Socket, nodeID string, custom bool) {
	_ = client.Emit("node", nodeID)
	_ = client.On("join", func(args ...any) {
		client.Join(socket.Room(fmt.Sprint(args[0])))
		replyClusterAck(args)
	})
	_ = client.On("control-has-room", func(args ...any) {
		replyClusterAck(args, client.Rooms().Has(socket.Room(fmt.Sprint(args[0]))))
	})
	if custom {
		_ = client.On("control-custom-broadcast", func(args ...any) {
			_ = server.Of("/custom", nil).Emit("custom-event", args[0])
			replyClusterAck(args)
		})
		return
	}
	_ = client.On("control-broadcast", func(args ...any) {
		server.Emit("cluster-text", args[0])
		replyClusterAck(args)
	})
	_ = client.On("control-soak", func(args ...any) {
		replyClusterAck(args, args[0])
	})
	_ = client.On("control-soak-broadcast", func(args ...any) {
		server.Emit("soak-event", args[0])
		replyClusterAck(args)
	})
	_ = client.On("control-binary-broadcast", func(args ...any) {
		server.Emit("cluster-binary", args[0])
		replyClusterAck(args)
	})
	_ = client.On("control-room-broadcast", func(args ...any) {
		_ = server.To(socket.Room(fmt.Sprint(args[0]))).Emit("room-event", args[1])
		replyClusterAck(args)
	})
	_ = client.On("control-except-broadcast", func(args ...any) {
		_ = server.Except(socket.Room(fmt.Sprint(args[0]))).Emit("except-event", args[1])
		replyClusterAck(args)
	})
	_ = client.On("control-local-broadcast", func(args ...any) {
		_ = server.Local().Emit("local-event", args[0])
		replyClusterAck(args)
	})
	_ = client.On("control-broadcast-ack", func(args ...any) {
		ack := clusterAck(args)
		server.Timeout(2*time.Second).EmitWithAck("cluster-ack", args[0])(func(responses []any, err error) {
			ack([]any{responses}, err)
		})
	})
	_ = client.On("control-binary-broadcast-ack", func(args ...any) {
		ack := clusterAck(args)
		server.Timeout(2*time.Second).EmitWithAck("cluster-binary-ack", args[0])(func(responses []any, err error) {
			ack([]any{responses}, err)
		})
	})
	_ = client.On("control-empty-broadcast-ack", func(args ...any) {
		ack := clusterAck(args)
		server.To("room-without-clients").Timeout(250*time.Millisecond).EmitWithAck("cluster-empty-ack", args[0])(func(responses []any, err error) {
			ack([]any{map[string]any{"responses": responses, "timedOut": err != nil}}, nil)
		})
	})
	_ = client.On("control-timeout-broadcast-ack", func(args ...any) {
		ack := clusterAck(args)
		server.Timeout(250*time.Millisecond).EmitWithAck("cluster-timeout-ack", args[0])(func(responses []any, err error) {
			ack([]any{map[string]any{"responses": responses, "timedOut": err != nil}}, nil)
		})
	})
	_ = client.On("control-fetch-sockets", func(args ...any) {
		ack := clusterAck(args)
		server.FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
			ack([]any{len(sockets)}, err)
		})
	})
	_ = client.On("control-sockets-join", func(args ...any) {
		server.SocketsJoin(socket.Room(fmt.Sprint(args[0])))
		replyClusterAck(args)
	})
	_ = client.On("control-sockets-join-matching", func(args ...any) {
		server.In(socket.Room(fmt.Sprint(args[0]))).SocketsJoin(socket.Room(fmt.Sprint(args[1])))
		replyClusterAck(args)
	})
	_ = client.On("control-sockets-leave", func(args ...any) {
		server.SocketsLeave(socket.Room(fmt.Sprint(args[0])))
		replyClusterAck(args)
	})
	_ = client.On("control-sockets-leave-matching", func(args ...any) {
		server.In(socket.Room(fmt.Sprint(args[0]))).SocketsLeave(socket.Room(fmt.Sprint(args[1])))
		replyClusterAck(args)
	})
	_ = client.On("control-disconnect-room", func(args ...any) {
		server.In(socket.Room(fmt.Sprint(args[0]))).DisconnectSockets(false)
		replyClusterAck(args)
	})
	_ = client.On("control-server-side-ack", func(args ...any) {
		ack := clusterAck(args)
		if err := server.ServerSideEmitWithAck("cluster-server-ack", args[0])(func(responses []any, err error) {
			ack([]any{responses}, err)
		}); err != nil {
			ack(nil, err)
		}
	})
	_ = client.On("control-server-side-event", func(args ...any) {
		_ = server.ServerSideEmit("cluster-server-event", args[0])
		replyClusterAck(args)
	})
	_ = client.On("control-server-side-timeout", func(args ...any) {
		ack := clusterAck(args)
		if err := server.ServerSideEmitWithAck("cluster-server-timeout", args[0])(func(responses []any, err error) {
			ack([]any{map[string]any{"responses": responses, "timedOut": err != nil}}, nil)
		}); err != nil {
			ack([]any{map[string]any{"responses": []any{}, "timedOut": true}}, nil)
		}
	})
	_ = client.On("control-disconnect-all", func(args ...any) {
		replyClusterAck(args)
		time.AfterFunc(25*time.Millisecond, func() { server.DisconnectSockets(false) })
	})
}

func clusterWorkerOpenFileDescriptors() uint64 {
	for _, directory := range []string{"/proc/self/fd", "/dev/fd"} {
		entries, err := os.ReadDir(directory)
		if err == nil {
			return uint64(len(entries))
		}
	}
	return 0
}

func TestUnixClusterWorkerProcess(t *testing.T) {
	if os.Getenv(clusterWorkerEnv) != "1" {
		t.Skip("helper process")
	}
	socketPath := os.Getenv("SOCKET_IO_UNIX_CLUSTER_PATH")
	listenAddress := os.Getenv("SOCKET_IO_UNIX_CLUSTER_ADDR")
	nodeID := os.Getenv("SOCKET_IO_UNIX_CLUSTER_NODE")
	if socketPath == "" || listenAddress == "" || nodeID == "" {
		t.Fatal("missing Unix cluster worker configuration")
	}

	client := unixbridge.NewUnixClient(t.Context(), socketPath)
	defer func() { _ = client.Close() }()
	opts := DefaultUnixAdapterOptions()
	opts.SetHeartbeatInterval(100 * time.Millisecond)
	opts.SetHeartbeatTimeout(500)
	serverOpts := socket.DefaultServerOptions()
	pingInterval := time.Second
	if configured := os.Getenv("SOCKET_IO_UNIX_CLUSTER_WORKER_PING_INTERVAL"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid worker ping interval %q", configured)
		}
		pingInterval = parsed
	}
	pingTimeout := 2 * time.Second
	if configured := os.Getenv("SOCKET_IO_UNIX_CLUSTER_WORKER_PING_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid worker ping timeout %q", configured)
		}
		pingTimeout = parsed
	}
	serverOpts.SetPingInterval(pingInterval)
	serverOpts.SetPingTimeout(pingTimeout)
	serverOpts.SetAdapter(&UnixAdapterBuilder{Unix: client, Opts: opts})
	server := socket.NewServer(nil, serverOpts)
	server.Of("/", func(args ...any) {
		registerClusterSocketHandlers(server, args[0].(*socket.Socket), nodeID, false)
	})
	server.Of("/custom", func(args ...any) {
		registerClusterSocketHandlers(server, args[0].(*socket.Socket), nodeID, true)
	})
	_ = server.Sockets().On("cluster-server-ack", func(args ...any) {
		if ack := clusterAck(args); ack != nil {
			ack([]any{nodeID + ":" + fmt.Sprint(args[0])}, nil)
		}
	})
	_ = server.Sockets().On("cluster-server-event", func(args ...any) {
		_ = server.Local().Emit("server-side-observed", nodeID+":"+fmt.Sprint(args[0]))
	})
	_ = server.Sockets().On("cluster-server-timeout", func(args ...any) {
		if nodeID == "node-3" {
			return
		}
		if ack := clusterAck(args); ack != nil {
			ack([]any{nodeID + ":" + fmt.Sprint(args[0])}, nil)
		}
	})
	engineOptions := engineconfig.DefaultServerOptions()
	engineOptions.SetPingInterval(50 * time.Millisecond)
	engineOptions.SetPingTimeout(500 * time.Millisecond)
	engineServer := engine.NewServer(engineOptions)
	_ = engineServer.On("connection", func(args ...any) {
		connection := args[0].(engine.Socket)
		connection.Send(types.NewStringBufferString("node:"+nodeID), nil, nil)
		_ = connection.On("message", func(messageArgs ...any) {
			message, ok := messageArgs[0].(types.BufferInterface)
			if !ok {
				return
			}
			switch message.String() {
			case "__deferred__":
				time.AfterFunc(100*time.Millisecond, func() {
					connection.Send(types.NewStringBufferString("deferred"), nil, nil)
				})
			case "__close__":
				connection.Close(false)
			default:
				connection.Send(message.Clone(), nil, nil)
			}
		})
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/debug/resources", func(w http.ResponseWriter, _ *http.Request) {
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		namespace := server.Sockets()
		namespaceAdapter := namespace.Adapter()
		_ = json.NewEncoder(w).Encode(map[string]uint64{
			"goroutines":       uint64(runtime.NumGoroutine()),
			"heapAlloc":        memory.HeapAlloc,
			"heapObjects":      memory.HeapObjects,
			"stackInuse":       memory.StackInuse,
			"openFDs":          clusterWorkerOpenFileDescriptors(),
			"socketClients":    uint64(namespace.Sockets().Len()),
			"engineClients":    server.Engine().ClientsCount(),
			"adapterRooms":     uint64(namespaceAdapter.Rooms().Len()),
			"adapterSids":      uint64(namespaceAdapter.Sids().Len()),
			"rawEngineClients": engineServer.ClientsCount(),
		})
	})
	mux.HandleFunc("/debug/goroutines", func(w http.ResponseWriter, _ *http.Request) {
		_ = pprof.Lookup("goroutine").WriteTo(w, 2)
	})
	mux.Handle("/socket.io/", server.ServeHandler(nil))
	mux.Handle("/engine.io/", engineServer)
	httpServer := &http.Server{Addr: listenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if configuredFD := os.Getenv(clusterWorkerParentFDEnv); configuredFD != "" {
		parentFD, err := strconv.Atoi(configuredFD)
		if err != nil || parentFD < 0 {
			t.Fatalf("invalid parent lifecycle descriptor %q", configuredFD)
		}
		parentSignal := os.NewFile(uintptr(parentFD), "cluster-parent-lifecycle")
		if parentSignal == nil {
			t.Fatalf("could not open parent lifecycle descriptor %d", parentFD)
		}
		defer func() { _ = parentSignal.Close() }()
		go func() {
			_, _ = io.Copy(io.Discard, parentSignal)
			_ = httpServer.Close()
		}()
	}
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}

type clusterWorker struct {
	id           string
	address      string
	command      *exec.Cmd
	parentSignal *os.File
	stopOnce     sync.Once
}

func (w *clusterWorker) stop() {
	if w == nil || w.command == nil {
		return
	}
	w.stopOnce.Do(func() {
		if w.parentSignal != nil {
			_ = w.parentSignal.Close()
		}
		done := make(chan struct{})
		go func() {
			_ = w.command.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = w.command.Process.Kill()
			<-done
		}
	})
}

func allocateAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func startClusterWorker(t *testing.T, socketPath, nodeID string) *clusterWorker {
	t.Helper()
	address := allocateAddress(t)
	parentRead, parentWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestUnixClusterWorkerProcess$", "-test.v")
	command.Env = append(os.Environ(),
		clusterWorkerEnv+"=1",
		clusterWorkerParentFDEnv+"=3",
		"SOCKET_IO_UNIX_CLUSTER_PATH="+socketPath,
		"SOCKET_IO_UNIX_CLUSTER_ADDR="+address,
		"SOCKET_IO_UNIX_CLUSTER_NODE="+nodeID,
	)
	command.ExtraFiles = []*os.File{parentRead}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		_ = parentRead.Close()
		_ = parentWrite.Close()
		t.Fatal(err)
	}
	_ = parentRead.Close()
	worker := &clusterWorker{id: nodeID, address: address, command: command, parentSignal: parentWrite}
	t.Cleanup(func() {
		worker.stop()
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://" + address + "/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return worker
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("worker %s did not become ready", nodeID)
	return nil
}

func TestUnixClusterWorkerStopsOnParentSignal(t *testing.T) {
	worker := startClusterWorker(t, filepath.Join(t.TempDir(), "cluster.sock"), "lifecycle")
	worker.stop()
	if worker.command.ProcessState == nil || !worker.command.ProcessState.Exited() {
		t.Fatal("worker did not exit after its parent lifecycle pipe closed")
	}
	if !worker.command.ProcessState.Success() {
		t.Fatalf("worker did not exit cleanly: %s", worker.command.ProcessState)
	}
}

func TestOfficialClusterDeploymentEquivalence(t *testing.T) {
	if os.Getenv(clusterOfficialInteropEnv) != "1" {
		t.Skip("set SOCKET_IO_CLUSTER_OFFICIAL_INTEROP=1 after npm ci in testdata/cluster-official")
	}
	socketPath := filepath.Join(os.TempDir(), "sio-cluster-"+strconv.Itoa(os.Getpid())+".sock")
	matches, _ := filepath.Glob(socketPath + ".*")
	for _, match := range matches {
		_ = os.Remove(match)
	}
	t.Cleanup(func() {
		matches, _ := filepath.Glob(socketPath + ".*")
		for _, match := range matches {
			_ = os.Remove(match)
		}
	})

	workers := []*clusterWorker{
		startClusterWorker(t, socketPath, "node-1"),
		startClusterWorker(t, socketPath, "node-2"),
		startClusterWorker(t, socketPath, "node-3"),
	}
	router, err := sticky.New(sticky.Options{LoadBalancingMethod: sticky.RoundRobin})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	for _, worker := range workers {
		target, _ := url.Parse("http://" + worker.address)
		if addErr := router.AddBackend(worker.id, target); addErr != nil {
			t.Fatal(addErr)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyServer := &http.Server{Handler: router, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = proxyServer.Serve(listener) }()
	t.Cleanup(func() { _ = proxyServer.Close() })

	matrixPath := filepath.Join("..", "testdata", "cluster-official", "matrix.cjs")
	command := exec.Command("node", matrixPath)
	command.Env = append(os.Environ(), "SOCKET_IO_CLUSTER_URL=http://"+listener.Addr().String())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("official cluster deployment matrix failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"clusterAdapterOfficialCases":18`) ||
		!strings.Contains(string(output), `"engineGatewayBoundary":"sticky-owner"`) ||
		!strings.Contains(string(output), `"workerDistribution":"pass"`) ||
		!strings.Contains(string(output), `"remoteClose":"pass"`) ||
		!strings.Contains(string(output), `"pollingSticky":"pass"`) {
		t.Fatalf("matrix did not report the expected coverage: %s", output)
	}

	// Simulate an ungraceful worker crash. The stale Unix socket is deliberately
	// left behind so this also exercises heartbeat-based membership convergence.
	workers[0].stop()
	router.RemoveBackend(workers[0].id)
	time.Sleep(1500 * time.Millisecond)
	runClusterFaultMatrix(t, listener.Addr().String(), 1)

	// Add a fresh process and prove the old and new processes overlap before the
	// new worker starts receiving cluster requests (rolling replacement).
	replacement := startClusterWorker(t, socketPath, "node-4")
	replacementTarget, _ := url.Parse("http://" + replacement.address)
	if err := router.AddBackend(replacement.id, replacementTarget); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	runClusterFaultMatrix(t, listener.Addr().String(), 2)
}

func runClusterFaultMatrix(t *testing.T, proxyAddress string, expectedPeers int) {
	t.Helper()
	matrixPath := filepath.Join("..", "testdata", "cluster-official", "fault.cjs")
	command := exec.Command("node", matrixPath)
	command.Env = append(os.Environ(),
		"SOCKET_IO_CLUSTER_URL=http://"+proxyAddress,
		"SOCKET_IO_CLUSTER_EXPECTED_PEERS="+strconv.Itoa(expectedPeers),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("cluster fault matrix failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"convergence":"pass"`) {
		t.Fatalf("fault matrix did not report convergence: %s", output)
	}
}
