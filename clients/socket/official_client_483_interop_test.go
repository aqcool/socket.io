package socket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	engineclient "github.com/aqcool/socket.io/clients/engine/v4"
	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

const officialClientInteropEnvironment = "SOCKET_IO_CLIENT_OFFICIAL_INTEROP"

type officialClientFixtureReady struct {
	Type string `json:"type"`
	Port int    `json:"port"`
}

func copyOfficialClientFixtureFile(t *testing.T, destination, name string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "official-4.8.3", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func startOfficialClient483Fixture(t *testing.T) string {
	t.Helper()
	if os.Getenv(officialClientInteropEnvironment) != "1" {
		t.Skipf("set %s=1 to run against socket.io@4.8.3", officialClientInteropEnvironment)
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("Node.js is unavailable: %v", err)
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skipf("npm is unavailable: %v", err)
	}

	directory := t.TempDir()
	for _, name := range []string{"package.json", "package-lock.json", "server.cjs"} {
		copyOfficialClientFixtureFile(t, directory, name)
	}
	installContext, cancelInstall := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelInstall()
	install := exec.CommandContext(installContext, "npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund")
	install.Dir = directory
	if output, err := install.CombinedOutput(); err != nil {
		t.Fatalf("install socket.io@4.8.3: %v\n%s", err, output)
	}

	command := exec.Command("node", "server.cjs")
	command.Dir = directory
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-wait:
		case <-time.After(3 * time.Second):
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			<-wait
		}
		if t.Failed() && stderr.Len() > 0 {
			t.Logf("official fixture stderr:\n%s", stderr.String())
		}
	})

	readyLine := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			readyLine <- scanner.Text()
			return
		}
		readyLine <- ""
	}()
	var line string
	select {
	case line = <-readyLine:
	case err := <-wait:
		t.Fatalf("official fixture exited before READY: %v\n%s", err, stderr.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for official fixture READY\n%s", stderr.String())
	}
	var ready officialClientFixtureReady
	if err := json.Unmarshal([]byte(line), &ready); err != nil || ready.Type != "READY" || ready.Port == 0 {
		t.Fatalf("invalid official fixture READY record %q: %v\n%s", line, err, stderr.String())
	}
	return fmt.Sprintf("http://127.0.0.1:%d", ready.Port)
}

func officialClient483Options() *Options {
	options := DefaultOptions()
	options.SetAutoConnect(false)
	options.SetForceNew(true)
	// Keep this suite focused on the Socket.IO layer. The Engine.IO client
	// module owns the per-transport and upgrade matrix; one stable carrier is
	// sufficient for these Socket.IO protocol assertions.
	options.SetTransportList([]engineclient.TransportCtor{&engineclient.WebSocketBuilder{}})
	options.SetTimeout(2 * time.Second)
	options.SetReconnectionDelay(10)
	options.SetReconnectionDelayMax(20)
	options.SetRandomizationFactor(0)
	return options
}

func connectOfficialClient483(t *testing.T, uri string, options *Options) *Socket {
	t.Helper()
	socket, err := Connect(uri, options)
	if err != nil {
		t.Fatal(err)
	}
	connected := make(chan struct{}, 1)
	connectError := make(chan error, 1)
	if err := socket.Once("connect", func(...any) { connected <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	if err := socket.Once("connect_error", func(args ...any) {
		if len(args) > 0 {
			connectError <- args[0].(error)
		}
	}); err != nil {
		t.Fatal(err)
	}
	socket.Connect()
	select {
	case <-connected:
	case err := <-connectError:
		t.Fatalf("connect to official socket.io@4.8.3: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out connecting to official socket.io@4.8.3")
	}
	t.Cleanup(func() { socket.Close() })
	return socket
}

func officialClient483Ack(t *testing.T, socket *Socket, event string, args ...any) []any {
	t.Helper()
	result := make(chan struct {
		args []any
		err  error
	}, 1)
	socket.EmitWithAck(event, args...)(func(args []any, err error) {
		result <- struct {
			args []any
			err  error
		}{args: args, err: err}
	})
	select {
	case result := <-result:
		if result.err != nil {
			t.Fatalf("official acknowledgement for %q: %v", event, result.err)
		}
		return result.args
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for official acknowledgement for %q", event)
		return nil
	}
}

func officialClient483Bytes(t *testing.T, value any) []byte {
	t.Helper()
	switch value := value.(type) {
	case *types.BytesBuffer:
		return value.Bytes()
	case types.BufferInterface:
		return value.Bytes()
	case []byte:
		return value
	default:
		t.Fatalf("binary value has type %T", value)
		return nil
	}
}

func officialClient483EnginePacketData(value any) string {
	packet, ok := value.(*enginepacket.Packet)
	if !ok || packet == nil || packet.Data == nil {
		return ""
	}
	switch data := packet.Data.(type) {
	case *types.StringBuffer:
		return data.String()
	case types.BufferInterface:
		return string(data.Bytes())
	default:
		return ""
	}
}

func TestOfficialClient483NodeInterop(t *testing.T) {
	baseURL := startOfficialClient483Fixture(t)

	t.Run("default namespace, query, auth, scalar and UTF-8 acknowledgements", func(t *testing.T) {
		options := officialClient483Options()
		options.SetAuth(map[string]any{"token": "static-token"})
		socket := connectOfficialClient483(t, baseURL+"/?query=a%20b", options)
		id := officialClient483Ack(t, socket, "get-id")
		if len(id) != 1 || id[0] != socket.Id() || socket.Id() == "" {
			t.Fatalf("official id acknowledgement = %#v, local id = %q", id, socket.Id())
		}
		values := []any{false, "てすと", "Я Б Г Д Ж Й", "utf8 — string"}
		if got := officialClient483Ack(t, socket, "echo", values...); !reflect.DeepEqual(got, values) {
			t.Fatalf("official scalar/UTF-8 acknowledgement = %#v, want %#v", got, values)
		}
		handshakeArgs := officialClient483Ack(t, socket, "get-handshake")
		handshake, ok := handshakeArgs[0].(map[string]any)
		if !ok {
			t.Fatalf("official handshake type = %T", handshakeArgs[0])
		}
		query, _ := handshake["query"].(map[string]any)
		auth, _ := handshake["auth"].(map[string]any)
		if query["query"] != "a b" || auth["token"] != "static-token" {
			t.Fatalf("official handshake = %#v", handshake)
		}
		date := officialClient483Ack(t, socket, "date-ack")
		if !reflect.DeepEqual(date, []any{"2024-01-02T03:04:05.000Z"}) {
			t.Fatalf("official Date acknowledgement = %#v", date)
		}

		clientAck := make(chan struct{}, 1)
		if err := socket.Once("client-ack", func(args ...any) {
			if len(args) != 3 || args[0] != float64(5) {
				return
			}
			ack, ok := args[2].(func([]any, error))
			if !ok {
				return
			}
			ack([]any{"client-acknowledged"}, nil)
			clientAck <- struct{}{}
		}); err != nil {
			t.Fatal(err)
		}
		if got := officialClient483Ack(t, socket, "request-client-ack"); !reflect.DeepEqual(got, []any{"client-acknowledged"}) {
			t.Fatalf("client acknowledgement round-trip = %#v", got)
		}
		select {
		case <-clientAck:
		default:
			t.Fatal("official server event acknowledgement did not run")
		}
	})

	t.Run("custom namespace", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL+"/custom", officialClient483Options())
		if socket.Nsp() != "/custom" || socket.Id() == "" {
			t.Fatalf("custom namespace state: nsp=%q id=%q", socket.Nsp(), socket.Id())
		}
		if got := officialClient483Ack(t, socket, "echo", "custom"); !reflect.DeepEqual(got, []any{"custom"}) {
			t.Fatalf("custom namespace acknowledgement = %#v", got)
		}
	})

	t.Run("namespace opened after the shared manager is connected", func(t *testing.T) {
		options := officialClient483Options()
		manager := NewManager(baseURL, options)
		root := manager.Socket("/", nil)
		rootConnected := make(chan struct{}, 1)
		if err := root.Once("connect", func(...any) { rootConnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		root.Connect()
		select {
		case <-rootConnected:
		case <-time.After(3 * time.Second):
			t.Fatal("default namespace did not connect")
		}
		engine := manager.Engine()
		custom := manager.Socket("/custom", nil)
		customConnected := make(chan struct{}, 1)
		if err := custom.Once("connect", func(...any) { customConnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		custom.Connect()
		select {
		case <-customConnected:
		case <-time.After(3 * time.Second):
			t.Fatal("custom namespace did not connect on the existing Manager")
		}
		if manager.Engine() != engine || root.Io() != custom.Io() {
			t.Fatal("opening a namespace created another Engine.IO connection")
		}
		custom.Close()
		root.Close()
	})

	t.Run("manual disconnect and reconnect", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		initialID := socket.Id()
		disconnected := make(chan string, 1)
		if err := socket.Once("disconnect", func(args ...any) { disconnected <- args[0].(string) }); err != nil {
			t.Fatal(err)
		}
		socket.Disconnect()
		select {
		case reason := <-disconnected:
			if reason != "io client disconnect" {
				t.Fatalf("manual disconnect reason = %q", reason)
			}
		case <-time.After(time.Second):
			t.Fatal("manual disconnect event was not emitted")
		}
		if socket.Active() || socket.Connected() || socket.Id() != "" {
			t.Fatalf("state after manual disconnect: active=%v connected=%v id=%q", socket.Active(), socket.Connected(), socket.Id())
		}
		reconnected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { reconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case <-reconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("manual reconnect did not complete")
		}
		if socket.Id() == "" || socket.Id() == initialID || socket.Recovered() {
			t.Fatalf("manual reconnect state: initial=%q current=%q recovered=%v", initialID, socket.Id(), socket.Recovered())
		}
	})

	t.Run("server namespace disconnect requires manual reconnect", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		disconnected := make(chan string, 1)
		if err := socket.Once("disconnect", func(args ...any) { disconnected <- args[0].(string) }); err != nil {
			t.Fatal(err)
		}
		if err := socket.Emit("disconnect-namespace"); err != nil {
			t.Fatal(err)
		}
		select {
		case reason := <-disconnected:
			if reason != "io server disconnect" {
				t.Fatalf("server disconnect reason = %q", reason)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("server namespace disconnect was not received")
		}
		time.Sleep(50 * time.Millisecond)
		if socket.Active() || socket.Connected() {
			t.Fatalf("server-disconnected socket unexpectedly active: active=%v connected=%v", socket.Active(), socket.Connected())
		}
		reconnected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { reconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case <-reconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("manual reconnect after server disconnect did not complete")
		}
	})

	t.Run("dynamic auth provider", func(t *testing.T) {
		options := officialClient483Options()
		options.SetAuthProvider(func(ctx context.Context) (map[string]any, error) {
			if ctx == nil {
				return nil, fmt.Errorf("nil auth context")
			}
			return map[string]any{"token": "accepted"}, nil
		})
		socket := connectOfficialClient483(t, baseURL+"/auth", options)
		handshakeArgs := officialClient483Ack(t, socket, "get-handshake")
		handshake := handshakeArgs[0].(map[string]any)
		auth := handshake["auth"].(map[string]any)
		if auth["token"] != "accepted" {
			t.Fatalf("dynamic auth = %#v", auth)
		}
	})

	t.Run("explicit query object on a custom namespace", func(t *testing.T) {
		options := officialClient483Options()
		options.SetQuery(url.Values{"key": {"a b"}, "special": {"&="}})
		socket := connectOfficialClient483(t, baseURL+"/custom", options)
		handshakeArgs := officialClient483Ack(t, socket, "get-handshake")
		handshake := handshakeArgs[0].(map[string]any)
		query := handshake["query"].(map[string]any)
		if query["key"] != "a b" || query["special"] != "&=" {
			t.Fatalf("explicit custom namespace query = %#v", query)
		}
	})

	t.Run("explicit query object on the default namespace", func(t *testing.T) {
		options := officialClient483Options()
		options.SetQuery(url.Values{"key": {"a b"}, "special": {"&="}})
		socket := connectOfficialClient483(t, baseURL, options)
		handshakeArgs := officialClient483Ack(t, socket, "get-handshake")
		handshake := handshakeArgs[0].(map[string]any)
		query := handshake["query"].(map[string]any)
		if query["key"] != "a b" || query["special"] != "&=" {
			t.Fatalf("explicit default namespace query = %#v", query)
		}
	})

	t.Run("URI query string on a custom namespace", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL+"/custom?key=a%20b&special=%26%3D", officialClient483Options())
		handshakeArgs := officialClient483Ack(t, socket, "get-handshake")
		handshake := handshakeArgs[0].(map[string]any)
		query := handshake["query"].(map[string]any)
		if query["key"] != "a b" || query["special"] != "&=" {
			t.Fatalf("custom namespace URI query = %#v", query)
		}
	})

	t.Run("middleware rejection carries the official message and does not reconnect", func(t *testing.T) {
		options := officialClient483Options()
		socket, err := Connect(baseURL+"/reject", options)
		if err != nil {
			t.Fatal(err)
		}
		connectError := make(chan error, 1)
		reconnectAttempt := make(chan struct{}, 1)
		if err := socket.Once("connect_error", func(args ...any) { connectError <- args[0].(error) }); err != nil {
			t.Fatal(err)
		}
		if err := socket.Io().On("reconnect_attempt", func(...any) { reconnectAttempt <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case err := <-connectError:
			if err.Error() != "middleware rejected" {
				t.Fatalf("middleware error = %q", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("middleware connect_error was not emitted")
		}
		select {
		case <-reconnectAttempt:
			t.Fatal("namespace middleware rejection triggered a Manager reconnection")
		case <-time.After(100 * time.Millisecond):
		}
		socket.Close()
	})

	t.Run("binary payloads and sequential acknowledgements", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		binary := types.NewBytesBuffer([]byte{1, 2, 3, 4})
		ack := officialClient483Ack(t, socket, "binary", binary)
		if got := officialClient483Bytes(t, ack[0]); !bytes.Equal(got, []byte{1, 2, 3, 4}) {
			t.Fatalf("official binary acknowledgement = %v", got)
		}
		nestedAck := officialClient483Ack(t, socket, "binary-nested", map[string]any{
			"payload": types.NewBytesBuffer([]byte{1, 2, 3, 4}),
			"text":    "ok",
		})
		nested := nestedAck[0].(map[string]any)
		if nested["text"] != "ok" || !bytes.Equal(officialClient483Bytes(t, nested["payload"]), []byte{1, 2, 3, 4}) {
			t.Fatalf("official nested binary acknowledgement = %#v", nested)
		}

		got := make([]float64, 0, 3)
		for _, value := range []float64{1, 2, 3} {
			ack := officialClient483Ack(t, socket, "ordered", value)
			if len(ack) != 1 {
				t.Fatalf("ordered acknowledgement = %#v", ack)
			}
			got = append(got, ack[0].(float64))
		}
		if !reflect.DeepEqual(got, []float64{1, 2, 3}) {
			t.Fatalf("official acknowledgement order = %#v", got)
		}
	})

	t.Run("buffered and connect-handler events preserve order", func(t *testing.T) {
		options := officialClient483Options()
		socket, err := Connect(baseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		order := make(chan struct {
			name string
			err  error
		}, 2)
		if err := socket.Emit("echo", "first", func(_ []any, err error) {
			order <- struct {
				name string
				err  error
			}{name: "first", err: err}
		}); err != nil {
			t.Fatal(err)
		}
		if err := socket.Once("connect", func(...any) {
			if err := socket.Emit("echo", "second", func(_ []any, err error) {
				order <- struct {
					name string
					err  error
				}{name: "second", err: err}
			}); err != nil {
				order <- struct {
					name string
					err  error
				}{name: "second", err: err}
			}
		}); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		for _, want := range []string{"first", "second"} {
			select {
			case got := <-order:
				if got.name != want || got.err != nil {
					t.Fatalf("event acknowledgement order: got %q err=%v, want %q", got.name, got.err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for %q acknowledgement", want)
			}
		}
		socket.Close()
	})

	t.Run("binary attachment completes before the following event", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		result := make(chan []any, 1)
		disconnected := make(chan []any, 1)
		if err := socket.Once("ordered-binary-ack", func(args ...any) { result <- args }); err != nil {
			t.Fatal(err)
		}
		if err := socket.Once("disconnect", func(args ...any) {
			if len(args) > 1 {
				if details, ok := args[1].(*engineclient.Error); ok {
					args[1] = fmt.Sprintf("%s: %v", details.Message, details.Description)
				}
			}
			disconnected <- args
		}); err != nil {
			t.Fatal(err)
		}
		if err := socket.Emit("ordered-binary-first", types.NewBytesBuffer([]byte("binary-first"))); err != nil {
			t.Fatal(err)
		}
		if err := socket.Emit("ordered-binary-second", "text-second"); err != nil {
			t.Fatal(err)
		}
		select {
		case args := <-result:
			if len(args) < 2 || args[0] != true || args[1] != "text-second" {
				t.Fatalf("official binary event order result = %#v", args)
			}
		case details := <-disconnected:
			t.Fatalf("connection closed during ordered binary events: %#v", details)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for ordered binary event acknowledgement; connected=%v engine=%v", socket.Connected(), socket.Io().Engine().ReadyState())
		}
	})

	t.Run("compression flag and acknowledgement timeout", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		transportName := socket.Io().Engine().Transport().Name()
		disconnected := make(chan string, 1)
		if err := socket.Once("disconnect", func(args ...any) {
			disconnected <- fmt.Sprintf("%v", args)
		}); err != nil {
			t.Fatal(err)
		}
		packetCreated := make(chan *enginepacket.Packet, 2)
		if err := socket.Io().Engine().On("packetCreate", func(args ...any) {
			packetCreated <- args[0].(*enginepacket.Packet)
		}); err != nil {
			t.Fatal(err)
		}
		disabled := false
		for index, test := range []struct {
			compress *bool
			want     bool
		}{
			{want: true},
			{compress: &disabled, want: false},
		} {
			acknowledged := make(chan error, 1)
			if test.compress != nil {
				socket.Compress(*test.compress)
			}
			if err := socket.Emit("echo", fmt.Sprintf("compression-%d", index), func(_ []any, err error) {
				acknowledged <- err
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case packet := <-packetCreated:
				if packet.Options == nil || packet.Options.Compress == nil || *packet.Options.Compress != test.want {
					t.Fatalf("packet %d compression options = %#v, want %v", index, packet.Options, test.want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for packetCreate")
			}
			select {
			case err := <-acknowledged:
				if err != nil {
					t.Fatalf("compression packet %d acknowledgement: %v", index, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("compression packet %d was not acknowledged", index)
			}
		}

		timedOut := make(chan error, 1)
		if err := socket.Timeout(25*time.Millisecond).Emit("never-ack", func(_ []any, err error) {
			timedOut <- err
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-timedOut:
			if err == nil || err.Error() != "operation has timed out" {
				var disconnectDetails string
				select {
				case disconnectDetails = <-disconnected:
				default:
				}
				t.Fatalf("official ack timeout = %v (transport=%s disconnect=%s)", err, transportName, disconnectDetails)
			}
		case <-time.After(time.Second):
			t.Fatal("official ack timeout callback did not run")
		}
	})

	t.Run("late acknowledgement is ignored after timeout", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		results := make(chan error, 2)
		if err := socket.Timeout(20*time.Millisecond).Emit("delayed-ack", float64(100), "late", func(_ []any, err error) {
			results <- err
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-results:
			if err == nil || err.Error() != "operation has timed out" {
				t.Fatalf("late acknowledgement timeout = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("late acknowledgement did not time out")
		}
		time.Sleep(150 * time.Millisecond)
		select {
		case err := <-results:
			t.Fatalf("late acknowledgement invoked callback twice: %v", err)
		default:
		}
	})

	t.Run("acknowledgement lifecycle across disconnect", func(t *testing.T) {
		options := officialClient483Options()
		options.SetAckTimeout(time.Second)
		socket := connectOfficialClient483(t, baseURL, options)
		disconnectedAck := make(chan error, 1)
		if err := socket.Timeout(time.Second).Emit("delayed-ack", float64(200), "too-late", func(_ []any, err error) {
			disconnectedAck <- err
		}); err != nil {
			t.Fatal(err)
		}
		socket.Disconnect()
		select {
		case err := <-disconnectedAck:
			if err == nil || err.Error() != "socket has been disconnected" {
				t.Fatalf("disconnect acknowledgement error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("pending acknowledgement was not failed on disconnect")
		}

		bufferedAck := make(chan struct {
			args []any
			err  error
		}, 1)
		if err := socket.Emit("echo", "buffered", func(args []any, err error) {
			bufferedAck <- struct {
				args []any
				err  error
			}{args: args, err: err}
		}); err != nil {
			t.Fatal(err)
		}
		if socket.SendBuffer().Len() != 1 {
			t.Fatalf("unsent acknowledgement packet was not buffered: %d", socket.SendBuffer().Len())
		}
		reconnected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { reconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case <-reconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("socket did not reconnect for buffered acknowledgement")
		}
		select {
		case result := <-bufferedAck:
			if result.err != nil || !reflect.DeepEqual(result.args, []any{"buffered"}) {
				t.Fatalf("buffered acknowledgement = args %#v err %v", result.args, result.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("buffered acknowledgement was not delivered after reconnect")
		}

		defaultTimeoutAck := make(chan error, 1)
		if err := socket.Emit("delayed-ack", float64(200), "too-late-default", func(_ []any, err error) {
			defaultTimeoutAck <- err
		}); err != nil {
			t.Fatal(err)
		}
		socket.Disconnect()
		select {
		case err := <-defaultTimeoutAck:
			if err == nil || err.Error() != "socket has been disconnected" {
				t.Fatalf("default-timeout disconnect acknowledgement error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("default-timeout acknowledgement was not failed on disconnect")
		}
	})

	t.Run("volatile packet sends on a writable connection", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		_ = officialClient483Ack(t, socket, "echo", "make-transport-idle")
		result := make(chan struct {
			args []any
			err  error
		}, 1)
		if err := socket.Volatile().Emit("echo", "volatile", func(args []any, err error) {
			result <- struct {
				args []any
				err  error
			}{args: args, err: err}
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case result := <-result:
			if result.err != nil || !reflect.DeepEqual(result.args, []any{"volatile"}) {
				t.Fatalf("volatile acknowledgement = args %#v err %v", result.args, result.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("writable volatile packet was not delivered")
		}
	})

	t.Run("Go time is serialized as an ISO date string", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		value := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
		ack := officialClient483Ack(t, socket, "receive-date", value)
		if !reflect.DeepEqual(ack, []any{"string", "2024-01-02T03:04:05Z"}) {
			t.Fatalf("official server received Go time as %#v", ack)
		}
		nestedAck := officialClient483Ack(t, socket, "receive-date-object", map[string]any{"when": value})
		if !reflect.DeepEqual(nestedAck, []any{"string", "2024-01-02T03:04:05Z"}) {
			t.Fatalf("official server received nested Go time as %#v", nestedAck)
		}
	})

	t.Run("retry queue preserves order and exhausts attempts", func(t *testing.T) {
		options := officialClient483Options()
		options.SetRetries(1)
		options.SetAckTimeout(25 * time.Millisecond)
		socket, err := Connect(baseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		seen := make(chan struct{}, 2)
		if err := socket.On("retry-seen", func(...any) { seen <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		failed := make(chan error, 1)
		if err := socket.Emit("retry-never", func(_ []any, err error) { failed <- err }); err != nil {
			t.Fatal(err)
		}
		connected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { connected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case <-connected:
		case <-time.After(3 * time.Second):
			t.Fatal("retry socket did not connect")
		}
		for range 2 {
			select {
			case <-seen:
			case <-time.After(3 * time.Second):
				t.Fatal("official server did not observe both retry attempts")
			}
		}
		select {
		case err := <-failed:
			if err == nil || err.Error() != "operation has timed out" {
				t.Fatalf("retry exhaustion = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("retry exhaustion callback did not run")
		}
		if socket._queue.Len() != 0 {
			t.Fatalf("retry queue length = %d after exhaustion", socket._queue.Len())
		}
		socket.Close()
	})

	t.Run("connection state recovery", func(t *testing.T) {
		socket := connectOfficialClient483(t, baseURL, officialClient483Options())
		initialID := socket.Id()
		recoveryEvent := make(chan struct{}, 1)
		if err := socket.Once("recovery-event", func(...any) { recoveryEvent <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		if err := socket.Emit("init-recovery"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-recoveryEvent:
		case <-time.After(3 * time.Second):
			t.Fatal("did not receive recovery offset event")
		}
		reconnected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { reconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Io().Engine().Close()
		select {
		case <-reconnected:
		case <-time.After(5 * time.Second):
			t.Fatal("client did not reconnect for state recovery")
		}
		if socket.Id() != initialID || !socket.Recovered() {
			t.Fatalf("official recovery state: initial id=%q current id=%q recovered=%v", initialID, socket.Id(), socket.Recovered())
		}
	})

	t.Run("cached inactive socket reopens with the same instance", func(t *testing.T) {
		options := officialClient483Options()
		options.SetAutoConnect(true)
		manager := NewManager(baseURL, options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/", nil)
		connected := make(chan struct{}, 2)
		disconnected := make(chan struct{}, 1)
		if err := socket.On("connect", func(...any) { connected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		if err := socket.Once("disconnect", func(...any) { disconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		select {
		case <-connected:
		case <-time.After(3 * time.Second):
			t.Fatal("cached socket did not initially connect")
		}
		socket.Disconnect()
		select {
		case <-disconnected:
		case <-time.After(time.Second):
			t.Fatal("cached socket did not disconnect")
		}
		cached := manager.Socket("/", nil)
		if cached != socket || !cached.Active() {
			t.Fatalf("cached socket state: same=%v active=%v", cached == socket, cached.Active())
		}
		select {
		case <-connected:
		case <-time.After(3 * time.Second):
			t.Fatal("cached socket did not reopen")
		}
		cached.Disconnect()
	})

	t.Run("active cached sockets do not send duplicate CONNECT packets", func(t *testing.T) {
		options := officialClient483Options()
		manager := NewManager(baseURL, options)
		t.Cleanup(manager._close)
		root := manager.Socket("/", nil)
		custom := manager.Socket("/custom", nil)
		rootConnected := make(chan struct{}, 1)
		customConnected := make(chan struct{}, 1)
		if err := root.Once("connect", func(...any) { rootConnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		if err := custom.Once("connect", func(...any) { customConnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		root.Connect()
		engine := manager.Engine()
		if engine == nil {
			t.Fatal("Manager did not create an Engine")
		}
		var packetMu sync.Mutex
		connectPackets := make(map[string]int)
		if err := engine.On("packetCreate", func(args ...any) {
			data := officialClient483EnginePacketData(args[0])
			if data == "0" || data == "0{}" {
				packetMu.Lock()
				connectPackets["/"]++
				packetMu.Unlock()
			} else if strings.HasPrefix(data, "0/custom,") {
				packetMu.Lock()
				connectPackets["/custom"]++
				packetMu.Unlock()
			}
		}); err != nil {
			t.Fatal(err)
		}
		custom.Connect()
		if manager.Socket("/", nil) != root || manager.Socket("/custom", nil) != custom {
			t.Fatal("Manager did not return its active cached Socket instances")
		}
		select {
		case <-rootConnected:
		case <-time.After(3 * time.Second):
			t.Fatal("root namespace did not connect")
		}
		select {
		case <-customConnected:
		case <-time.After(3 * time.Second):
			t.Fatal("custom namespace did not connect")
		}
		packetMu.Lock()
		rootPackets := connectPackets["/"]
		customPackets := connectPackets["/custom"]
		packetMu.Unlock()
		if rootPackets != 1 || customPackets != 1 {
			t.Fatalf("CONNECT packet counts: root=%d custom=%d", rootPackets, customPackets)
		}
		root.Disconnect()
		custom.Disconnect()
	})

	t.Run("decoding failure closes the old Engine and reconnects with a new one", func(t *testing.T) {
		options := officialClient483Options()
		options.SetReconnectionDelay(10)
		options.SetReconnectionDelayMax(10)
		manager := NewManager(baseURL, options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/", nil)
		connected := make(chan struct{}, 1)
		if err := socket.Once("connect", func(...any) { connected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case <-connected:
		case <-time.After(3 * time.Second):
			t.Fatal("socket did not connect before parser failure")
		}
		oldEngine := manager.Engine()
		reconnected := make(chan struct{}, 1)
		if err := manager.Once("reconnect", func(...any) { reconnected <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		oldEngine.Emit("data", types.NewStringBufferString("bad"))
		select {
		case <-reconnected:
		case <-time.After(5 * time.Second):
			t.Fatal("Manager did not reconnect after parser failure")
		}
		if manager.Engine() == oldEngine || oldEngine.ReadyState() != engineclient.SocketStateClosed {
			t.Fatalf("Engine replacement: same=%v old state=%q", manager.Engine() == oldEngine, oldEngine.ReadyState())
		}
	})
}
