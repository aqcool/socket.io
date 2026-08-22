package socket

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	engineclient "github.com/aqcool/socket.io/clients/engine/v4"
	enginepacket "github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/v4/pkg/types"
	websocket "github.com/gorilla/websocket"
)

type official483EngineState struct {
	engineclient.Socket
	transport   engineclient.Transport
	pingExpired atomic.Bool
	writes      atomic.Int32
}

func (engine *official483EngineState) Transport() engineclient.Transport {
	return engine.transport
}

func (engine *official483EngineState) HasPingExpired() bool {
	return engine.pingExpired.Load()
}

func (engine *official483EngineState) Write(io.Reader, *enginepacket.Options, func()) engineclient.SocketWithoutUpgrade {
	engine.writes.Add(1)
	return engine
}

func installOfficial483EngineState(socket *Socket, writable, pingExpired bool) *official483EngineState {
	transport := engineclient.MakeTransport()
	transport.SetWritable(writable)
	state := &official483EngineState{transport: transport}
	state.pingExpired.Store(pingExpired)
	var engine Engine = state
	socket.io.engine.Store(&engine)
	return state
}

func resetClientManagerCache(t *testing.T) {
	t.Helper()
	cacheMu.Lock()
	cache.Clear()
	cacheMu.Unlock()
	t.Cleanup(func() {
		cacheMu.Lock()
		cache.Clear()
		cacheMu.Unlock()
	})
}

func disconnectedClientOptions() *Options {
	opts := DefaultOptions()
	opts.SetAutoConnect(false)
	return opts
}

func TestOfficialClient483LookupManagerSelection(t *testing.T) {
	t.Run("protocol-less URLs default to HTTPS without a browser location", func(t *testing.T) {
		for _, test := range []struct {
			name string
			uri  string
		}{
			{name: "bare host", uri: "localhost:3000/custom"},
			{name: "protocol relative", uri: "//localhost:3000/custom"},
		} {
			t.Run(test.name, func(t *testing.T) {
				resetClientManagerCache(t)
				socket, err := Connect(test.uri, disconnectedClientOptions())
				if err != nil {
					t.Fatal(err)
				}
				if socket.Nsp() != "/custom" || socket.Io().uri != "https://localhost:3000/custom" {
					t.Fatalf("protocol-less lookup: namespace=%q manager URI=%q", socket.Nsp(), socket.Io().uri)
				}
			})
		}
	})

	t.Run("concurrent same-namespace lookups each get a manager", func(t *testing.T) {
		resetClientManagerCache(t)
		const clients = 20
		start := make(chan struct{})
		managers := make(chan *Manager, clients)
		errors := make(chan error, clients)
		var wait sync.WaitGroup
		wait.Add(clients)
		for range clients {
			go func() {
				defer wait.Done()
				<-start
				socket, err := Connect("http://localhost:3000/same", disconnectedClientOptions())
				if err != nil {
					errors <- err
					return
				}
				managers <- socket.Io()
			}()
		}
		close(start)
		wait.Wait()
		close(errors)
		for err := range errors {
			t.Fatal(err)
		}
		close(managers)
		unique := make(map[*Manager]struct{}, clients)
		for manager := range managers {
			unique[manager] = struct{}{}
		}
		if len(unique) != clients {
			t.Fatalf("unique managers = %d, want %d", len(unique), clients)
		}
	})

	t.Run("default ports share the same URL identity", func(t *testing.T) {
		resetClientManagerCache(t)
		first, err := Connect("http://localhost/first", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		second, err := Connect("http://localhost:80/second", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		if first.Io() != second.Io() {
			t.Fatal("implicit and explicit default ports did not share a Manager")
		}
	})

	t.Run("IPv6 URLs retain namespace and default port identity", func(t *testing.T) {
		resetClientManagerCache(t)
		first, err := Connect("http://[::1]/first", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		second, err := Connect("http://[::1]:80/second", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		if first.Nsp() != "/first" || second.Nsp() != "/second" || first.Io() != second.Io() {
			t.Fatalf("IPv6 lookup mismatch: first=%q second=%q shared=%v", first.Nsp(), second.Nsp(), first.Io() == second.Io())
		}
	})

	t.Run("different namespaces reuse the default multiplexed manager", func(t *testing.T) {
		resetClientManagerCache(t)
		opts := disconnectedClientOptions()
		first, err := Connect("http://localhost:3000/first", opts)
		if err != nil {
			t.Fatal(err)
		}
		second, err := Connect("http://localhost:3000/second", opts)
		if err != nil {
			t.Fatal(err)
		}
		if first.Io() != second.Io() {
			t.Fatal("different namespaces did not reuse the default multiplexed Manager")
		}
	})

	t.Run("same namespace gets a new manager", func(t *testing.T) {
		resetClientManagerCache(t)
		first, err := Connect("http://localhost:3000/", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		second, err := Connect("http://localhost:3000/?woot", disconnectedClientOptions())
		if err != nil {
			t.Fatal(err)
		}
		if first.Io() == second.Io() {
			t.Fatal("same namespace unexpectedly reused a Manager")
		}
	})

	t.Run("different engine paths get different managers", func(t *testing.T) {
		resetClientManagerCache(t)
		fooOptions := disconnectedClientOptions()
		fooOptions.SetPath("/foo")
		barOptions := disconnectedClientOptions()
		barOptions.SetPath("/bar")
		first, err := Connect("http://localhost:3000/first", fooOptions)
		if err != nil {
			t.Fatal(err)
		}
		second, err := Connect("http://localhost:3000/second", barOptions)
		if err != nil {
			t.Fatal(err)
		}
		if first.Io() == second.Io() {
			t.Fatal("different Engine.IO paths unexpectedly reused a Manager")
		}
	})

	t.Run("forceNew and multiplex false bypass the cache", func(t *testing.T) {
		for _, configure := range []func(*Options){
			func(options *Options) { options.SetForceNew(true) },
			func(options *Options) { options.SetMultiplex(false) },
		} {
			resetClientManagerCache(t)
			firstOptions := disconnectedClientOptions()
			configure(firstOptions)
			secondOptions := disconnectedClientOptions()
			configure(secondOptions)
			first, err := Connect("http://localhost:3000/first", firstOptions)
			if err != nil {
				t.Fatal(err)
			}
			second, err := Connect("http://localhost:3000/second", secondOptions)
			if err != nil {
				t.Fatal(err)
			}
			if first.Io() == second.Io() {
				t.Fatal("cache-bypassing option unexpectedly reused a Manager")
			}
		}
	})

	t.Run("URI query is available before the manager opens", func(t *testing.T) {
		resetClientManagerCache(t)
		opts := disconnectedClientOptions()
		socket, err := Connect("http://localhost:3000/custom?token=a%20b&flag", opts)
		if err != nil {
			t.Fatal(err)
		}
		if socket.Nsp() != "/custom" {
			t.Fatalf("namespace = %q, want /custom", socket.Nsp())
		}
		query := socket.Io().Opts().Query()
		if query.Get("token") != "a b" {
			t.Fatalf("query token = %q, want %q", query.Get("token"), "a b")
		}
		if _, ok := query["flag"]; !ok {
			t.Fatal("query flag was not retained")
		}
	})
}

func TestOfficialClient483SocketLifecycleAndRecoveryState(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	if socket.Connected() || !socket.Disconnected() || socket.Id() != "" || socket.Recovered() {
		t.Fatalf("unexpected initial state: connected=%v id=%q recovered=%v", socket.Connected(), socket.Id(), socket.Recovered())
	}

	socket.onconnect("first-id", "private-id")
	if !socket.Connected() || socket.Disconnected() || socket.Id() != "first-id" || socket.Recovered() {
		t.Fatalf("unexpected first connection state: connected=%v id=%q recovered=%v", socket.Connected(), socket.Id(), socket.Recovered())
	}
	socket.onclose("transport close", nil)
	if socket.Connected() || socket.Id() != "" {
		t.Fatalf("disconnect did not clear state: connected=%v id=%q", socket.Connected(), socket.Id())
	}

	socket.onconnect("first-id", "private-id")
	if !socket.Recovered() {
		t.Fatal("matching private session id did not mark the connection as recovered")
	}
	socket.onclose("transport close", nil)
	socket.onconnect("second-id", "another-private-id")
	if socket.Recovered() {
		t.Fatal("different private session id incorrectly marked the connection as recovered")
	}
}

func TestOfficialClient483ConnectionErrorBoundaries(t *testing.T) {
	t.Run("force disconnect while opening suppresses later errors", func(t *testing.T) {
		requestStarted := make(chan struct{})
		var startedOnce sync.Once
		dialCanceled := make(chan struct{})
		var canceledOnce sync.Once
		options := DefaultOptions()
		options.SetWebSocketDialer(func(ctx context.Context, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
			startedOnce.Do(func() { close(requestStarted) })
			<-ctx.Done()
			canceledOnce.Do(func() { close(dialCanceled) })
			return nil, nil, ctx.Err()
		})
		options.SetForceNew(true)
		options.SetReconnection(false)
		options.SetTimeout(100 * time.Millisecond)
		options.SetTransports(types.NewSet(WebSocket))
		socket, err := Connect("http://engine.invalid", options)
		if err != nil {
			t.Fatal(err)
		}
		managerError := make(chan struct{}, 1)
		connectError := make(chan struct{}, 1)
		if err := socket.Io().On("error", func(...any) { managerError <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		if err := socket.On("connect_error", func(...any) { connectError <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		select {
		case <-requestStarted:
		case <-time.After(time.Second):
			t.Fatal("WebSocket handshake did not enter the opening state")
		}
		socket.Disconnect()
		select {
		case <-dialCanceled:
		case <-time.After(time.Second):
			t.Fatal("force disconnect did not cancel the in-flight WebSocket dial")
		}
		select {
		case <-managerError:
			t.Fatal("Manager emitted an error after force disconnect while opening")
		case <-connectError:
			t.Fatal("Socket emitted connect_error after force disconnect while opening")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("unreachable address emits connect_error", func(t *testing.T) {
		options := DefaultOptions()
		options.SetAutoConnect(false)
		options.SetForceNew(true)
		options.SetReconnection(false)
		options.SetTimeout(100 * time.Millisecond)
		options.SetTransports(types.NewSet(WebSocket))
		socket, err := Connect(official483UnusedURL(t), options)
		if err != nil {
			t.Fatal(err)
		}
		connectError := make(chan error, 1)
		if err := socket.Once("connect_error", func(args ...any) {
			connectError <- args[0].(error)
		}); err != nil {
			t.Fatal(err)
		}
		socket.Connect()
		select {
		case err := <-connectError:
			if err == nil {
				t.Fatal("connect_error carried a nil error")
			}
		case <-time.After(time.Second):
			t.Fatal("unreachable address did not emit connect_error")
		}
		socket.Close()
	})

	t.Run("manager errors are not connect_error after the namespace connected", func(t *testing.T) {
		socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
		connectError := make(chan struct{}, 1)
		if err := socket.On("connect_error", func(...any) { connectError <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		socket.onconnect("socket-id", "")
		socket.onerror(errors.New("transport error after connect"))
		select {
		case <-connectError:
			t.Fatal("connected Socket forwarded a Manager error as connect_error")
		default:
		}
	})
}

func TestOfficialClient483OpenTimeoutByTransport(t *testing.T) {
	for _, transport := range []struct {
		name string
		ctor engineclient.TransportCtor
	}{
		{name: "polling", ctor: &engineclient.PollingBuilder{}},
		{name: "websocket", ctor: &engineclient.WebSocketBuilder{}},
	} {
		t.Run(transport.name, func(t *testing.T) {
			options := DefaultOptions()
			options.SetAutoConnect(false)
			options.SetForceNew(true)
			options.SetReconnection(false)
			options.SetTimeout(0)
			options.SetTransportList([]engineclient.TransportCtor{transport.ctor})
			socket, err := Connect("http://127.0.0.1:1", options)
			if err != nil {
				t.Fatal(err)
			}
			connectError := make(chan error, 1)
			if err := socket.Once("connect_error", func(args ...any) {
				connectError <- args[0].(error)
			}); err != nil {
				t.Fatal(err)
			}
			socket.Connect()
			select {
			case err := <-connectError:
				if err == nil || err.Error() != "timeout" {
					t.Fatalf("connect_error = %v, want timeout", err)
				}
			case <-time.After(time.Second):
				t.Fatal("open timeout did not emit connect_error")
			}
			socket.Disconnect()
		})
	}
}

func TestOfficialClient483ConnectErrorPayloadAllowsMissingData(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	got := make(chan error, 1)
	if err := socket.On("connect_error", func(args ...any) {
		got <- args[0].(error)
	}); err != nil {
		t.Fatal(err)
	}

	socket.onpacket(&parser.Packet{
		Type: parser.CONNECT_ERROR,
		Nsp:  "/",
		Data: map[string]any{"message": "not authorized"},
	})

	select {
	case err := <-got:
		if err.Error() != "not authorized" {
			t.Fatalf("connect_error = %q, want %q", err, "not authorized")
		}
	case <-time.After(time.Second):
		t.Fatal("connect_error was not emitted")
	}
}

func TestOfficialClient483RejectsLegacyConnectPacketWithoutSID(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	got := make(chan error, 1)
	if err := socket.On("connect_error", func(args ...any) { got <- args[0].(error) }); err != nil {
		t.Fatal(err)
	}
	socket.onpacket(&parser.Packet{Type: parser.CONNECT, Nsp: "/", Data: map[string]any{}})
	select {
	case err := <-got:
		if err == nil || err.Error() == "" {
			t.Fatalf("legacy protocol error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy CONNECT packet did not emit connect_error")
	}
}

func TestOfficialClient483ReservedEvents(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	for _, event := range []string{"connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener"} {
		if err := socket.Emit(event); err == nil {
			t.Errorf("Emit(%q) succeeded; want reserved-event error", event)
		}
	}
	if err := socket.Emit("application-event"); err != nil {
		t.Fatalf("application event failed: %v", err)
	}
}

func TestOfficialClient483AnyListeners(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	var incoming []string
	last := types.EventListener(func(args ...any) { incoming = append(incoming, "last") })
	first := types.EventListener(func(args ...any) {
		if !reflect.DeepEqual(args, []any{"event", 1.0}) {
			t.Errorf("OnAny args = %#v", args)
		}
		incoming = append(incoming, "first")
	})
	socket.OnAny(last).PrependAny(first)
	socket.emitEvent([]any{"event", 1.0})
	if !reflect.DeepEqual(incoming, []string{"first", "last"}) {
		t.Fatalf("incoming listener order = %#v", incoming)
	}
	socket.OffAny(first)
	if len(socket.ListenersAny()) != 1 {
		t.Fatalf("incoming listener count = %d, want 1", len(socket.ListenersAny()))
	}
	socket.OffAny(nil)
	if len(socket.ListenersAny()) != 0 {
		t.Fatalf("incoming listeners were not cleared: %d", len(socket.ListenersAny()))
	}

	var outgoing []string
	outLast := types.EventListener(func(args ...any) { outgoing = append(outgoing, "last") })
	outFirst := types.EventListener(func(args ...any) {
		if !reflect.DeepEqual(args, []any{"event", []byte{1, 2, 3}}) {
			t.Errorf("OnAnyOutgoing args = %#v", args)
		}
		outgoing = append(outgoing, "first")
	})
	socket.OnAnyOutgoing(outLast).PrependAnyOutgoing(outFirst)
	socket.notifyOutgoingListeners(&Packet{Packet: &parser.Packet{Type: parser.BINARY_EVENT, Data: []any{"event", []byte{1, 2, 3}}}})
	if !reflect.DeepEqual(outgoing, []string{"first", "last"}) {
		t.Fatalf("outgoing listener order = %#v", outgoing)
	}
	socket.OffAnyOutgoing(outFirst)
	if len(socket.ListenersAnyOutgoing()) != 1 {
		t.Fatalf("outgoing listener count = %d, want 1", len(socket.ListenersAnyOutgoing()))
	}
	socket.OffAnyOutgoing(nil)
	if len(socket.ListenersAnyOutgoing()) != 0 {
		t.Fatalf("outgoing listeners were not cleared: %d", len(socket.ListenersAnyOutgoing()))
	}
}

func TestOfficialComponentEmitterClientBoundary(t *testing.T) {
	t.Run("listeners keep registration order and arbitrary event names", func(t *testing.T) {
		socket := MakeSocket()
		var calls []any
		if err := socket.On("constructor", func(args ...any) { calls = append(calls, "one", args[0]) }); err != nil {
			t.Fatal(err)
		}
		if err := socket.On("constructor", func(args ...any) { calls = append(calls, "two", args[0]) }); err != nil {
			t.Fatal(err)
		}
		if err := socket.On("__proto__", func(args ...any) { calls = append(calls, "proto", args[0]) }); err != nil {
			t.Fatal(err)
		}
		socket.EventEmitter.Emit("constructor", float64(1))
		socket.EventEmitter.Emit("__proto__", float64(2))
		if !reflect.DeepEqual(calls, []any{"one", float64(1), "two", float64(1), "proto", float64(2)}) {
			t.Fatalf("emitter calls = %#v", calls)
		}
	})

	t.Run("once can be removed by its original listener", func(t *testing.T) {
		socket := MakeSocket()
		calls := 0
		listener := types.EventListener(func(...any) { calls++ })
		if err := socket.Once("once", listener); err != nil {
			t.Fatal(err)
		}
		if !socket.RemoveListener("once", listener) {
			t.Fatal("removing a Once listener by its original function returned false")
		}
		socket.EventEmitter.Emit("once")
		if calls != 0 {
			t.Fatalf("removed Once listener calls = %d", calls)
		}
		if err := socket.Once("once", listener); err != nil {
			t.Fatal(err)
		}
		socket.EventEmitter.Emit("once")
		socket.EventEmitter.Emit("once")
		if calls != 1 {
			t.Fatalf("Once listener calls = %d, want 1", calls)
		}
	})

	t.Run("removal during emit uses the current listener snapshot", func(t *testing.T) {
		socket := MakeSocket()
		var calls []string
		second := types.EventListener(func(...any) { calls = append(calls, "second") })
		first := types.EventListener(func(...any) {
			calls = append(calls, "first")
			socket.RemoveListener("event", second)
		})
		if err := socket.On("event", first, second); err != nil {
			t.Fatal(err)
		}
		socket.EventEmitter.Emit("event")
		socket.EventEmitter.Emit("event")
		if !reflect.DeepEqual(calls, []string{"first", "second", "first"}) {
			t.Fatalf("removal-during-emit calls = %#v", calls)
		}
	})

	t.Run("event and global listener removal", func(t *testing.T) {
		socket := MakeSocket()
		listener := types.EventListener(func(...any) {})
		if err := socket.On("first", listener); err != nil {
			t.Fatal(err)
		}
		if err := socket.On("second", listener); err != nil {
			t.Fatal(err)
		}
		if len(socket.Listeners("first")) != 1 || socket.ListenerCount("missing") != 0 {
			t.Fatalf("unexpected listener state: first=%d missing=%d", len(socket.Listeners("first")), socket.ListenerCount("missing"))
		}
		if !socket.RemoveAllListeners("first") || socket.ListenerCount("first") != 0 {
			t.Fatal("event-specific listener removal failed")
		}
		socket.Clear()
		if socket.Len() != 0 || socket.ListenerCount("second") != 0 {
			t.Fatal("global listener removal failed")
		}
	})
}

func TestOfficialClient483VolatilePacketIsDiscardedWhileDisconnected(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	if err := socket.Volatile().Emit("event", "value"); err != nil {
		t.Fatal(err)
	}
	if socket.SendBuffer().Len() != 0 {
		t.Fatalf("volatile packet was buffered while disconnected: %#v", socket.SendBuffer().All())
	}
}

func TestOfficialClient483VolatilePacketIsDiscardedWhileTransportIsNotWritable(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	state := installOfficial483EngineState(socket, false, false)
	socket.connected.Store(true)
	if err := socket.Volatile().Emit("event", "value"); err != nil {
		t.Fatal(err)
	}
	if state.writes.Load() != 0 || socket.SendBuffer().Len() != 0 {
		t.Fatalf("non-writable volatile packet was retained: writes=%d buffer=%d", state.writes.Load(), socket.SendBuffer().Len())
	}
}

func TestOfficialClient483ExpiredPingBuffersUntilReconnect(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	state := installOfficial483EngineState(socket, true, true)
	socket.connected.Store(true)
	var outgoing atomic.Int32
	socket.OnAnyOutgoing(func(...any) { outgoing.Add(1) })

	if err := socket.Emit("echo", "123"); err != nil {
		t.Fatal(err)
	}
	if state.writes.Load() != 0 || outgoing.Load() != 0 || socket.SendBuffer().Len() != 1 {
		t.Fatalf("expired-ping packet was not buffered: writes=%d outgoing=%d buffer=%d",
			state.writes.Load(), outgoing.Load(), socket.SendBuffer().Len())
	}

	state.pingExpired.Store(false)
	socket.onconnect("reconnected", "")
	if state.writes.Load() != 1 || outgoing.Load() != 1 || socket.SendBuffer().Len() != 0 {
		t.Fatalf("buffered packet was not flushed after reconnect: writes=%d outgoing=%d buffer=%d",
			state.writes.Load(), outgoing.Load(), socket.SendBuffer().Len())
	}
}

func TestOfficialClient483RetryQueueUsesOneEntryAndRetries(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetRetries(1)
		options.SetAckTimeout(10 * time.Millisecond)
	})
	socket.connected.Store(true)
	result := make(chan error, 1)
	if err := socket.Emit("event", "value", func(_ []any, err error) {
		result <- err
	}); err != nil {
		t.Fatal(err)
	}
	if socket._queue.Len() != 1 {
		t.Fatalf("retry queue length = %d immediately after Emit, want 1", socket._queue.Len())
	}
	queued, err := socket._queue.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if !queued.Flags.FromQueue {
		t.Fatal("retry packet did not retain the fromQueue flag")
	}

	select {
	case err := <-result:
		if err == nil || err.Error() != "operation has timed out" {
			t.Fatalf("retry result = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry callback did not complete")
	}
	if socket._queue.Len() != 0 {
		t.Fatalf("retry queue length = %d after exhausting retries, want 0", socket._queue.Len())
	}
	if socket.ids.Load() != 2 {
		t.Fatalf("ack attempts = %d, want 2", socket.ids.Load())
	}
}

func TestOfficialClient483RetryQueuePreservesOrder(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(options *SocketOptions) {
		options.SetRetries(2)
	})
	for _, value := range []float64{1, 2, 3} {
		if err := socket.Emit("echo", value); err != nil {
			t.Fatal(err)
		}
	}
	if socket._queue.Len() != 3 {
		t.Fatalf("retry queue length = %d, want 3", socket._queue.Len())
	}
	socket.connected.Store(true)
	socket._drainQueue(false)

	for index, want := range []float64{1, 2, 3} {
		queued, err := socket._queue.Get(0)
		if err != nil {
			t.Fatalf("queue item %d: %v", index, err)
		}
		if got := queued.Args[1]; got != want {
			t.Fatalf("queue item %d value = %#v, want %#v", index, got, want)
		}
		ack, ok := queued.Args[len(queued.Args)-1].(func([]any, error))
		if !ok {
			t.Fatalf("queue item %d ack type = %T", index, queued.Args[len(queued.Args)-1])
		}
		ack([]any{want}, nil)
	}
	if socket._queue.Len() != 0 {
		t.Fatalf("retry queue length = %d after acknowledgements, want 0", socket._queue.Len())
	}
}

func TestOfficialClient483AckTimeoutRemovesBufferedPacketAndAccounting(t *testing.T) {
	socket := newBackpressureTestSocket(t, func(*SocketOptions) {})
	result := make(chan error, 1)
	if err := socket.Timeout(10*time.Millisecond).Emit("event", func(_ []any, err error) {
		result <- err
	}); err != nil {
		t.Fatal(err)
	}
	if stats := socket.BufferStats(); stats.SendPackets != 1 || stats.SendBytes == 0 {
		t.Fatalf("unexpected buffered stats before timeout: %#v", stats)
	}
	select {
	case err := <-result:
		if err == nil || err.Error() != "operation has timed out" {
			t.Fatalf("ack timeout result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ack timeout callback did not run")
	}
	if stats := socket.BufferStats(); stats.SendPackets != 0 || stats.SendBytes != 0 {
		t.Fatalf("buffer accounting after timeout = %#v, want empty", stats)
	}
}
