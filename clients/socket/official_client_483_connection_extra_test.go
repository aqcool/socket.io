package socket

import (
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	server "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

const officialClient483ConnectionExtraWait = 5 * time.Second

type officialClient483ConnectionExtraServer struct {
	io        *server.Server
	http      *httptest.Server
	closeOnce sync.Once
}

type officialClient483ConnectionExtraOnceEmitter interface {
	Once(types.EventName, ...types.EventListener) error
}

func newOfficialClient483ConnectionExtraServer(t *testing.T, namespaces ...string) *officialClient483ConnectionExtraServer {
	t.Helper()
	options := server.DefaultServerOptions()
	options.SetTransports(types.NewSet(server.WebSocket))
	io := server.NewServer(nil, options)
	for _, namespace := range namespaces {
		io.Of(namespace, nil)
	}

	fixture := &officialClient483ConnectionExtraServer{
		io:   io,
		http: httptest.NewServer(io.ServeHandler(nil)),
	}
	t.Cleanup(fixture.Close)
	return fixture
}

func (s *officialClient483ConnectionExtraServer) URL() string {
	return s.http.URL
}

func (s *officialClient483ConnectionExtraServer) Close() {
	s.closeOnce.Do(func() {
		s.io.Close(nil)
		s.http.Close()
	})
}

func officialClient483ConnectionExtraOptions() *Options {
	options := DefaultOptions()
	options.SetAutoConnect(false)
	options.SetTransports(types.NewSet(WebSocket))
	options.SetReconnection(true)
	options.SetReconnectionDelay(20)
	options.SetReconnectionDelayMax(20)
	options.SetRandomizationFactor(0)
	return options
}

func officialClient483ConnectionExtraSignal(t *testing.T, emitter officialClient483ConnectionExtraOnceEmitter, event types.EventName) <-chan struct{} {
	t.Helper()
	signal := make(chan struct{}, 1)
	if err := emitter.Once(event, func(...any) {
		select {
		case signal <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatalf("registering %q listener: %v", event, err)
	}
	return signal
}

func officialClient483ConnectionExtraAwait(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(officialClient483ConnectionExtraWait):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestOfficialClient483ConnectionLine334StopsOneNamespaceWhileAnotherReconnects(t *testing.T) {
	fixture := newOfficialClient483ConnectionExtraServer(t, "/asd")
	manager := NewManager(fixture.URL(), officialClient483ConnectionExtraOptions())
	t.Cleanup(manager._close)
	socket1 := manager.Socket("/", nil)
	socket2 := manager.Socket("/asd", nil)
	t.Cleanup(func() {
		socket1.Close()
		socket2.Close()
	})

	firstSocket1Connect := officialClient483ConnectionExtraSignal(t, socket1, "connect")
	firstSocket2Connect := officialClient483ConnectionExtraSignal(t, socket2, "connect")
	socket1.Connect()
	socket2.Connect()
	officialClient483ConnectionExtraAwait(t, firstSocket1Connect, "the first namespace to connect")
	officialClient483ConnectionExtraAwait(t, firstSocket2Connect, "the second namespace to connect")

	reconnectedSocket1 := officialClient483ConnectionExtraSignal(t, socket1, "connect")
	reconnectedSocket2 := officialClient483ConnectionExtraSignal(t, socket2, "connect")
	firstAttempt := make(chan struct{}, 1)
	var attempts atomic.Int32
	if err := manager.On("reconnect_attempt", func(...any) {
		attempts.Add(1)
		socket1.Disconnect()
		select {
		case firstAttempt <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}

	initialEngine := manager.Engine()
	if initialEngine == nil {
		t.Fatal("missing initial Engine.IO connection")
	}
	initialEngine.Close()
	officialClient483ConnectionExtraAwait(t, firstAttempt, "the Manager reconnect attempt")
	officialClient483ConnectionExtraAwait(t, reconnectedSocket2, "the still-active namespace to reconnect")

	select {
	case <-reconnectedSocket1:
		t.Fatal("the manually disconnected namespace reconnected")
	case <-time.After(100 * time.Millisecond):
	}
	if attempts.Load() != 1 {
		t.Fatalf("reconnect attempts = %d, want 1", attempts.Load())
	}
	if socket1.Active() || socket1.Connected() || !socket2.Active() || !socket2.Connected() {
		t.Fatalf("namespace states after reconnect: first active=%v connected=%v; second active=%v connected=%v",
			socket1.Active(), socket1.Connected(), socket2.Active(), socket2.Connected())
	}
	if manager.Engine() == nil || manager.Engine() == initialEngine {
		t.Fatal("the active namespace did not reconnect over a new Engine.IO connection")
	}
}

func TestOfficialClient483ConnectionLine441DefaultTimeoutConnectsWithoutReconnect(t *testing.T) {
	fixture := newOfficialClient483ConnectionExtraServer(t, "/valid")
	options := officialClient483ConnectionExtraOptions()
	manager := NewManager(fixture.URL(), options)
	t.Cleanup(manager._close)
	socket := manager.Socket("/valid", nil)
	t.Cleanup(func() { socket.Close() })

	if timeout := manager.Timeout(); timeout == nil || *timeout != DefaultTimeout {
		t.Fatalf("Manager timeout = %v, want the official default %v", timeout, DefaultTimeout)
	}
	var reconnectAttempts atomic.Int32
	if err := manager.On("reconnect_attempt", func(...any) { reconnectAttempts.Add(1) }); err != nil {
		t.Fatal(err)
	}
	connected := officialClient483ConnectionExtraSignal(t, socket, "connect")
	socket.Connect()
	officialClient483ConnectionExtraAwait(t, connected, "the valid namespace connection")

	time.Sleep(100 * time.Millisecond)
	if reconnectAttempts.Load() != 0 {
		t.Fatalf("reconnect attempts after a successful connection = %d, want 0", reconnectAttempts.Load())
	}
	if !socket.Connected() || manager.Engine() == nil {
		t.Fatalf("successful connection state: connected=%v engine=%v", socket.Connected(), manager.Engine())
	}
}

func TestOfficialClient483ConnectionLine464ConnectsWhileAnotherNamespaceDisconnects(t *testing.T) {
	fixture := newOfficialClient483ConnectionExtraServer(t, "/foo", "/asd")
	manager := NewManager(fixture.URL(), officialClient483ConnectionExtraOptions())
	t.Cleanup(manager._close)
	socket1 := manager.Socket("/foo", nil)
	t.Cleanup(func() { socket1.Close() })

	firstConnected := officialClient483ConnectionExtraSignal(t, socket1, "connect")
	socket1.Connect()
	officialClient483ConnectionExtraAwait(t, firstConnected, "the first namespace to connect")
	sharedEngine := manager.Engine()

	socket2 := manager.Socket("/asd", nil)
	t.Cleanup(func() { socket2.Close() })
	secondConnected := officialClient483ConnectionExtraSignal(t, socket2, "connect")
	managerClosed := officialClient483ConnectionExtraSignal(t, manager, "close")
	socket2.Connect()
	socket1.Disconnect()
	officialClient483ConnectionExtraAwait(t, secondConnected, "the second namespace to connect while the first disconnects")

	select {
	case <-managerClosed:
		t.Fatal("disconnecting the first namespace closed the shared Manager")
	default:
	}
	if socket1.Active() || socket1.Connected() || !socket2.Connected() {
		t.Fatalf("namespace states: first active=%v connected=%v; second connected=%v",
			socket1.Active(), socket1.Connected(), socket2.Connected())
	}
	if manager.Engine() == nil || manager.Engine() != sharedEngine {
		t.Fatal("the second namespace did not connect on the existing Engine.IO connection")
	}
}

func TestOfficialClient483ConnectionLine494SingleNamespaceDisconnectKeepsSharedEngine(t *testing.T) {
	fixture := newOfficialClient483ConnectionExtraServer(t, "/foo", "/asd")
	manager := NewManager(fixture.URL(), officialClient483ConnectionExtraOptions())
	t.Cleanup(manager._close)
	socket1 := manager.Socket("/foo", nil)
	socket2 := manager.Socket("/asd", nil)
	t.Cleanup(func() {
		socket1.Close()
		socket2.Close()
	})

	firstConnected := officialClient483ConnectionExtraSignal(t, socket1, "connect")
	socket1.Connect()
	officialClient483ConnectionExtraAwait(t, firstConnected, "the first namespace to connect")
	secondConnected := officialClient483ConnectionExtraSignal(t, socket2, "connect")
	socket2.Connect()
	officialClient483ConnectionExtraAwait(t, secondConnected, "the second namespace to connect")
	sharedEngine := manager.Engine()

	managerClosed := officialClient483ConnectionExtraSignal(t, manager, "close")
	secondDisconnected := officialClient483ConnectionExtraSignal(t, socket2, "disconnect")
	socket1.Disconnect()

	select {
	case <-secondDisconnected:
		t.Fatal("disconnecting one namespace disconnected the other namespace")
	case <-managerClosed:
		t.Fatal("disconnecting one namespace closed the shared Manager")
	case <-time.After(200 * time.Millisecond):
	}
	if !socket2.Connected() || manager.Engine() == nil || manager.Engine() != sharedEngine {
		t.Fatalf("shared connection after first disconnect: second connected=%v engine retained=%v",
			socket2.Connected(), manager.Engine() == sharedEngine)
	}

	socket2.RemoveAllListeners("disconnect")
	socket2.Disconnect()
	officialClient483ConnectionExtraAwait(t, managerClosed, "the Manager to close after its last namespace disconnects")
}

func TestOfficialClient483ConnectionLine596AsyncSecondSocketKeepsTwoReconnectAttempts(t *testing.T) {
	fixture := newOfficialClient483ConnectionExtraServer(t, "/room1", "/room2")
	endpoint := fixture.URL()
	fixture.Close()

	options := officialClient483ConnectionExtraOptions()
	options.SetReconnectionAttempts(2)
	options.SetReconnectionDelay(40)
	options.SetReconnectionDelayMax(40)
	manager := NewManager(endpoint, options)
	t.Cleanup(manager._close)
	socket1 := manager.Socket("/room1", nil)
	t.Cleanup(func() { socket1.Close() })

	var reconnectAttempts atomic.Int32
	if err := manager.On("reconnect_attempt", func(...any) { reconnectAttempts.Add(1) }); err != nil {
		t.Fatal(err)
	}
	reconnectFailed := officialClient483ConnectionExtraSignal(t, manager, "reconnect_failed")
	secondCreated := make(chan *Socket, 1)
	timer := time.AfterFunc(10*time.Millisecond, func() {
		socket2 := manager.Socket("/room2", nil)
		socket2.Connect()
		secondCreated <- socket2
	})
	t.Cleanup(func() { timer.Stop() })

	socket1.Connect()
	officialClient483ConnectionExtraAwait(t, reconnectFailed, "the two-attempt reconnect loop to fail")
	if reconnectAttempts.Load() != 2 {
		t.Fatalf("reconnect attempts after asynchronously opening another socket = %d, want 2", reconnectAttempts.Load())
	}
	select {
	case socket2 := <-secondCreated:
		if !socket2.Active() {
			t.Fatal("the asynchronously opened second namespace was not active during reconnection")
		}
		socket2.Close()
	case <-time.After(time.Second):
		t.Fatal("the second namespace was not opened asynchronously")
	}
}
