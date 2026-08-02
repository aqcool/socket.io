package socket

import (
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	server "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

const official483ReconnectWait = 5 * time.Second

func official483WebSocketOptions() *Options {
	options := DefaultOptions()
	options.SetAutoConnect(false)
	options.SetTransports(types.NewSet(WebSocket))
	options.SetRandomizationFactor(0)
	return options
}

func official483UnusedURL(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return "http://" + address
}

func official483SocketServer(t *testing.T, namespaces ...string) string {
	t.Helper()
	options := server.DefaultServerOptions()
	options.SetTransports(types.NewSet(server.WebSocket))
	io := server.NewServer(nil, options)
	for _, namespace := range namespaces {
		io.Of(namespace, nil)
	}
	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		io.Close(nil)
		httpServer.Close()
	})
	return httpServer.URL
}

func official483Wait(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(official483ReconnectWait):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestOfficialClient483OpensNamespaceAfterPreviousSocketClosed(t *testing.T) {
	manager := NewManager(official483SocketServer(t, "/foo"), official483WebSocketOptions())
	t.Cleanup(manager._close)
	root := manager.Socket("/", nil)
	done := make(chan struct{})
	var stage atomic.Int32

	if err := root.On("connect", func(...any) {
		if stage.CompareAndSwap(0, 1) {
			root.Disconnect()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := root.On("disconnect", func(...any) {
		if !stage.CompareAndSwap(1, 2) {
			return
		}
		foo := manager.Socket("/foo", nil)
		if err := foo.On("connect", func(...any) {
			if stage.CompareAndSwap(2, 3) {
				foo.Disconnect()
				close(done)
			}
		}); err != nil {
			t.Errorf("register /foo connect listener: %v", err)
			close(done)
			return
		}
		foo.Connect()
	}); err != nil {
		t.Fatal(err)
	}

	root.Connect()
	official483Wait(t, done, "the new namespace to connect after the first socket closed")
	if stage.Load() != 3 {
		t.Fatalf("namespace lifecycle stage = %d, want 3", stage.Load())
	}
}

func TestOfficialClient483AutomaticReconnectStillWorksAfterManualReconnect(t *testing.T) {
	options := official483WebSocketOptions()
	options.SetReconnectionDelay(10)
	options.SetReconnectionDelayMax(10)
	manager := NewManager(official483SocketServer(t), options)
	t.Cleanup(manager._close)
	socket := manager.Socket("/", nil)
	done := make(chan struct{})
	var stage atomic.Int32
	var reconnects atomic.Int32

	if err := manager.On("reconnect", func(...any) {
		reconnects.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := socket.On("connect", func(...any) {
		switch {
		case stage.CompareAndSwap(0, 1):
			socket.Disconnect()
		case stage.CompareAndSwap(2, 3):
			time.AfterFunc(20*time.Millisecond, func() {
				if engine := manager.Engine(); engine != nil {
					engine.Close()
				}
			})
		case stage.CompareAndSwap(3, 4):
			socket.Disconnect()
			close(done)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := socket.On("disconnect", func(...any) {
		if stage.CompareAndSwap(1, 2) {
			socket.Connect()
		}
	}); err != nil {
		t.Fatal(err)
	}

	socket.Connect()
	official483Wait(t, done, "automatic reconnect after a manual reconnect")
	if stage.Load() != 4 || reconnects.Load() != 1 {
		t.Fatalf("manual/automatic reconnect state = stage %d, manager reconnects %d", stage.Load(), reconnects.Load())
	}
}

func TestOfficialClient483ReconnectFailureEventsAndSecondLoop(t *testing.T) {
	options := official483WebSocketOptions()
	options.SetTimeout(0)
	options.SetReconnection(true)
	options.SetReconnectionAttempts(2)
	options.SetReconnectionDelay(10)
	options.SetReconnectionDelayMax(10)
	manager := NewManager(official483UnusedURL(t), options)
	t.Cleanup(manager._close)
	socket := manager.Socket("/timeout", nil)
	failed := make(chan struct{}, 2)
	var mu sync.Mutex
	attempts := make([]uint64, 0, 4)
	var managerErrors, reconnectErrors, failedCount atomic.Int32

	if err := manager.On("error", func(...any) {
		managerErrors.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.On("reconnect_attempt", func(args ...any) {
		attempt, ok := args[0].(uint64)
		if !ok {
			t.Errorf("reconnect attempt type = %T, want uint64", args[0])
			return
		}
		mu.Lock()
		attempts = append(attempts, attempt)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.On("reconnect_error", func(...any) {
		reconnectErrors.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.On("reconnect_failed", func(...any) {
		failedCount.Add(1)
		failed <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}

	socket.Connect()
	official483Wait(t, failed, "the first exhausted reconnect loop")
	socket.Connect()
	official483Wait(t, failed, "a second reconnect loop after the first failed")
	socket.Disconnect()

	mu.Lock()
	gotAttempts := append([]uint64(nil), attempts...)
	mu.Unlock()
	wantAttempts := []uint64{1, 2, 1, 2}
	if len(gotAttempts) != len(wantAttempts) {
		t.Fatalf("reconnect attempts = %v, want %v", gotAttempts, wantAttempts)
	}
	for index := range wantAttempts {
		if gotAttempts[index] != wantAttempts[index] {
			t.Fatalf("reconnect attempts = %v, want %v", gotAttempts, wantAttempts)
		}
	}
	if failedCount.Load() != 2 || reconnectErrors.Load() != 4 || managerErrors.Load() < 6 {
		t.Fatalf("manager events = error %d, reconnect_error %d, reconnect_failed %d", managerErrors.Load(), reconnectErrors.Load(), failedCount.Load())
	}
}

func TestOfficialClient483ReconnectDelayIncreasesEveryAttempt(t *testing.T) {
	options := official483WebSocketOptions()
	options.SetTimeout(20 * time.Millisecond)
	options.SetReconnection(true)
	options.SetReconnectionAttempts(3)
	options.SetReconnectionDelay(100)
	options.SetReconnectionDelayMax(500)
	manager := NewManager(official483UnusedURL(t), options)
	t.Cleanup(manager._close)
	socket := manager.Socket("/timeout", nil)
	type delayRecord struct {
		attempt uint64
		delay   time.Duration
	}
	records := make(chan delayRecord, 3)
	failed := make(chan struct{}, 1)
	var lastError atomic.Int64

	if err := manager.On("error", func(...any) {
		lastError.Store(time.Now().UnixNano())
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.On("reconnect_attempt", func(args ...any) {
		started := lastError.Load()
		records <- delayRecord{
			attempt: args[0].(uint64),
			delay:   time.Since(time.Unix(0, started)),
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.On("reconnect_failed", func(...any) {
		failed <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}

	socket.Connect()
	official483Wait(t, failed, "three exponentially delayed reconnect attempts")
	socket.Disconnect()
	minimums := []time.Duration{80 * time.Millisecond, 180 * time.Millisecond, 380 * time.Millisecond}
	for index, minimum := range minimums {
		select {
		case record := <-records:
			if record.attempt != uint64(index+1) || record.delay < minimum {
				t.Fatalf("attempt %d delay = %v, want attempt %d and at least %v", record.attempt, record.delay, index+1, minimum)
			}
		default:
			t.Fatalf("missing reconnect delay record %d", index+1)
		}
	}
}

func TestOfficialClient483ForceCloseControlsReconnectLoop(t *testing.T) {
	t.Run("force close before the first attempt", func(t *testing.T) {
		options := official483WebSocketOptions()
		options.SetReconnection(true)
		options.SetReconnectionDelay(100)
		options.SetReconnectionDelayMax(100)
		manager := NewManager(official483UnusedURL(t), options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/invalid", nil)
		closed := make(chan struct{})
		var closeOnce sync.Once
		var attempts atomic.Int32
		_ = manager.On("reconnect_attempt", func(...any) { attempts.Add(1) })
		_ = manager.On("error", func(...any) {
			closeOnce.Do(func() {
				socket.Disconnect()
				close(closed)
			})
		})

		socket.Connect()
		official483Wait(t, closed, "initial connection error and force close")
		time.Sleep(200 * time.Millisecond)
		if attempts.Load() != 0 {
			t.Fatalf("reconnect attempts after force close = %d, want 0", attempts.Load())
		}
	})

	t.Run("force close stops an active loop", func(t *testing.T) {
		options := official483WebSocketOptions()
		options.SetReconnection(true)
		options.SetReconnectionDelay(20)
		options.SetReconnectionDelayMax(20)
		manager := NewManager(official483UnusedURL(t), options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/invalid", nil)
		first := make(chan struct{})
		var once sync.Once
		var attempts atomic.Int32
		_ = manager.On("reconnect_attempt", func(...any) {
			attempts.Add(1)
			once.Do(func() {
				socket.Disconnect()
				close(first)
			})
		})

		socket.Connect()
		official483Wait(t, first, "the first reconnect attempt")
		time.Sleep(100 * time.Millisecond)
		if attempts.Load() != 1 {
			t.Fatalf("reconnect attempts after force close = %d, want 1", attempts.Load())
		}
	})

	t.Run("manual connect restarts a stopped loop", func(t *testing.T) {
		options := official483WebSocketOptions()
		options.SetReconnection(true)
		options.SetReconnectionDelay(20)
		options.SetReconnectionDelayMax(20)
		manager := NewManager(official483UnusedURL(t), options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/invalid", nil)
		done := make(chan struct{})
		var attempts atomic.Int32
		_ = manager.On("reconnect_attempt", func(...any) {
			switch attempts.Add(1) {
			case 1:
				socket.Disconnect()
				socket.Connect()
			case 2:
				socket.Disconnect()
				close(done)
			}
		})

		socket.Connect()
		official483Wait(t, done, "a reconnect attempt after restarting the stopped loop")
		if attempts.Load() != 2 {
			t.Fatalf("reconnect attempts after restart = %d, want 2", attempts.Load())
		}
	})
}

func TestOfficialClient483CanDisableReconnectLoop(t *testing.T) {
	t.Run("disable the current loop after reconnect_error", func(t *testing.T) {
		options := official483WebSocketOptions()
		options.SetReconnection(true)
		options.SetReconnectionAttempts(5)
		options.SetReconnectionDelay(20)
		options.SetReconnectionDelayMax(20)
		manager := NewManager(official483UnusedURL(t), options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/invalid", nil)
		disabled := make(chan struct{})
		var once sync.Once
		var attempts atomic.Int32
		_ = manager.On("reconnect_attempt", func(...any) { attempts.Add(1) })
		_ = manager.On("reconnect_error", func(...any) {
			once.Do(func() {
				manager.SetReconnection(false)
				close(disabled)
			})
		})

		socket.Connect()
		official483Wait(t, disabled, "reconnection to be disabled after its first error")
		time.Sleep(120 * time.Millisecond)
		socket.Disconnect()
		if attempts.Load() != 1 {
			t.Fatalf("attempts after disabling current loop = %d, want 1", attempts.Load())
		}
	})

	t.Run("disabled from construction", func(t *testing.T) {
		options := official483WebSocketOptions()
		options.SetReconnection(false)
		options.SetReconnectionDelay(10)
		manager := NewManager(official483UnusedURL(t), options)
		t.Cleanup(manager._close)
		socket := manager.Socket("/invalid", nil)
		failed := make(chan struct{})
		var once sync.Once
		var attempts atomic.Int32
		_ = manager.On("reconnect_attempt", func(...any) { attempts.Add(1) })
		_ = manager.On("error", func(...any) {
			once.Do(func() { close(failed) })
		})

		socket.Connect()
		official483Wait(t, failed, "the non-reconnecting initial connection error")
		time.Sleep(100 * time.Millisecond)
		socket.Disconnect()
		if attempts.Load() != 0 {
			t.Fatalf("attempts with reconnection disabled = %d, want 0", attempts.Load())
		}
	})
}
