package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
)

func TestOfficialClientWriteBufferVisibleDuringClose(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	client.opts = DefaultSocketOptions()
	client.readyState.Store(SocketStateOpen)
	client.writeBuffer.Push(&packet.Packet{Type: packet.MESSAGE, Data: strings.NewReader("foo")})

	bufferAtClose := make(chan int, 1)
	reasonAtClose := make(chan string, 1)
	_ = client.Once("close", func(args ...any) {
		bufferAtClose <- client.writeBuffer.Len()
		reasonAtClose <- args[0].(string)
	})
	client._onError(errors.New("test failure"))

	if got := <-bufferAtClose; got != 1 {
		t.Fatalf("write buffer length during close = %d, want 1", got)
	}
	if reason := <-reasonAtClose; reason != "transport error" {
		t.Fatalf("close reason = %q, want transport error", reason)
	}
	if got := client.writeBuffer.Len(); got != 0 {
		t.Fatalf("write buffer length after close = %d, want 0", got)
	}
}

func TestOfficialClientPingTimeoutUsesIntervalPlusTimeout(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	client.opts = DefaultSocketOptions()
	client.readyState.Store(SocketStateOpen)
	client._pingInterval.Store(50)
	client._pingTimeout.Store(30)
	closed := make(chan string, 1)
	_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })

	started := time.Now()
	client._resetPingTimeout()
	select {
	case reason := <-closed:
		t.Fatalf("client closed before ping interval elapsed: %q", reason)
	case <-time.After(45 * time.Millisecond):
	}
	select {
	case reason := <-closed:
		if reason != "ping timeout" {
			t.Fatalf("close reason = %q, want ping timeout", reason)
		}
		if elapsed := time.Since(started); elapsed < 70*time.Millisecond {
			t.Fatalf("ping timeout fired after %v, want at least 70ms", elapsed)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("client did not close after ping interval plus timeout")
	}
}

func TestOfficialClientTransportCloseWinsBeforePingTimeout(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	client.opts = DefaultSocketOptions()
	client.readyState.Store(SocketStateOpen)
	client._pingInterval.Store(80)
	client._pingTimeout.Store(50)
	closed := make(chan string, 2)
	_ = client.On("close", func(args ...any) { closed <- args[0].(string) })

	client._resetPingTimeout()
	time.AfterFunc(20*time.Millisecond, func() { client._onClose("transport close", nil) })
	select {
	case reason := <-closed:
		if reason != "transport close" {
			t.Fatalf("close reason = %q, want transport close", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("transport close did not reach client")
	}
	time.Sleep(140 * time.Millisecond)
	select {
	case reason := <-closed:
		t.Fatalf("client emitted a second close after timer expiry: %q", reason)
	default:
	}
}

func newOfficialOpeningClient(t *testing.T, transportName string) (*socketWithoutUpgrade, Transport) {
	t.Helper()
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	options := DefaultSocketOptions()
	client.opts = options
	client.readyState.Store(SocketStateOpening)
	var transport Transport
	switch transportName {
	case "polling":
		transport = NewPolling(client, options)
	case "websocket":
		transport = NewWebSocket(client, options)
	default:
		t.Fatalf("unknown transport %q", transportName)
	}
	transport.SetReadyState(TransportStateOpening)
	client.transport.Store(&transport)
	return client, transport
}

func TestOfficialClientTransportErrorBeforeOpen(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			client, transport := newOfficialOpeningClient(t, transportName)
			closed := make(chan string, 1)
			_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })

			client._onError(errors.New("connection failed"))
			if reason := <-closed; reason != "transport error" {
				t.Fatalf("close reason = %q, want transport error", reason)
			}
			if client.ReadyState() != SocketStateClosed || transport.ReadyState() != TransportStateClosed {
				t.Fatalf("states after error = %q/%q", client.ReadyState(), transport.ReadyState())
			}
		})
	}
}

func TestOfficialClientForcedCloseBeforeOpen(t *testing.T) {
	for _, transportName := range []string{"polling", "websocket"} {
		t.Run(transportName, func(t *testing.T) {
			client, transport := newOfficialOpeningClient(t, transportName)
			closed := make(chan string, 1)
			_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })

			client.Close()
			if reason := <-closed; reason != "forced close" {
				t.Fatalf("close reason = %q, want forced close", reason)
			}
			if client.ReadyState() != SocketStateClosed || transport.ReadyState() != TransportStateClosed {
				t.Fatalf("states after forced close = %q/%q", client.ReadyState(), transport.ReadyState())
			}
		})
	}
}
