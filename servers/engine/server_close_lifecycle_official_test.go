package engine

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/gorilla/websocket"
)

func TestOfficialServerCloseDiscardsPendingPackets(t *testing.T) {
	for _, alreadyClosing := range []bool{false, true} {
		name := "open socket"
		if alreadyClosing {
			name = "already closing socket"
		}
		t.Run(name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			options.SetPingInterval(20 * time.Millisecond)
			options.SetPingTimeout(100 * time.Millisecond)
			server := NewServer(options)
			sockets := make(chan *socket, 1)
			_ = server.On("connection", func(args ...any) { sockets <- args[0].(*socket) })
			httpServer := newEngineHTTPTestServer(t, server)

			_, _ = engineHandshake(t, httpServer.URL)
			engineSocket := <-sockets
			callbackCalled := make(chan struct{}, 1)
			bufferAtClose := make(chan int, 1)
			pingsAfterClose := make(chan struct{}, 1)
			_ = engineSocket.On("packetCreate", func(args ...any) {
				if args[0].(*packet.Packet).Type == packet.PING {
					pingsAfterClose <- struct{}{}
				}
			})
			_ = engineSocket.Once("close", func(...any) {
				bufferAtClose <- engineSocket.writeBuffer.Len()
			})
			engineSocket.Send(strings.NewReader("hello"), nil, func(transports.Transport) {
				callbackCalled <- struct{}{}
			})
			if alreadyClosing {
				engineSocket.Close(false)
				if engineSocket.ReadyState() != "closing" {
					t.Fatalf("socket state before Server.Close = %q", engineSocket.ReadyState())
				}
			}

			server.Close()
			select {
			case buffered := <-bufferAtClose:
				if buffered == 0 {
					t.Fatal("Server.Close cleared writeBuffer before close listeners")
				}
			case <-time.After(time.Second):
				t.Fatal("Server.Close did not close pending socket")
			}
			if engineSocket.ReadyState() != "closed" || engineSocket.Transport().ReadyState() != "closed" {
				t.Fatalf("states after Server.Close = %q/%q", engineSocket.ReadyState(), engineSocket.Transport().ReadyState())
			}
			if engineSocket.writeBuffer.Len() != 0 || server.ClientsCount() != 0 {
				t.Fatalf("pending state after Server.Close: buffer=%d clients=%d", engineSocket.writeBuffer.Len(), server.ClientsCount())
			}
			select {
			case <-callbackCalled:
				t.Fatal("discarded packet callback was executed")
			case <-time.After(30 * time.Millisecond):
			}
			select {
			case <-pingsAfterClose:
				t.Fatal("heartbeat timer emitted PING after Server.Close")
			case <-time.After(40 * time.Millisecond):
			}
		})
	}
}

func TestOfficialServerCloseStopsUpgradedSocketAndTimers(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetPingInterval(20 * time.Millisecond)
	options.SetPingTimeout(100 * time.Millisecond)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	_ = server.On("connection", func(args ...any) { sockets <- args[0].(Socket) })
	httpServer := newEngineHTTPTestServer(t, server)

	_, sid := engineHandshake(t, httpServer.URL)
	engineSocket := <-sockets
	connection := beginWebSocketProbe(t, httpServer.URL, sid)
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.WriteMessage(websocket.TextMessage, []byte("5")); err != nil {
		t.Fatalf("completing upgrade: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !engineSocket.Upgraded() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !engineSocket.Upgraded() || engineSocket.Transport().Name() != "websocket" {
		t.Fatalf("socket did not upgrade: upgraded=%t transport=%s", engineSocket.Upgraded(), engineSocket.Transport().Name())
	}

	closed := make(chan string, 1)
	transportClosed := make(chan struct{}, 1)
	pingsAfterClose := make(chan struct{}, 1)
	_ = engineSocket.Once("close", func(args ...any) { closed <- args[0].(string) })
	_ = engineSocket.Transport().Once("close", func(...any) { transportClosed <- struct{}{} })
	_ = engineSocket.On("packetCreate", func(args ...any) {
		if args[0].(*packet.Packet).Type == packet.PING {
			pingsAfterClose <- struct{}{}
		}
	})
	server.Close()
	select {
	case reason := <-closed:
		if reason != "forced close" {
			t.Fatalf("upgraded socket close reason = %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not close upgraded socket")
	}
	select {
	case <-transportClosed:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not close upgraded transport")
	}
	if server.ClientsCount() != 0 {
		t.Fatalf("clients after upgraded Server.Close = %d", server.ClientsCount())
	}
	select {
	case <-pingsAfterClose:
		t.Fatal("upgraded socket emitted PING after Server.Close")
	case <-time.After(40 * time.Millisecond):
	}
}

func TestOfficialServerCloseStopsUpgradingSocketAndTimers(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetUpgradeTimeout(time.Second)
	options.SetPingInterval(20 * time.Millisecond)
	options.SetPingTimeout(100 * time.Millisecond)
	server := NewServer(options)
	sockets := make(chan Socket, 1)
	candidates := make(chan transports.Transport, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.Once("upgrading", func(upgradeArgs ...any) {
			candidates <- upgradeArgs[0].(transports.Transport)
		})
		sockets <- socket
	})
	httpServer := newEngineHTTPTestServer(t, server)

	_, sid := engineHandshake(t, httpServer.URL)
	engineSocket := <-sockets
	connection := beginWebSocketProbe(t, httpServer.URL, sid)
	t.Cleanup(func() { _ = connection.Close() })
	var candidate transports.Transport
	select {
	case candidate = <-candidates:
	case <-time.After(time.Second):
		t.Fatal("socket did not enter upgrading state")
	}
	candidateClosed := make(chan struct{}, 1)
	socketClosed := make(chan string, 1)
	_ = candidate.Once("close", func(...any) { candidateClosed <- struct{}{} })
	_ = engineSocket.Once("close", func(args ...any) { socketClosed <- args[0].(string) })

	server.Close()
	select {
	case reason := <-socketClosed:
		if reason != "forced close" {
			t.Fatalf("upgrading socket close reason = %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not close upgrading socket")
	}
	select {
	case <-candidateClosed:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not close candidate transport")
	}
	if engineSocket.Upgrading() || server.ClientsCount() != 0 {
		t.Fatalf("state after upgrading Server.Close: upgrading=%t clients=%d", engineSocket.Upgrading(), server.ClientsCount())
	}
}

func newEngineHTTPTestServer(t *testing.T, server Server) *httptest.Server {
	t.Helper()
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	return httpServer
}
