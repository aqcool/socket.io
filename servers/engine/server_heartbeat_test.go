package engine

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/gorilla/websocket"
)

func TestServerPingTimeoutWithoutOutstandingPoll(t *testing.T) {
	for _, test := range []struct {
		name      string
		eio       string
		transport string
		allowEIO3 bool
	}{
		{name: "EIO4 polling", eio: "4", transport: "polling"},
		{name: "EIO4 WebSocket", eio: "4", transport: "websocket"},
		{name: "EIO3 polling", eio: "3", transport: "polling", allowEIO3: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowEIO3(test.allowEIO3)
			options.SetAllowUpgrades(false)
			options.SetPingInterval(20 * time.Millisecond)
			options.SetPingTimeout(20 * time.Millisecond)
			server := NewServer(options)
			closed := make(chan string, 1)
			transportClosed := make(chan struct{}, 1)
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(Socket)
				_ = socket.Transport().Once("close", func(...any) { transportClosed <- struct{}{} })
				_ = socket.Once("close", func(args ...any) {
					closed <- args[0].(string)
				})
			})
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			if test.transport == "websocket" {
				wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/engine.io/?EIO=" + test.eio + "&transport=websocket"
				connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatalf("WebSocket handshake: %v", err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				if _, _, err := connection.ReadMessage(); err != nil {
					t.Fatalf("reading WebSocket OPEN: %v", err)
				}
			} else {
				response, err := http.Get(httpServer.URL + "/engine.io/?EIO=" + test.eio + "&transport=polling")
				if err != nil {
					t.Fatalf("handshake: %v", err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatalf("handshake status = %d, want 200", response.StatusCode)
				}
			}

			select {
			case reason := <-closed:
				if reason != "ping timeout" {
					t.Fatalf("close reason = %q, want ping timeout", reason)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not close inactive client after ping timeout")
			}
			if test.transport == "websocket" {
				select {
				case <-transportClosed:
				case <-time.After(time.Second):
					t.Fatal("server did not close WebSocket transport after ping timeout")
				}
			}
			if got := server.ClientsCount(); got != 0 {
				t.Fatalf("clients count after ping timeout = %d, want 0", got)
			}
		})
	}
}

func TestOfficialServerPingTimeoutClosesActivePollingTransport(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetPingInterval(50 * time.Millisecond)
	options.SetPingTimeout(100 * time.Millisecond)
	server := NewServer(options)
	socketClosed := make(chan string, 1)
	transportClosed := make(chan struct{}, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		_ = socket.Once("close", func(args ...any) { socketClosed <- args[0].(string) })
		_ = socket.Transport().Once("close", func(...any) { transportClosed <- struct{}{} })
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	_, sid := engineHandshake(t, httpServer.URL)
	pollingURL := httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + sid
	pollingDone := make(chan error, 1)
	go func() {
		for range 3 {
			response, err := http.Get(pollingURL)
			if err != nil {
				pollingDone <- err
				return
			}
			payload, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				pollingDone <- readErr
				return
			}
			if response.StatusCode != http.StatusOK {
				pollingDone <- errors.New("polling request was rejected")
				return
			}
			if string(payload) == "1" {
				pollingDone <- nil
				return
			}
		}
		pollingDone <- errors.New("polling transport did not receive CLOSE")
	}()

	select {
	case reason := <-socketClosed:
		if reason != "ping timeout" {
			t.Fatalf("socket close reason = %q, want ping timeout", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("socket did not close after ping timeout")
	}
	select {
	case <-transportClosed:
	case <-time.After(time.Second):
		t.Fatal("active Polling transport did not close after ping timeout")
	}
	select {
	case err := <-pollingDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("active Polling request did not finish after ping timeout")
	}
}
