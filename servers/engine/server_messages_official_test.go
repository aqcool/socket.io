package engine

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestOfficialServerPollingReceivesSplitAndChunkedPayloads(t *testing.T) {
	for _, test := range []struct {
		name    string
		chunks  []string
		want    string
		maximum int64
	}{
		{name: "large split body", chunks: []string{"4" + strings.Repeat("a", 300_000), strings.Repeat("a", 300_000), strings.Repeat("a", 400_000)}, want: strings.Repeat("a", 1_000_000), maximum: 2_000_000},
		{name: "chunked transfer encoding", chunks: []string{"41", "2", "3"}, want: "123", maximum: 1_000_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowUpgrades(false)
			options.SetMaxHttpBufferSize(test.maximum)
			server := NewServer(options)
			messages := make(chan string, 1)
			_ = server.On("connection", func(args ...any) {
				_ = args[0].(Socket).Once("message", func(messageArgs ...any) {
					messages <- messageArgs[0].(types.BufferInterface).String()
				})
			})
			httpServer := newEngineHTTPTestServer(t, server)
			_, sid := engineHandshake(t, httpServer.URL)

			reader, writer := io.Pipe()
			go func() {
				for _, chunk := range test.chunks {
					if _, err := io.WriteString(writer, chunk); err != nil {
						_ = writer.CloseWithError(err)
						return
					}
				}
				_ = writer.Close()
			}()
			request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), reader)
			if err != nil {
				t.Fatalf("creating split Polling POST: %v", err)
			}
			request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
			if test.name == "large split body" {
				request.ContentLength = int64(len(test.want) + 1)
			} else {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("split Polling POST: %v", err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK || string(body) != "ok" {
				t.Fatalf("split Polling response = %d/%q, error=%v", response.StatusCode, body, readErr)
			}
			select {
			case message := <-messages:
				if message != test.want {
					t.Fatalf("message length/value = %d/%q, want %d/%q", len(message), message[:min(len(message), 32)], len(test.want), test.want[:min(len(test.want), 32)])
				}
			case <-time.After(2 * time.Second):
				t.Fatal("split Polling payload was not delivered")
			}
		})
	}
}

func TestOfficialEngineIO669RejectsBinaryEIO4PollingData(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	httpServer := newEngineHTTPTestServer(t, server)
	_, sid := engineHandshake(t, httpServer.URL)

	request, err := http.NewRequest(
		http.MethodPost,
		httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid),
		bytes.NewReader([]byte{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("creating binary Polling POST: %v", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("binary Polling POST: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading binary Polling response: %v", readErr)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("binary EIO4 Polling status/body = %d/%q, want 400", response.StatusCode, body)
	}
}

func TestOfficialServerEmitsFlushAndDrainEvents(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	server := NewServer(options)
	events := make(chan string, 4)
	var connected Socket
	_ = server.On("connection", func(args ...any) {
		connected = args[0].(Socket)
		_ = connected.On("flush", func(flushArgs ...any) {
			packets := flushArgs[0].([]*packet.Packet)
			if len(packets) == 1 && packets[0].Type == packet.MESSAGE {
				events <- "socket flush"
			}
		})
		_ = connected.On("drain", func(...any) { events <- "socket drain" })
		connected.Send(strings.NewReader("aaaa"), nil, nil)
	})
	_ = server.On("flush", func(args ...any) {
		if args[0].(Socket) == connected && len(args[1].([]*packet.Packet)) == 1 {
			events <- "server flush"
		}
	})
	_ = server.On("drain", func(args ...any) {
		if args[0].(Socket) == connected {
			events <- "server drain"
		}
	})
	httpServer := newEngineHTTPTestServer(t, server)
	_, sid := engineHandshake(t, httpServer.URL)
	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling&sid=" + url.QueryEscape(sid))
	if err != nil {
		t.Fatalf("Polling receive: %v", err)
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || !bytes.Contains(payload, []byte("4aaaa")) {
		t.Fatalf("Polling payload = %q, error=%v", payload, readErr)
	}

	seen := map[string]bool{}
	for range 4 {
		select {
		case event := <-events:
			seen[event] = true
		case <-time.After(time.Second):
			t.Fatalf("flush/drain events = %#v", seen)
		}
	}
	for _, event := range []string{"socket flush", "socket drain", "server flush", "server drain"} {
		if !seen[event] {
			t.Fatalf("missing %s: %#v", event, seen)
		}
	}
}
