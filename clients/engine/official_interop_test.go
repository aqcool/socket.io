package engine

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	engineServer "github.com/aqcool/socket.io/servers/engine/v4"
	serverConfig "github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
	gorillaws "github.com/gorilla/websocket"
)

const officialClientHandshake = `0{"sid":"official","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`

type officialCustomPolling struct{ Polling }
type officialCustomWebSocket struct{ WebSocket }

type officialCustomPollingBuilder struct{}

func (*officialCustomPollingBuilder) Name() string { return transports.POLLING }

func (*officialCustomPollingBuilder) New(socket Socket, opts SocketOptionsInterface) Transport {
	base := NewPolling(socket, opts)
	custom := &officialCustomPolling{Polling: base}
	base.Prototype(custom)
	return custom
}

type officialCustomWebSocketBuilder struct{}

func (*officialCustomWebSocketBuilder) Name() string { return transports.WEBSOCKET }

func (*officialCustomWebSocketBuilder) New(socket Socket, opts SocketOptionsInterface) Transport {
	base := NewWebSocket(socket, opts)
	custom := &officialCustomWebSocket{WebSocket: base}
	base.Prototype(custom)
	return custom
}

func TestOfficialClientCustomTransportImplementations(t *testing.T) {
	for _, builder := range []TransportCtor{&officialCustomPollingBuilder{}, &officialCustomWebSocketBuilder{}} {
		t.Run(builder.Name(), func(t *testing.T) {
			serverOptions := serverConfig.DefaultServerOptions()
			serverOptions.SetAllowUpgrades(false)
			server := engineServer.NewServer(serverOptions)
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})

			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{builder})
			opts.SetUpgrade(false)
			client := NewSocket(httpServer.URL, opts)
			t.Cleanup(func() { client.Close() })
			opened := make(chan string, 1)
			_ = client.Once("open", func(...any) { opened <- client.Transport().Name() })
			select {
			case got := <-opened:
				if got != builder.Name() {
					t.Fatalf("custom transport = %q, want %q", got, builder.Name())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("custom transport did not connect")
			}
		})
	}
}

func TestOfficialClientPollingToWebSocketUpgrade(t *testing.T) {
	server := engineServer.NewServer(nil)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	_ = server.On("connection", func(args ...any) {
		engineSocket := args[0].(engineServer.Socket)
		_ = engineSocket.On("message", func(message ...any) {
			engineSocket.Send(message[0].(types.BufferInterface).Clone(), nil, nil)
		})
		_ = engineSocket.Once("upgrade", func(...any) {
			engineSocket.Send(types.NewStringBufferString("server-after-upgrade"), nil, nil)
		})
	})

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}, &WebSocketBuilder{}})
	client := NewSocket(httpServer.URL, opts)
	t.Cleanup(func() { client.Close() })
	openedWith := make(chan string, 1)
	upgradedTo := make(chan string, 1)
	messages := make(chan string, 2)
	closed := make(chan string, 1)
	_ = client.Once("open", func(...any) {
		openedWith <- client.Transport().Name()
	})
	_ = client.Once("upgrade", func(args ...any) {
		upgradedTo <- args[0].(Transport).Name()
		client.Send(types.NewStringBufferString("client-after-upgrade"), nil, nil)
	})
	_ = client.On("message", func(args ...any) {
		messages <- args[0].(types.BufferInterface).String()
	})
	_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })

	select {
	case got := <-openedWith:
		if got != transports.POLLING {
			t.Fatalf("opened with %q, want polling", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not open")
	}
	select {
	case got := <-upgradedTo:
		if got != transports.WEBSOCKET {
			t.Fatalf("upgraded to %q, want websocket", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not upgrade to websocket")
	}
	gotMessages := map[string]bool{}
	for range 2 {
		select {
		case got := <-messages:
			gotMessages[got] = true
		case reason := <-closed:
			t.Fatalf("client closed during upgraded bidirectional traffic: %q", reason)
		case <-time.After(3 * time.Second):
			t.Fatalf("upgraded messages = %#v, want client echo and server push", gotMessages)
		}
	}
	if !gotMessages["client-after-upgrade"] || !gotMessages["server-after-upgrade"] {
		t.Fatalf("upgraded messages = %#v", gotMessages)
	}
	select {
	case reason := <-closed:
		t.Fatalf("client closed unexpectedly after upgrade: %q", reason)
	default:
	}
}

func TestOfficialClientForceBase64RoundTrip(t *testing.T) {
	for _, builder := range []TransportCtor{&PollingBuilder{}, &WebSocketBuilder{}} {
		t.Run(builder.Name(), func(t *testing.T) {
			server := engineServer.NewServer(nil)
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(engineServer.Socket)
				_ = socket.Once("message", func(message ...any) {
					socket.Send(message[0].(types.BufferInterface).Clone(), nil, nil)
				})
			})

			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{builder})
			opts.SetUpgrade(false)
			opts.SetForceBase64(true)
			client := NewSocket(httpServer.URL, opts)
			t.Cleanup(func() { client.Close() })
			want := []byte{0, 1, 2, 127, 128, 254, 255}
			received := make(chan []byte, 1)
			_ = client.Once("open", func(...any) {
				client.Send(types.NewBytesBuffer(want), nil, nil)
			})
			_ = client.Once("message", func(args ...any) {
				received <- append([]byte(nil), args[0].(types.BufferInterface).Bytes()...)
			})
			select {
			case got := <-received:
				if !bytes.Equal(got, want) {
					t.Fatalf("base64 round trip = %v, want %v", got, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("forced-base64 message did not round-trip")
			}
		})
	}
}

func TestOfficialClientMixedBinaryMaxPayloadSequence(t *testing.T) {
	options := serverConfig.DefaultServerOptions()
	options.SetAllowUpgrades(false)
	options.SetMaxHttpBufferSize(100)
	server := engineServer.NewServer(options)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(engineServer.Socket)
		_ = socket.On("message", func(message ...any) {
			socket.Send(message[0].(types.BufferInterface).Clone(), nil, nil)
		})
	})

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
	opts.SetUpgrade(false)
	client := NewSocket(httpServer.URL, opts)
	t.Cleanup(func() { client.Close() })
	want := [][]byte{
		make([]byte, 72),
		make([]byte, 20),
		[]byte(strings.Repeat("a", 20)),
		make([]byte, 20),
		make([]byte, 72),
	}
	received := make(chan []byte, len(want))
	_ = client.Once("open", func(...any) {
		client.Send(types.NewBytesBuffer(want[0]), nil, nil)
		client.Send(types.NewBytesBuffer(want[1]), nil, nil)
		client.Send(types.NewStringBuffer(want[2]), nil, nil)
		client.Send(types.NewBytesBuffer(want[3]), nil, nil)
		client.Send(types.NewBytesBuffer(want[4]), nil, nil)
	})
	_ = client.On("message", func(args ...any) {
		received <- append([]byte(nil), args[0].(types.BufferInterface).Bytes()...)
	})
	for index, expected := range want {
		select {
		case actual := <-received:
			if !bytes.Equal(actual, expected) {
				t.Fatalf("message %d length/content = %d/%t", index, len(actual), bytes.Equal(actual, expected))
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing maxPayload message %d", index)
		}
	}
}

func TestOfficialClientPollingWritesWhileLongPollIsPending(t *testing.T) {
	serverOptions := serverConfig.DefaultServerOptions()
	serverOptions.SetAllowUpgrades(false)
	server := engineServer.NewServer(serverOptions)
	secondPoll := make(chan struct{}, 1)
	var pollingGets atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Query().Get("transport") == transports.POLLING {
			if pollingGets.Add(1) == 2 {
				secondPoll <- struct{}{}
			}
		}
		server.ServeHTTP(writer, request)
	}))
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	serverReceived := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		socket := args[0].(engineServer.Socket)
		_ = socket.Once("message", func(message ...any) {
			serverReceived <- message[0].(types.BufferInterface).String()
		})
	})

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
	opts.SetUpgrade(false)
	client := NewSocket(httpServer.URL, opts)
	t.Cleanup(func() { client.Close() })

	select {
	case <-secondPoll:
		client.Send(types.NewStringBufferString("write-during-poll"), nil, nil)
	case <-time.After(3 * time.Second):
		t.Fatal("client did not start its long-poll request")
	}

	select {
	case got := <-serverReceived:
		if got != "write-during-poll" {
			t.Fatalf("server message = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("polling POST was blocked behind the pending GET")
	}
}

func TestOfficialClientBurstWriteOrdering(t *testing.T) {
	for _, builder := range []TransportCtor{&PollingBuilder{}, &WebSocketBuilder{}} {
		t.Run(builder.Name(), func(t *testing.T) {
			server := engineServer.NewServer(nil)
			httpServer := httptest.NewServer(server)
			t.Cleanup(func() {
				server.Close()
				httpServer.Close()
			})
			_ = server.On("connection", func(args ...any) {
				socket := args[0].(engineServer.Socket)
				_ = socket.On("message", func(message ...any) {
					socket.Send(message[0].(types.BufferInterface).Clone(), nil, nil)
				})
			})

			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{builder})
			opts.SetUpgrade(false)
			opts.SetPerMessageDeflate(&types.PerMessageDeflate{Threshold: 0})
			client := NewSocket(httpServer.URL, opts)
			t.Cleanup(func() { client.Close() })
			const total = 128
			received := make(chan string, total)
			callbacks := make(chan int, total)
			closed := make(chan string, 1)
			_ = client.On("message", func(args ...any) {
				received <- args[0].(types.BufferInterface).String()
			})
			_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })
			_ = client.Once("open", func(...any) {
				for index := range total {
					value := index
					compress := index%2 == 0
					payload := fmt.Sprintf("burst-%03d", index)
					var message io.Reader = types.NewStringBufferString(payload)
					// Model Socket.IO's binary EVENT sequence: a text header,
					// binary attachment, standalone text EVENT, then another
					// text header and binary attachment.
					if index%5 == 1 || index%5 == 4 {
						message = types.NewBytesBufferString(payload)
					}
					client.Send(
						message,
						&packet.Options{Compress: &compress},
						func() { callbacks <- value },
					)
				}
			})

			for want := range total {
				select {
				case got := <-received:
					expected := fmt.Sprintf("burst-%03d", want)
					if got != expected {
						t.Fatalf("message %d = %q, want %q", want, got, expected)
					}
				case reason := <-closed:
					t.Fatalf("transport closed during burst at %d: %q", want, reason)
				case <-time.After(5 * time.Second):
					t.Fatalf("timed out after %d ordered burst messages", want)
				}
			}
			seenCallbacks := make(map[int]bool, total)
			for range total {
				select {
				case value := <-callbacks:
					if seenCallbacks[value] {
						t.Fatalf("duplicate send callback %d", value)
					}
					seenCallbacks[value] = true
				case <-time.After(time.Second):
					t.Fatalf("send callbacks = %d, want %d", len(seenCallbacks), total)
				}
			}
		})
	}
}

func TestOfficialClientMixedBurstWireFrames(t *testing.T) {
	const total = 128
	type wireFrame struct {
		messageType int
		payload     []byte
	}
	frames := make(chan wireFrame, total)
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		if err := connection.WriteMessage(gorillaws.TextMessage, []byte(officialClientHandshake)); err != nil {
			return
		}
		for range total {
			messageType, payload, err := connection.ReadMessage()
			if err != nil {
				return
			}
			frames <- wireFrame{messageType: messageType, payload: payload}
		}
	}))
	t.Cleanup(server.Close)

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&WebSocketBuilder{}})
	opts.SetUpgrade(false)
	client := NewSocket(server.URL, opts)
	t.Cleanup(func() { client.Close() })
	_ = client.Once("open", func(...any) {
		for index := range total {
			payload := fmt.Sprintf("wire-%03d", index)
			var message io.Reader = types.NewStringBufferString(payload)
			if index%5 == 1 || index%5 == 4 {
				message = types.NewBytesBufferString(payload)
			}
			client.Send(message, nil, nil)
		}
	})

	for index := range total {
		select {
		case frame := <-frames:
			wantPayload := fmt.Appendf(nil, "wire-%03d", index)
			wantType := gorillaws.BinaryMessage
			if index%5 != 1 && index%5 != 4 {
				wantType = gorillaws.TextMessage
				wantPayload = append([]byte{'4'}, wantPayload...)
			}
			if frame.messageType != wantType || !bytes.Equal(frame.payload, wantPayload) {
				t.Fatalf("wire frame %d = type %d payload %q, want type %d payload %q", index, frame.messageType, frame.payload, wantType, wantPayload)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing wire frame %d", index)
		}
	}
}

func TestOfficialClientPollingTextAndBinaryRoundTrip(t *testing.T) {
	payloads := make(chan []byte, 4)
	var handshake sync.Once
	var echoed atomic.Int32
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		if request.Method == http.MethodPost {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			select {
			case payloads <- body:
			case <-request.Context().Done():
				return
			}
			_, _ = writer.Write([]byte("ok"))
			return
		}

		initial := false
		handshake.Do(func() { initial = true })
		if initial {
			_, _ = writer.Write([]byte(officialClientHandshake))
			return
		}
		if echoed.Load() >= 2 {
			_, _ = writer.Write([]byte("1"))
			return
		}
		select {
		case payload := <-payloads:
			echoed.Add(1)
			_, _ = writer.Write(payload)
		case <-time.After(2 * time.Second):
			_, _ = writer.Write([]byte("1"))
		case <-request.Context().Done():
		}
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
	opts.SetUpgrade(false)
	client := NewSocket(server.URL, opts)
	t.Cleanup(func() { client.Close() })

	want := [][]byte{
		[]byte("cash money €€€ / 😀 / пойду спать"),
		{0, 1, 2, 127, 128, 254, 255},
	}
	_ = client.Once("open", func(...any) {
		client.Send(types.NewStringBuffer(want[0]), nil, nil)
		client.Send(types.NewBytesBuffer(want[1]), nil, nil)
	})
	received := make(chan []byte, len(want))
	_ = client.On("message", func(args ...any) {
		reader, ok := args[0].(io.Reader)
		if !ok {
			received <- fmt.Appendf(nil, "unexpected message type %T", args[0])
			return
		}
		data, _ := io.ReadAll(reader)
		received <- data
	})

	for i, expected := range want {
		select {
		case actual := <-received:
			if !bytes.Equal(actual, expected) {
				t.Fatalf("message %d = %q, want %q", i, actual, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestOfficialClientWebSocketTextAndBinaryRoundTrip(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		if err := connection.WriteMessage(gorillaws.TextMessage, []byte(officialClientHandshake)); err != nil {
			return
		}
		for range 2 {
			messageType, payload, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if err := connection.WriteMessage(messageType, payload); err != nil {
				return
			}
		}
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&WebSocketBuilder{}})
	opts.SetUpgrade(false)
	client := NewSocket(server.URL, opts)
	t.Cleanup(func() { client.Close() })
	want := [][]byte{[]byte("€€€ 😀"), {0, 1, 127, 128, 255}}
	_ = client.Once("open", func(...any) {
		client.Send(types.NewStringBuffer(want[0]), nil, nil)
		client.Send(types.NewBytesBuffer(want[1]), nil, nil)
	})
	received := make(chan []byte, len(want))
	_ = client.On("message", func(args ...any) {
		reader, ok := args[0].(io.Reader)
		if !ok {
			received <- fmt.Appendf(nil, "unexpected message type %T", args[0])
			return
		}
		data, _ := io.ReadAll(reader)
		received <- data
	})

	for i, expected := range want {
		select {
		case actual := <-received:
			if !bytes.Equal(actual, expected) {
				t.Fatalf("message %d = %v, want %v", i, actual, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestOfficialClientPollingCookiesFollowWithCredentials(t *testing.T) {
	for _, withCredentials := range []bool{true, false} {
		t.Run(fmt.Sprintf("withCredentials=%v", withCredentials), func(t *testing.T) {
			cookies := make(chan string, 1)
			var first sync.Once
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
				initial := false
				first.Do(func() { initial = true })
				if initial {
					http.SetCookie(writer, &http.Cookie{Name: "one", Value: "1", Path: "/"})
					http.SetCookie(writer, &http.Cookie{Name: "two", Value: "2", Path: "/"})
					_, _ = writer.Write([]byte(officialClientHandshake))
					return
				}
				select {
				case cookies <- request.Header.Get("Cookie"):
				default:
				}
				if request.Method == http.MethodPost {
					_, _ = writer.Write([]byte("ok"))
				} else {
					_, _ = writer.Write([]byte("1"))
				}
			})
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)

			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
			opts.SetUpgrade(false)
			opts.SetWithCredentials(withCredentials)
			client := NewSocket(server.URL, opts)
			t.Cleanup(func() { client.Close() })

			select {
			case got := <-cookies:
				if withCredentials {
					if !strings.Contains(got, "one=1") || !strings.Contains(got, "two=2") {
						t.Fatalf("Cookie = %q, want both server cookies", got)
					}
				} else if got != "" {
					t.Fatalf("Cookie = %q, want none", got)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("server did not receive a follow-up polling request")
			}
		})
	}
}
