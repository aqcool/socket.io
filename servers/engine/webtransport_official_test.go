package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	enginetransports "github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/types"
	enginewebtransport "github.com/aqcool/socket.io/v3/pkg/webtransport"
	"github.com/quic-go/quic-go/http3"
	webtransportgo "github.com/quic-go/webtransport-go"
)

type officialWebTransportHarness struct {
	engine        Server
	webTransport  *webtransportgo.Server
	httpServer    *http.Server
	tcpListener   net.Listener
	udpConnection net.PacketConn
	HTTPURL       string
	WebURL        string
	clientTLS     *tls.Config
}

func newOfficialWebTransportHarness(t *testing.T, configure func(*config.ServerOptions)) *officialWebTransportHarness {
	t.Helper()
	serverCertificate, _, roots := makeOfficialTLSCertificates(t)
	options := config.DefaultServerOptions()
	options.SetTransports(types.NewSet[enginetransports.TransportCtor](
		&enginetransports.PollingBuilder{},
		&enginetransports.WebSocketBuilder{},
		&enginetransports.WebTransportBuilder{},
	))
	options.SetPingInterval(time.Minute)
	options.SetPingTimeout(time.Minute)
	if configure != nil {
		configure(options)
	}
	engineServer := NewServer(options)

	var tcpListener net.Listener
	var udpConnection net.PacketConn
	var err error
	for attempt := range 20 {
		tcpListener, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			continue
		}
		udpConnection, err = net.ListenPacket("udp4", tcpListener.Addr().String())
		if err == nil {
			break
		}
		_ = tcpListener.Close()
		if attempt == 19 {
			t.Fatalf("reserving matching TCP/UDP WebTransport port: %v", err)
		}
	}
	if tcpListener == nil || udpConnection == nil {
		t.Fatalf("reserving matching TCP/UDP WebTransport port: %v", err)
	}

	harness := &officialWebTransportHarness{
		engine:        engineServer,
		tcpListener:   tcpListener,
		udpConnection: udpConnection,
		HTTPURL:       "http://" + tcpListener.Addr().String(),
		WebURL:        "https://" + tcpListener.Addr().String() + "/engine.io/",
		clientTLS:     &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13},
	}
	h3Server := &http3.Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{serverCertificate}, MinVersion: tls.VersionTLS13, NextProtos: []string{http3.NextProtoH3}},
	}
	webtransportgo.ConfigureHTTP3Server(h3Server)
	harness.webTransport = &webtransportgo.Server{H3: h3Server}
	h3Server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		engineServer.OnWebTransportSession(types.NewHttpContext(writer, request), harness.webTransport)
	})
	harness.httpServer = &http.Server{Handler: engineServer, ReadHeaderTimeout: time.Second}
	go func() {
		if serveErr := harness.httpServer.Serve(tcpListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("WebTransport companion HTTP server: %v", serveErr)
		}
	}()
	go func() {
		if serveErr := harness.webTransport.Serve(udpConnection); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, context.Canceled) {
			t.Errorf("WebTransport HTTP/3 server: %v", serveErr)
		}
	}()
	t.Cleanup(func() {
		engineServer.Close()
		_ = harness.webTransport.Close()
		_ = harness.httpServer.Close()
		_ = harness.udpConnection.Close()
		_ = harness.tcpListener.Close()
	})
	return harness
}

func (harness *officialWebTransportHarness) dialSession(t *testing.T) *webtransportgo.Session {
	t.Helper()
	dialer := &webtransportgo.Dialer{TLSClientConfig: harness.clientTLS.Clone()}
	var session *webtransportgo.Session
	var err error
	for attempt := range 20 {
		_, session, err = dialer.Dial(context.Background(), harness.WebURL, nil)
		if err == nil {
			break
		}
		if attempt == 19 {
			t.Fatalf("dialing real WebTransport session: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return session
}

func (harness *officialWebTransportHarness) dial(t *testing.T) (*webtransportgo.Session, *enginewebtransport.Conn) {
	t.Helper()
	session := harness.dialSession(t)
	stream, err := session.OpenStreamSync(context.Background())
	if err != nil {
		_ = session.CloseWithError(0, "")
		t.Fatalf("opening WebTransport bidirectional stream: %v", err)
	}
	connection := enginewebtransport.NewConn(session, stream, false, 0, 1_100_000, nil, nil, nil)
	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("0")); err != nil {
		_ = session.CloseWithError(0, "")
		t.Fatalf("writing WebTransport Engine.IO handshake: %v", err)
	}
	return session, connection
}

func readOfficialWebTransportMessage(t *testing.T, connection *enginewebtransport.Conn) (int, []byte) {
	t.Helper()
	messageType, reader, err := connection.NextReader()
	if err != nil {
		t.Fatalf("reading WebTransport frame: %v", err)
	}
	payload, readErr := io.ReadAll(reader)
	if closer, ok := reader.(io.Closer); ok {
		_ = closer.Close()
	}
	if readErr != nil {
		t.Fatalf("reading WebTransport payload: %v", readErr)
	}
	return messageType, payload
}

func TestOfficialWebTransportDirectMessagesAndCallbacks(t *testing.T) {
	harness := newOfficialWebTransportHarness(t, nil)
	connected := make(chan Socket, 1)
	textFromClient := make(chan string, 1)
	binaryFromClient := make(chan []byte, 2)
	_ = harness.engine.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		connected <- socket
		_ = socket.On("data", func(dataArgs ...any) {
			data := dataArgs[0].(types.BufferInterface)
			if _, ok := data.(*types.StringBuffer); ok {
				textFromClient <- data.String()
			} else {
				binaryFromClient <- append([]byte(nil), data.Bytes()...)
			}
		})
	})

	session, connection := harness.dial(t)
	t.Cleanup(func() { _ = session.CloseWithError(0, "") })
	messageType, open := readOfficialWebTransportMessage(t, connection)
	if messageType != enginewebtransport.TextMessage || !bytes.HasPrefix(open, []byte("0{")) || !bytes.Contains(open, []byte(`"sid":`)) {
		t.Fatalf("WebTransport OPEN = type %d, %q", messageType, open)
	}
	var socket Socket
	select {
	case socket = <-connected:
		if socket.Transport().Name() != enginetransports.WEBTRANSPORT {
			t.Fatalf("direct transport = %q", socket.Transport().Name())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("direct WebTransport connection was not emitted")
	}

	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("4hello")); err != nil {
		t.Fatalf("writing client text: %v", err)
	}
	select {
	case got := <-textFromClient:
		if got != "hello" {
			t.Fatalf("client text = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client text did not reach server")
	}

	socket.Send(types.NewStringBufferString("world"), nil, nil)
	messageType, payload := readOfficialWebTransportMessage(t, connection)
	if messageType != enginewebtransport.TextMessage || string(payload) != "4world" {
		t.Fatalf("server text = type %d, %q", messageType, payload)
	}

	for _, size := range []int{3, 1_000_000} {
		binary := make([]byte, size)
		for index := range binary {
			binary[index] = byte(index % 251)
		}
		if err := connection.WriteMessage(enginewebtransport.BinaryMessage, binary); err != nil {
			t.Fatalf("writing client binary (%d): %v", size, err)
		}
		select {
		case got := <-binaryFromClient:
			if !bytes.Equal(got, binary) {
				t.Fatalf("client binary size/content = %d/%t, want %d/true", len(got), bytes.Equal(got, binary), size)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("client binary (%d) did not reach server", size)
		}

		socket.Send(types.NewBytesBuffer(binary), nil, nil)
		messageType, payload = readOfficialWebTransportMessage(t, connection)
		if messageType != enginewebtransport.BinaryMessage || !bytes.Equal(payload, binary) {
			t.Fatalf("server binary = type %d/size %d, want binary/%d", messageType, len(payload), size)
		}
	}

	callbacks := make(chan int, 4)
	for index := range 4 {
		value := index
		socket.Send(types.NewStringBufferString("callback:"+strconv.Itoa(value)), nil, func(enginetransports.Transport) {
			callbacks <- value
		})
	}
	for want := range 4 {
		messageType, payload = readOfficialWebTransportMessage(t, connection)
		if messageType != enginewebtransport.TextMessage || string(payload) != "4callback:"+strconv.Itoa(want) {
			t.Fatalf("callback message %d = type %d/%q", want, messageType, payload)
		}
		select {
		case got := <-callbacks:
			if got != want {
				t.Fatalf("callback order = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("callback %d did not execute", want)
		}
	}
}

func TestOfficialWebTransportUpgradeFromPolling(t *testing.T) {
	harness := newOfficialWebTransportHarness(t, nil)
	connected := make(chan Socket, 1)
	upgraded := make(chan enginetransports.Transport, 1)
	received := make(chan string, 1)
	_ = harness.engine.On("connection", func(args ...any) {
		socket := args[0].(Socket)
		connected <- socket
		_ = socket.Once("upgrade", func(upgradeArgs ...any) {
			upgraded <- upgradeArgs[0].(enginetransports.Transport)
		})
		_ = socket.Once("message", func(messageArgs ...any) {
			received <- messageArgs[0].(types.BufferInterface).String()
		})
	})

	response, err := http.Get(harness.HTTPURL + "/engine.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("Polling handshake before WebTransport upgrade: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("Polling handshake = %d/%q, error=%v", response.StatusCode, body, readErr)
	}
	if !bytes.Contains(body, []byte(`"webtransport"`)) {
		t.Fatalf("Polling OPEN does not advertise WebTransport: %q", body)
	}
	marker := []byte(`"sid":"`)
	start := bytes.Index(body, marker)
	if start == -1 {
		t.Fatalf("Polling OPEN has no SID: %q", body)
	}
	start += len(marker)
	end := bytes.IndexByte(body[start:], '"')
	sid := string(body[start : start+end])
	socket := <-connected

	session := harness.dialSession(t)
	t.Cleanup(func() { _ = session.CloseWithError(0, "") })
	stream, err := session.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("opening upgrade stream: %v", err)
	}
	connection := enginewebtransport.NewConn(session, stream, false, 0, 1_100_000, nil, nil, nil)
	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte(`0{"sid":"`+sid+`"}`)); err != nil {
		t.Fatalf("writing WebTransport upgrade handshake: %v", err)
	}
	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("2probe")); err != nil {
		t.Fatalf("writing WebTransport probe: %v", err)
	}
	messageType, probe := readOfficialWebTransportMessage(t, connection)
	if messageType != enginewebtransport.TextMessage || string(probe) != "3probe" {
		t.Fatalf("WebTransport probe response = type %d/%q", messageType, probe)
	}
	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("5")); err != nil {
		t.Fatalf("completing WebTransport upgrade: %v", err)
	}
	select {
	case transport := <-upgraded:
		if transport.Name() != enginetransports.WEBTRANSPORT || socket.Transport().Name() != enginetransports.WEBTRANSPORT {
			t.Fatalf("upgrade transport/current = %q/%q", transport.Name(), socket.Transport().Name())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Polling socket did not upgrade to WebTransport")
	}

	if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("4after-upgrade")); err != nil {
		t.Fatalf("writing after WebTransport upgrade: %v", err)
	}
	select {
	case got := <-received:
		if got != "after-upgrade" {
			t.Fatalf("post-upgrade client message = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-upgrade client message did not arrive")
	}
	socket.Send(types.NewStringBufferString("server-after-upgrade"), nil, nil)
	messageType, payload := readOfficialWebTransportMessage(t, connection)
	if messageType != enginewebtransport.TextMessage || string(payload) != "4server-after-upgrade" {
		t.Fatalf("post-upgrade server message = type %d/%q", messageType, payload)
	}
}

func TestOfficialWebTransportPingPongAndCloseLifecycle(t *testing.T) {
	t.Run("ping pong", func(t *testing.T) {
		harness := newOfficialWebTransportHarness(t, func(options *config.ServerOptions) {
			options.SetPingInterval(20 * time.Millisecond)
			options.SetPingTimeout(time.Second)
		})
		session, connection := harness.dial(t)
		t.Cleanup(func() { _ = session.CloseWithError(0, "") })
		_, _ = readOfficialWebTransportMessage(t, connection)
		for range 5 {
			messageType, ping := readOfficialWebTransportMessage(t, connection)
			if messageType != enginewebtransport.TextMessage || string(ping) != "2" {
				t.Fatalf("WebTransport PING = type %d/%q", messageType, ping)
			}
			if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte("3")); err != nil {
				t.Fatalf("writing WebTransport PONG: %v", err)
			}
		}
	})

	t.Run("ping timeout", func(t *testing.T) {
		harness := newOfficialWebTransportHarness(t, func(options *config.ServerOptions) {
			options.SetPingInterval(20 * time.Millisecond)
			options.SetPingTimeout(30 * time.Millisecond)
		})
		closed := make(chan string, 1)
		_ = harness.engine.On("connection", func(args ...any) {
			_ = args[0].(Socket).Once("close", func(closeArgs ...any) { closed <- closeArgs[0].(string) })
		})
		session, connection := harness.dial(t)
		_, _ = readOfficialWebTransportMessage(t, connection)
		select {
		case reason := <-closed:
			if reason != "ping timeout" {
				t.Fatalf("ping-timeout reason = %q", reason)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("WebTransport socket did not close on ping timeout")
		}
		select {
		case <-session.Context().Done():
		case <-time.After(2 * time.Second):
			t.Fatal("WebTransport session remained open after ping timeout")
		}
	})

	t.Run("server close", func(t *testing.T) {
		harness := newOfficialWebTransportHarness(t, nil)
		connected := make(chan Socket, 1)
		_ = harness.engine.On("connection", func(args ...any) { connected <- args[0].(Socket) })
		session, connection := harness.dial(t)
		_, _ = readOfficialWebTransportMessage(t, connection)
		(<-connected).Close(false)
		select {
		case <-session.Context().Done():
		case <-time.After(2 * time.Second):
			t.Fatal("server close did not close WebTransport session")
		}
	})

	t.Run("client close", func(t *testing.T) {
		harness := newOfficialWebTransportHarness(t, nil)
		closed := make(chan []any, 1)
		_ = harness.engine.On("connection", func(args ...any) {
			_ = args[0].(Socket).Once("close", func(closeArgs ...any) { closed <- closeArgs })
		})
		session, connection := harness.dial(t)
		_, _ = readOfficialWebTransportMessage(t, connection)
		if err := session.CloseWithError(0, "client close"); err != nil {
			t.Fatalf("closing client WebTransport session: %v", err)
		}
		select {
		case closeArgs := <-closed:
			reason := closeArgs[0].(string)
			if reason != "transport close" {
				t.Fatalf("client-close reason/details = %q/%v", reason, closeArgs[1:])
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not observe WebTransport client close")
		}
	})
}

func TestOfficialWebTransportRejectsMissingStreamAndInvalidHandshake(t *testing.T) {
	for _, test := range []struct {
		name         string
		openStream   bool
		invalidBytes []byte
	}{
		{name: "missing bidirectional stream"},
		{name: "invalid handshake", openStream: true, invalidBytes: []byte{1, 2, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newOfficialWebTransportHarness(t, func(options *config.ServerOptions) {
				options.SetUpgradeTimeout(50 * time.Millisecond)
			})
			session := harness.dialSession(t)
			if test.openStream {
				stream, streamErr := session.OpenStreamSync(context.Background())
				if streamErr != nil {
					t.Fatalf("opening invalid handshake stream: %v", streamErr)
				}
				if _, writeErr := stream.Write(test.invalidBytes); writeErr != nil {
					t.Fatalf("writing invalid handshake: %v", writeErr)
				}
			}
			select {
			case <-session.Context().Done():
			case <-time.After(2 * time.Second):
				t.Fatalf("%s session remained open", test.name)
			}
		})
	}
}

func TestOfficialEngineIO669WebTransportRejectsUnknownUpgradeSID(t *testing.T) {
	for _, sid := range []string{"11111111111111111111", "__proto__"} {
		t.Run(sid, func(t *testing.T) {
			harness := newOfficialWebTransportHarness(t, nil)

			response, err := http.Get(harness.HTTPURL + "/engine.io/?EIO=4&transport=polling")
			if err != nil {
				t.Fatalf("Polling handshake before invalid WebTransport upgrade: %v", err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK {
				t.Fatalf("Polling handshake = %d/%q, error=%v", response.StatusCode, body, readErr)
			}
			opened := decodeOfficialOpenPacket(t, body)
			if !slices.Equal(opened.Upgrades, []string{"websocket", "webtransport"}) {
				t.Fatalf("Polling upgrades = %#v, want websocket and webtransport", opened.Upgrades)
			}

			session := harness.dialSession(t)
			t.Cleanup(func() { _ = session.CloseWithError(0, "") })
			stream, err := session.OpenStreamSync(context.Background())
			if err != nil {
				t.Fatalf("opening invalid WebTransport upgrade stream: %v", err)
			}
			connection := enginewebtransport.NewConn(session, stream, false, 0, 1_100_000, nil, nil, nil)
			if err := connection.WriteMessage(enginewebtransport.TextMessage, []byte(`0{"sid":"`+sid+`"}`)); err != nil {
				t.Fatalf("writing invalid WebTransport upgrade: %v", err)
			}

			select {
			case <-session.Context().Done():
			case <-time.After(2 * time.Second):
				t.Fatalf("invalid WebTransport SID %q session remained open", sid)
			}
			if _, ok := harness.engine.Clients().Load(opened.SID); !ok || harness.engine.ClientsCount() != 1 {
				t.Fatalf("invalid upgrade affected polling client: present=%t clients=%d", ok, harness.engine.ClientsCount())
			}
		})
	}
}

func TestOfficialEngineIO669WebTransportRejectsRegisteredMiddleware(t *testing.T) {
	harness := newOfficialWebTransportHarness(t, nil)
	connections := make(chan struct{}, 1)
	_ = harness.engine.On("connection", func(...any) { connections <- struct{}{} })
	harness.engine.Use(func(_ *types.HttpContext, next func(error)) { next(nil) })

	session := harness.dialSession(t)
	t.Cleanup(func() { _ = session.CloseWithError(0, "") })
	select {
	case <-session.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("WebTransport session with registered middleware remained open")
	}
	select {
	case <-connections:
		t.Fatal("WebTransport emitted connection with registered middleware")
	default:
	}
	if harness.engine.ClientsCount() != 0 {
		t.Fatalf("WebTransport with middleware registered %d clients", harness.engine.ClientsCount())
	}
}
