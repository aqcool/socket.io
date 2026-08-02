package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	engineServer "github.com/aqcool/socket.io/servers/engine/v3"
	serverConfig "github.com/aqcool/socket.io/servers/engine/v3/config"
	serverTransports "github.com/aqcool/socket.io/servers/engine/v3/transports"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/quic-go/quic-go/http3"
	webtransportgo "github.com/quic-go/webtransport-go"
)

type officialClientWebTransportHarness struct {
	engine        engineServer.Server
	webTransport  *webtransportgo.Server
	httpServer    *http.Server
	tcpListener   net.Listener
	udpConnection net.PacketConn
	httpURL       string
	clientTLS     *tls.Config
}

func newOfficialClientWebTransportHarness(
	t *testing.T,
	configure func(*serverConfig.ServerOptions),
) *officialClientWebTransportHarness {
	t.Helper()
	certificateSource := httptest.NewTLSServer(http.NotFoundHandler())
	serverCertificate := certificateSource.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certificateSource.Certificate())
	certificateSource.Close()

	options := serverConfig.DefaultServerOptions()
	options.SetTransports(types.NewSet[serverTransports.TransportCtor](
		&serverTransports.PollingBuilder{},
		&serverTransports.WebSocketBuilder{},
		&serverTransports.WebTransportBuilder{},
	))
	options.SetPingInterval(time.Minute)
	options.SetPingTimeout(time.Minute)
	if configure != nil {
		configure(options)
	}
	engine := engineServer.NewServer(options)

	var tcpListener net.Listener
	var udpConnection net.PacketConn
	var err error
	for range 20 {
		tcpListener, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			continue
		}
		udpConnection, err = net.ListenPacket("udp4", tcpListener.Addr().String())
		if err == nil {
			break
		}
		_ = tcpListener.Close()
	}
	if tcpListener == nil || udpConnection == nil {
		engine.Close()
		t.Skipf("matching TCP/UDP WebTransport port unavailable: %v", err)
	}

	h3Server := &http3.Server{TLSConfig: &tls.Config{
		Certificates: []tls.Certificate{serverCertificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{http3.NextProtoH3},
	}}
	webTransport := &webtransportgo.Server{H3: h3Server}
	webtransportgo.ConfigureHTTP3Server(h3Server)
	h3Server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		engine.OnWebTransportSession(types.NewHttpContext(writer, request), webTransport)
	})
	harness := &officialClientWebTransportHarness{
		engine:        engine,
		webTransport:  webTransport,
		httpServer:    &http.Server{Handler: engine, ReadHeaderTimeout: time.Second},
		tcpListener:   tcpListener,
		udpConnection: udpConnection,
		httpURL:       "http://" + tcpListener.Addr().String(),
		clientTLS: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    roots,
		},
	}
	serverErrors := make(chan error, 2)
	go func() {
		if serveErr := harness.httpServer.Serve(tcpListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErrors <- serveErr
		}
	}()
	go func() {
		if serveErr := webTransport.Serve(udpConnection); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, context.Canceled) {
			serverErrors <- serveErr
		}
	}()
	t.Cleanup(func() {
		engine.Close()
		_ = webTransport.Close()
		_ = harness.httpServer.Close()
		_ = udpConnection.Close()
		_ = tcpListener.Close()
		select {
		case serveErr := <-serverErrors:
			t.Errorf("WebTransport harness: %v", serveErr)
		default:
		}
	})
	return harness
}

func (h *officialClientWebTransportHarness) clientOptions(builders ...TransportCtor) *SocketOptions {
	opts := DefaultSocketOptions()
	opts.SetTransportList(builders)
	opts.SetTLSClientConfig(h.clientTLS.Clone())
	return opts
}

func TestOfficialClientWebTransportDirectTrafficAndHeartbeat(t *testing.T) {
	harness := newOfficialClientWebTransportHarness(t, func(options *serverConfig.ServerOptions) {
		options.SetPingInterval(20 * time.Millisecond)
		options.SetPingTimeout(time.Second)
	})
	connected := make(chan engineServer.Socket, 1)
	heartbeat := make(chan struct{}, 1)
	_ = harness.engine.On("connection", func(args ...any) {
		socket := args[0].(engineServer.Socket)
		connected <- socket
		_ = socket.On("message", func(message ...any) {
			socket.Send(message[0].(types.BufferInterface).Clone(), nil, nil)
		})
		_ = socket.Once("heartbeat", func(...any) { heartbeat <- struct{}{} })
	})

	client := NewSocket(harness.httpURL, harness.clientOptions(&WebTransportBuilder{}))
	t.Cleanup(func() { client.Close() })
	opened := make(chan struct{}, 1)
	messages := make(chan types.BufferInterface, 2)
	_ = client.Once("open", func(...any) {
		opened <- struct{}{}
		client.Send(types.NewStringBufferString("webtransport-text"), nil, nil)
		client.Send(types.NewBytesBuffer([]byte{0, 1, 2, 127, 128, 255}), nil, nil)
	})
	_ = client.On("message", func(args ...any) { messages <- args[0].(types.BufferInterface) })

	select {
	case <-opened:
		if client.Transport().Name() != serverTransports.WEBTRANSPORT {
			t.Fatalf("direct transport = %q", client.Transport().Name())
		}
	case <-time.After(4 * time.Second):
		t.Fatal("direct WebTransport client did not open")
	}
	select {
	case socket := <-connected:
		if socket.Transport().Name() != serverTransports.WEBTRANSPORT {
			t.Fatalf("server transport = %q", socket.Transport().Name())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not emit direct WebTransport connection")
	}
	want := [][]byte{[]byte("webtransport-text"), {0, 1, 2, 127, 128, 255}}
	for index, expected := range want {
		select {
		case actual := <-messages:
			if !bytes.Equal(actual.Bytes(), expected) {
				t.Fatalf("message %d = %v, want %v", index, actual.Bytes(), expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing WebTransport message %d", index)
		}
	}
	select {
	case <-heartbeat:
	case <-time.After(2 * time.Second):
		t.Fatal("WebTransport client did not answer Engine.IO ping")
	}
}

func TestOfficialClientWebTransportCloseBothDirections(t *testing.T) {
	t.Run("server", func(t *testing.T) {
		harness := newOfficialClientWebTransportHarness(t, nil)
		connected := make(chan engineServer.Socket, 1)
		_ = harness.engine.On("connection", func(args ...any) { connected <- args[0].(engineServer.Socket) })
		client := NewSocket(harness.httpURL, harness.clientOptions(&WebTransportBuilder{}))
		t.Cleanup(func() { client.Close() })
		closed := make(chan string, 1)
		_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })
		select {
		case socket := <-connected:
			socket.Close(true)
		case <-time.After(4 * time.Second):
			t.Fatal("WebTransport client did not connect")
		}
		select {
		case reason := <-closed:
			if reason != "transport close" {
				t.Fatalf("client close reason = %q", reason)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("server close did not reach WebTransport client")
		}
	})

	t.Run("client", func(t *testing.T) {
		harness := newOfficialClientWebTransportHarness(t, nil)
		serverClosed := make(chan string, 1)
		_ = harness.engine.On("connection", func(args ...any) {
			_ = args[0].(engineServer.Socket).Once("close", func(closeArgs ...any) {
				serverClosed <- closeArgs[0].(string)
			})
		})
		client := NewSocket(harness.httpURL, harness.clientOptions(&WebTransportBuilder{}))
		opened := make(chan struct{}, 1)
		_ = client.Once("open", func(...any) { opened <- struct{}{} })
		select {
		case <-opened:
			client.Close()
		case <-time.After(4 * time.Second):
			t.Fatal("WebTransport client did not open")
		}
		select {
		case <-serverClosed:
		case <-time.After(3 * time.Second):
			t.Fatal("client close did not reach WebTransport server")
		}
	})
}

func TestOfficialClientPollingToWebTransportUpgrade(t *testing.T) {
	harness := newOfficialClientWebTransportHarness(t, nil)
	serverUpgraded := make(chan string, 1)
	_ = harness.engine.On("connection", func(args ...any) {
		socket := args[0].(engineServer.Socket)
		_ = socket.Once("upgrade", func(upgradeArgs ...any) {
			serverUpgraded <- upgradeArgs[0].(serverTransports.Transport).Name()
			socket.Send(types.NewStringBufferString("server-after-webtransport-upgrade"), nil, nil)
		})
		_ = socket.On("message", func(messageArgs ...any) {
			socket.Send(messageArgs[0].(types.BufferInterface).Clone(), nil, nil)
		})
	})

	client := NewSocket(
		harness.httpURL,
		harness.clientOptions(&PollingBuilder{}, &WebTransportBuilder{}),
	)
	t.Cleanup(func() { client.Close() })
	clientUpgraded := make(chan string, 1)
	messages := make(chan string, 2)
	_ = client.Once("upgrade", func(args ...any) {
		clientUpgraded <- args[0].(Transport).Name()
		client.Send(types.NewStringBufferString("client-after-webtransport-upgrade"), nil, nil)
	})
	_ = client.On("message", func(args ...any) { messages <- args[0].(types.BufferInterface).String() })

	for name, channel := range map[string]<-chan string{
		"client": clientUpgraded,
		"server": serverUpgraded,
	} {
		select {
		case got := <-channel:
			if got != serverTransports.WEBTRANSPORT {
				t.Fatalf("%s upgrade transport = %q", name, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not observe WebTransport upgrade", name)
		}
	}
	gotMessages := map[string]bool{}
	for range 2 {
		select {
		case message := <-messages:
			gotMessages[message] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("post-upgrade messages = %#v", gotMessages)
		}
	}
	if !gotMessages["client-after-webtransport-upgrade"] || !gotMessages["server-after-webtransport-upgrade"] {
		t.Fatalf("post-upgrade messages = %#v", gotMessages)
	}
}

func TestOfficialClientFavorsWebTransportOverWebSocket(t *testing.T) {
	harness := newOfficialClientWebTransportHarness(t, nil)
	serverUpgrades := make(chan string, 2)
	_ = harness.engine.On("connection", func(args ...any) {
		_ = args[0].(engineServer.Socket).On("upgrade", func(upgradeArgs ...any) {
			serverUpgrades <- upgradeArgs[0].(serverTransports.Transport).Name()
		})
	})
	client := NewSocket(
		harness.httpURL,
		harness.clientOptions(&PollingBuilder{}, &WebSocketBuilder{}, &WebTransportBuilder{}),
	)
	t.Cleanup(func() { client.Close() })
	clientUpgrades := make(chan string, 2)
	_ = client.On("upgrade", func(args ...any) {
		clientUpgrades <- args[0].(Transport).Name()
	})

	for name, channel := range map[string]<-chan string{
		"client": clientUpgrades,
		"server": serverUpgrades,
	} {
		select {
		case got := <-channel:
			if got != serverTransports.WEBTRANSPORT {
				t.Fatalf("%s selected %q, want webtransport", name, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not observe transport upgrade", name)
		}
	}
	time.Sleep(DefaultWebTransportUpgradeDelay + 100*time.Millisecond)
	if client.Transport().Name() != serverTransports.WEBTRANSPORT {
		t.Fatalf("final client transport = %q", client.Transport().Name())
	}
	select {
	case got := <-clientUpgrades:
		t.Fatalf("client upgraded a second time to %q", got)
	default:
	}
}
