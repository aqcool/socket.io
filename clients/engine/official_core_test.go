package engine

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/parsers/engine/v4/packet"
	"github.com/aqcool/socket.io/servers/engine/v4/transports"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

type officialNoopBuilder struct {
	name string
}

func (b *officialNoopBuilder) Name() string { return b.name }

func (b *officialNoopBuilder) New(_ Socket, opts SocketOptionsInterface) Transport {
	base := MakeTransport()
	transport := &officialNoopTransport{Transport: base, name: b.name}
	base.Prototype(transport)
	base.Construct(nil, opts)
	return transport
}

type officialNoopTransport struct {
	Transport
	name string
}

func (t *officialNoopTransport) Name() string { return t.name }

type officialCountingBuilder struct {
	name  string
	build atomic.Int32
}

func (b *officialCountingBuilder) Name() string { return b.name }

func (b *officialCountingBuilder) New(socket Socket, opts SocketOptionsInterface) Transport {
	b.build.Add(1)
	return (&officialNoopBuilder{name: b.name}).New(socket, opts)
}

func newOfficialOptions(builders ...TransportCtor) *SocketOptions {
	opts := DefaultSocketOptions()
	opts.SetTransportList(builders)
	return opts
}

func TestOfficialClientURIAndHostParsing(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		host     string
		secure   bool
		setPort  string
		port     string
		hostname string
	}{
		{name: "http default port", uri: "http://localhost", port: "80", hostname: "localhost"},
		{name: "https default port", uri: "https://localhost", secure: true, port: "443", hostname: "localhost"},
		{name: "wss default port", uri: "wss://localhost", secure: true, port: "443", hostname: "localhost"},
		{name: "wss explicit port", uri: "wss://localhost:2020", secure: true, port: "2020", hostname: "localhost"},
		{name: "scheme-less host and port", uri: "localhost:8080", port: "8080", hostname: "localhost"},
		{name: "IPv6 URI", uri: "http://[::1]", port: "80", hostname: "::1"},
		{name: "IPv6 URI and port", uri: "http://[::1]:8080", port: "8080", hostname: "::1"},
		{name: "host option", host: "localhost", port: "80", hostname: "localhost"},
		{name: "host option and port", host: "localhost", setPort: "8080", port: "8080", hostname: "localhost"},
		{name: "bracketed IPv6 host", host: "[::1]", port: "80", hostname: "::1"},
		{name: "bracketed IPv6 host and port", host: "[::1]", setPort: "8080", port: "8080", hostname: "::1"},
		{name: "unbracketed IPv6 host", host: "::1", port: "80", hostname: "::1"},
		{name: "secure IPv6 host", host: "[::1]", secure: true, port: "443", hostname: "::1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := newOfficialOptions(&officialNoopBuilder{name: "noop"})
			if test.host != "" {
				opts.SetHost(test.host)
			}
			if test.secure {
				opts.SetSecure(true)
			}
			if test.setPort != "" {
				opts.SetPort(test.setPort)
			}
			client := NewSocket(test.uri, opts)
			t.Cleanup(func() { client.Close() })
			if got := client.Opts().Hostname(); got != test.hostname {
				t.Fatalf("hostname = %q, want %q", got, test.hostname)
			}
			if got := client.Opts().Port(); got != test.port {
				t.Fatalf("port = %q, want %q", got, test.port)
			}
			if got := client.Opts().Secure(); got != test.secure {
				t.Fatalf("secure = %v, want %v", got, test.secure)
			}
		})
	}
}

func TestOfficialClientRandomString(t *testing.T) {
	seen := make(map[string]bool, 100)
	for range 100 {
		value := randomString()
		if len(value) != 8 {
			t.Fatalf("random string length = %d, want 8 (%q)", len(value), value)
		}
		if seen[value] {
			t.Fatalf("duplicate random string %q", value)
		}
		seen[value] = true
	}
}

func TestOfficialClientTransportQuerySnapshotsAreIsolated(t *testing.T) {
	opts := DefaultSocketOptions()
	opts.SetQuery(url.Values{"token": {"original"}})
	parent := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	parent.id.Store("initial-session")
	transport := NewTransport(parent, opts)

	snapshot := transport.Query()
	snapshot.Set("token", "mutated")
	if got := transport.Query().Get("token"); got != "original" {
		t.Fatalf("transport query was mutated through a snapshot: %q", got)
	}

	if got := transport.Query().Get("sid"); got != "initial-session" {
		t.Fatalf("transport query session = %q", got)
	}

	var wait sync.WaitGroup
	for index := range 100 {
		wait.Add(2)
		go func() {
			defer wait.Done()
			parent.id.Store(fmt.Sprintf("session-%d", index))
		}()
		go func() {
			defer wait.Done()
			_ = transport.Query().Encode()
		}()
	}
	wait.Wait()
	if transport.Query().Get("sid") == "" {
		t.Fatal("transport query did not derive the socket session id")
	}
}

func TestOfficialClientPathAndQueryParsing(t *testing.T) {
	opts := newOfficialOptions(&officialNoopBuilder{name: "noop"})
	opts.SetAddTrailingSlash(false)
	client := NewSocket("https://example.test:8443/ignored?token=a%20b&token=c", opts)
	t.Cleanup(func() { client.Close() })

	if got := client.Opts().Path(); got != "/engine.io" {
		t.Fatalf("path = %q, want /engine.io", got)
	}
	if got := client.Opts().Query()["token"]; len(got) != 2 || got[0] != "a b" || got[1] != "c" {
		t.Fatalf("query token = %#v", got)
	}
	if client.Opts().Hostname() != "example.test" || client.Opts().Port() != "8443" {
		t.Fatalf("endpoint = %s:%s", client.Opts().Hostname(), client.Opts().Port())
	}
}

func TestOfficialClientOrderedTransportsAndCallerList(t *testing.T) {
	first := &officialNoopBuilder{name: transports.WEBSOCKET}
	second := &officialNoopBuilder{name: transports.POLLING}
	ordered := []TransportCtor{first, second}
	opts := newOfficialOptions(ordered...)
	client := NewSocket("http://localhost", opts)
	t.Cleanup(func() { client.Close() })

	if got := client.Transport().Name(); got != transports.WEBSOCKET {
		t.Fatalf("initial transport = %q, want websocket", got)
	}
	if ordered[0] != first || ordered[1] != second {
		t.Fatal("constructor mutated the caller transport list")
	}
	copyOfList := opts.TransportList()
	copyOfList[0] = second
	if opts.TransportList()[0] != first {
		t.Fatal("TransportList returned mutable option storage")
	}
}

func TestOfficialClientLegacyTransportSetIsDeterministic(t *testing.T) {
	for range 20 {
		opts := DefaultSocketOptions()
		opts.SetTransports(types.NewSet[TransportCtor](&WebTransportBuilder{}, &WebSocketBuilder{}, &PollingBuilder{}))
		client := NewSocket("http://127.0.0.1:1", opts)
		if got := client.Transport().Name(); got != transports.POLLING {
			client.Close()
			t.Fatalf("initial transport = %q, want deterministic polling", got)
		}
		client.Close()
	}
}

func TestOfficialClientTryAllTransports(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		opts := newOfficialOptions(
			&officialNoopBuilder{name: "first"},
			&officialNoopBuilder{name: "second"},
		)
		opts.SetTryAllTransports(true)
		client := NewSocket("http://localhost", opts)
		t.Cleanup(func() { client.Close() })
		if client.Transport().Name() != "first" {
			t.Fatalf("initial transport = %q", client.Transport().Name())
		}

		concrete := client.(*socket).SocketWithUpgrade.(*socketWithUpgrade).SocketWithoutUpgrade.(*socketWithoutUpgrade)
		concrete._onError(errors.New("first transport failed"))
		if got := client.Transport().Name(); got != "second" {
			t.Fatalf("fallback transport = %q, want second", got)
		}
		if client.ReadyState() != SocketStateOpening {
			t.Fatalf("state after fallback = %q", client.ReadyState())
		}
	})

	t.Run("disabled", func(t *testing.T) {
		opts := newOfficialOptions(
			&officialNoopBuilder{name: "first"},
			&officialNoopBuilder{name: "second"},
		)
		client := NewSocket("http://localhost", opts)
		closed := make(chan string, 1)
		_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })
		concrete := client.(*socket).SocketWithUpgrade.(*socketWithUpgrade).SocketWithoutUpgrade.(*socketWithoutUpgrade)
		concrete._onError(errors.New("first transport failed"))
		if got := <-closed; got != "transport error" {
			t.Fatalf("close reason = %q", got)
		}
		if client.Transport().Name() != "first" || client.ReadyState() != SocketStateClosed {
			t.Fatalf("unexpected fallback: transport=%q state=%q", client.Transport().Name(), client.ReadyState())
		}
	})

	for _, transportName := range []string{transports.POLLING, transports.WEBSOCKET} {
		t.Run("same-name "+transportName, func(t *testing.T) {
			first := &officialCountingBuilder{name: transportName}
			second := &officialCountingBuilder{name: transportName}
			opts := newOfficialOptions(first, second)
			opts.SetTryAllTransports(true)
			client := NewSocket("http://localhost", opts)
			t.Cleanup(func() { client.Close() })
			if first.build.Load() != 1 || second.build.Load() != 0 {
				t.Fatalf("initial constructors = %d/%d, want 1/0", first.build.Load(), second.build.Load())
			}

			concrete := client.(*socket).SocketWithUpgrade.(*socketWithUpgrade).SocketWithoutUpgrade.(*socketWithoutUpgrade)
			concrete._onError(errors.New("first implementation failed"))
			if first.build.Load() != 1 || second.build.Load() != 1 {
				t.Fatalf("fallback constructors = %d/%d, want 1/1", first.build.Load(), second.build.Load())
			}
		})
	}
}

func TestOfficialClientNoTransportsAvailable(t *testing.T) {
	opts := newOfficialOptions()
	client := NewSocket("http://localhost", opts)
	errorsSeen := make(chan error, 1)
	_ = client.Once("error", func(args ...any) { errorsSeen <- args[0].(error) })
	select {
	case err := <-errorsSeen:
		if err.Error() != "No transports available" {
			t.Fatalf("error = %q", err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing no-transports error")
	}
}

func TestOfficialClientRememberUpgradeAcrossInstances(t *testing.T) {
	sharedPriorWebsocketSuccess.Store(false)
	t.Cleanup(func() { sharedPriorWebsocketSuccess.Store(false) })

	first := MakeSocketWithoutUpgrade()
	first.SetPriorWebsocketSuccess(true)
	if !MakeSocketWithoutUpgrade().PriorWebsocketSuccess() {
		t.Fatal("websocket success was not shared across socket instances")
	}

	opts := newOfficialOptions(
		&officialNoopBuilder{name: transports.POLLING},
		&officialNoopBuilder{name: transports.WEBSOCKET},
	)
	opts.SetRememberUpgrade(true)
	client := NewSocket("https://localhost", opts)
	t.Cleanup(func() { client.Close() })
	if got := client.Transport().Name(); got != transports.WEBSOCKET {
		t.Fatalf("remembered initial transport = %q, want websocket", got)
	}

	concrete := client.(*socket).SocketWithUpgrade.(*socketWithUpgrade).SocketWithoutUpgrade.(*socketWithoutUpgrade)
	concrete._onError(errors.New("websocket failed"))
	if MakeSocketWithoutUpgrade().PriorWebsocketSuccess() {
		t.Fatal("transport error did not clear remembered websocket success")
	}
}

func TestOfficialClientRememberUpgradeDisabledUsesFirstTransport(t *testing.T) {
	sharedPriorWebsocketSuccess.Store(true)
	t.Cleanup(func() { sharedPriorWebsocketSuccess.Store(false) })
	opts := newOfficialOptions(
		&officialNoopBuilder{name: transports.POLLING},
		&officialNoopBuilder{name: transports.WEBSOCKET},
	)
	opts.SetRememberUpgrade(false)
	client := NewSocket("https://localhost", opts)
	t.Cleanup(func() { client.Close() })
	if got := client.Transport().Name(); got != transports.POLLING {
		t.Fatalf("initial transport = %q, want configured first transport", got)
	}
}

func TestOfficialClientFilterUpgrades(t *testing.T) {
	opts := newOfficialOptions(
		&officialNoopBuilder{name: transports.POLLING},
		&officialNoopBuilder{name: transports.WEBSOCKET},
	)
	client := NewSocket("http://localhost", opts)
	t.Cleanup(func() { client.Close() })
	concrete := client.(*socket).SocketWithUpgrade.(*socketWithUpgrade)
	filtered := concrete._filterUpgrades([]string{transports.WEBTRANSPORT, transports.WEBSOCKET})
	if filtered.Len() != 1 || !filtered.Has(transports.WEBSOCKET) {
		t.Fatalf("filtered upgrades = %#v", filtered.Keys())
	}
}

func TestOfficialClientTransportURIs(t *testing.T) {
	tests := []struct {
		name     string
		secure   bool
		hostname string
		port     string
		schema   string
		want     string
	}{
		{name: "http default", hostname: "localhost", port: "80", schema: "http", want: "http://localhost/engine.io?sid=test"},
		{name: "http explicit", hostname: "localhost", port: "3000", schema: "http", want: "http://localhost:3000/engine.io?sid=test"},
		{name: "https default", secure: true, hostname: "localhost", port: "443", schema: "https", want: "https://localhost/engine.io?sid=test"},
		{name: "https explicit", secure: true, hostname: "localhost", port: "8443", schema: "https", want: "https://localhost:8443/engine.io?sid=test"},
		{name: "IPv6 default", hostname: "::1", port: "80", schema: "http", want: "http://[::1]/engine.io?sid=test"},
		{name: "IPv6 explicit", hostname: "::1", port: "8080", schema: "http", want: "http://[::1]:8080/engine.io?sid=test"},
		{name: "ws default", hostname: "test", port: "80", schema: "ws", want: "ws://test/engine.io?sid=test"},
		{name: "wss default", secure: true, hostname: "test", port: "443", schema: "wss", want: "wss://test/engine.io?sid=test"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultSocketOptions()
			opts.SetHostname(test.hostname)
			opts.SetPort(test.port)
			opts.SetSecure(test.secure)
			opts.SetPath("/engine.io")
			base := MakeTransport()
			base.Construct(nil, opts)
			if got := base.CreateUri(test.schema, url.Values{"sid": {"test"}}).String(); got != test.want {
				t.Fatalf("URI = %q, want %q", got, test.want)
			}
		})
	}
}

func TestOfficialClientTimestampAndBase64TransportURIs(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	opts := DefaultSocketOptions()
	opts.SetHostname("localhost")
	opts.SetPort("80")
	opts.SetPath("/engine.io")
	opts.SetTimestampRequests(true)
	opts.SetTimestampParam("stamp")
	opts.SetForceBase64(true)
	opts.SetQuery(url.Values{"transport": {transports.POLLING}})
	polling := NewPolling(client, opts).(*polling)
	parsed := polling.uri()
	if parsed.Query().Get("stamp") == "" || parsed.Query().Get("b64") != "1" {
		t.Fatalf("polling query = %s", parsed.RawQuery)
	}

	opts.SetQuery(url.Values{"transport": {transports.WEBSOCKET}})
	websocket := NewWebSocket(client, opts).(*websocket)
	parsed = websocket.uri()
	if parsed.Scheme != "ws" || parsed.Query().Get("stamp") == "" || parsed.Query().Get("b64") != "1" {
		t.Fatalf("websocket URI = %s", parsed)
	}
}

func TestOfficialClientMaxPayloadBatching(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	client.readyState.Store(SocketStateOpen)
	client._maxPayload.Store(100)
	transport := newControlledDrainTransport(transports.POLLING)
	client.SetTransport(transport)

	for _, value := range []string{strings.Repeat("a", 30), strings.Repeat("b", 30), strings.Repeat("c", 35)} {
		client.writeBuffer.Push(&packet.Packet{Type: packet.MESSAGE, Data: strings.NewReader(value)})
	}
	if got := len(client._getWritablePackets()); got != 3 {
		t.Fatalf("exact maxPayload batch contains %d packets, want 3", got)
	}
	client.writeBuffer.Push(&packet.Packet{Type: packet.MESSAGE, Data: strings.NewReader("overflow")})
	if got := len(client._getWritablePackets()); got != 3 {
		t.Fatalf("overflow batch contains %d packets, want first 3", got)
	}

	client.writeBuffer.Clear()
	client.writeBuffer.Push(&packet.Packet{Type: packet.MESSAGE, Data: strings.NewReader(strings.Repeat("x", 101))})
	client.writeBuffer.Push(&packet.Packet{Type: packet.MESSAGE, Data: strings.NewReader("b")})
	if got := len(client._getWritablePackets()); got != 1 {
		t.Fatalf("oversized first-packet batch contains %d packets, want 1", got)
	}
}

func TestOfficialClientNoPacketsAfterCloseBegins(t *testing.T) {
	client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
	client.readyState.Store(SocketStateClosing)
	created := make(chan struct{}, 1)
	_ = client.On("packetCreate", func(...any) { created <- struct{}{} })
	client.Send(strings.NewReader("ignored"), nil, nil)
	if client.WriteBuffer().Len() != 0 {
		t.Fatal("packet was buffered while socket was closing")
	}
	select {
	case <-created:
		t.Fatal("packetCreate was emitted while socket was closing")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestOfficialClientCloseWaitsForUpgradeOutcome(t *testing.T) {
	for _, outcome := range []string{"upgrade", "upgradeError"} {
		t.Run(outcome, func(t *testing.T) {
			client := MakeSocketWithoutUpgrade().(*socketWithoutUpgrade)
			client.opts = DefaultSocketOptions()
			client.readyState.Store(SocketStateOpen)
			transport := newControlledDrainTransport(transports.POLLING)
			client.SetTransport(transport)
			client.SetUpgrading(true)
			closed := make(chan string, 1)
			_ = client.Once("close", func(args ...any) { closed <- args[0].(string) })

			client.Close()
			select {
			case reason := <-closed:
				t.Fatalf("closed before upgrade outcome: %q", reason)
			default:
			}
			client.Emit(types.EventName(outcome), transport)
			select {
			case reason := <-closed:
				if reason != "forced close" {
					t.Fatalf("close reason = %q", reason)
				}
			case <-time.After(time.Second):
				t.Fatal("close did not resume after upgrade outcome")
			}
		})
	}
}
