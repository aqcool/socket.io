package engine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"
)

func TestOfficialClientPollingExplicitProxyAndObserver(t *testing.T) {
	requests := make(chan *http.Request, 2)
	var first sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.Clone(request.Context())
		initial := false
		first.Do(func() { initial = true })
		if initial {
			_, _ = writer.Write([]byte(officialClientHandshake))
		} else {
			_, _ = writer.Write([]byte("1"))
		}
	}))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan NetworkEvent, 4)
	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
	opts.SetUpgrade(false)
	opts.SetProxyURL(proxyURL)
	opts.SetNetworkObserver(func(event NetworkEvent) { events <- event })
	client := NewSocket("http://engine.invalid", opts)
	t.Cleanup(func() { client.Close() })
	opened := make(chan struct{}, 1)
	_ = client.Once("open", func(...any) { opened <- struct{}{} })
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not connect through explicit proxy")
	}
	select {
	case request := <-requests:
		if request.URL.Host != "engine.invalid" || request.URL.Query().Get("transport") != "polling" {
			t.Fatalf("proxy request URL = %s", request.URL)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy did not receive request")
	}
	select {
	case event := <-events:
		if !event.Success || event.Transport != "polling" || event.Operation != http.MethodGet || event.Proxy != proxyURL.Redacted() {
			t.Fatalf("network event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("network observer did not receive polling event")
	}
}

func TestOfficialClientTLSConfiguration(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
			connection, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				return
			}
			defer func() { _ = connection.Close() }()
			_ = connection.WriteMessage(gorillaws.TextMessage, []byte(officialClientHandshake))
			_, _, _ = connection.ReadMessage()
			return
		}
		if request.URL.Query().Get("sid") == "" {
			_, _ = writer.Write([]byte(officialClientHandshake))
		} else {
			_, _ = writer.Write([]byte("1"))
		}
	}))
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())

	for _, builder := range []TransportCtor{&PollingBuilder{}, &WebSocketBuilder{}} {
		t.Run(builder.Name(), func(t *testing.T) {
			opts := DefaultSocketOptions()
			opts.SetTransportList([]TransportCtor{builder})
			opts.SetUpgrade(false)
			opts.SetTLSClientConfig(&tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    roots,
			})
			client := NewSocket(server.URL, opts)
			t.Cleanup(func() { client.Close() })
			opened := make(chan struct{}, 1)
			_ = client.Once("open", func(...any) { opened <- struct{}{} })
			select {
			case <-opened:
			case <-time.After(3 * time.Second):
				t.Fatalf("%s client did not open with custom trust roots", builder.Name())
			}
		})
	}
}

type officialRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn officialRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestOfficialClientCustomHTTPComponents(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	configuredClient := &http.Client{Jar: jar}
	requests := make(chan *http.Request, 1)
	roundTripper := officialRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests <- request.Clone(request.Context())
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(officialClientHandshake)),
			Request:    request,
		}, nil
	})
	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&PollingBuilder{}})
	opts.SetUpgrade(false)
	opts.SetHTTPClient(configuredClient)
	opts.SetRoundTripper(roundTripper)
	opts.SetExtraHeaders(http.Header{"X-Test": {"custom-components"}})
	client := NewSocket("http://engine.invalid", opts)
	t.Cleanup(func() { client.Close() })
	select {
	case request := <-requests:
		if request.Header.Get("X-Test") != "custom-components" {
			t.Fatalf("custom RoundTripper header = %q", request.Header.Get("X-Test"))
		}
	case <-time.After(time.Second):
		t.Fatal("custom RoundTripper was not called")
	}
	if configuredClient.Jar != jar {
		t.Fatal("client construction mutated the caller's HTTP cookie jar")
	}
}

func TestOfficialClientCustomWebSocketDialer(t *testing.T) {
	dialed := make(chan struct {
		url     string
		headers http.Header
	}, 1)
	opts := DefaultSocketOptions()
	opts.SetTransportList([]TransportCtor{&WebSocketBuilder{}})
	opts.SetExtraHeaders(http.Header{"X-Test": {"custom-dialer"}})
	opts.SetWebSocketDialer(func(_ context.Context, rawURL string, headers http.Header) (*gorillaws.Conn, *http.Response, error) {
		dialed <- struct {
			url     string
			headers http.Header
		}{rawURL, headers.Clone()}
		return nil, nil, errors.New("dial refused")
	})
	client := NewSocket("http://engine.invalid", opts)
	t.Cleanup(func() { client.Close() })
	seenError := make(chan error, 1)
	_ = client.Once("error", func(args ...any) { seenError <- args[0].(error) })
	select {
	case call := <-dialed:
		if !strings.HasPrefix(call.url, "ws://engine.invalid/engine.io/") || call.headers.Get("X-Test") != "custom-dialer" {
			t.Fatalf("dial call = %q/%v", call.url, call.headers)
		}
	case <-time.After(time.Second):
		t.Fatal("custom WebSocket dialer was not called")
	}
	select {
	case err := <-seenError:
		var transportError *Error
		if !errors.As(err, &transportError) || transportError.Message != "websocket error" {
			t.Fatalf("dial error = %#v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("custom WebSocket dial error was not emitted")
	}
}
