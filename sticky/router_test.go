package sticky

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func targetURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	target, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestRouterRejectsInvalidConfiguration(t *testing.T) {
	if _, err := New(Options{LoadBalancingMethod: "unknown"}); err == nil {
		t.Fatal("expected invalid method error")
	}
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("", targetURL(t, "http://localhost")); err == nil {
		t.Fatal("expected empty backend ID error")
	}
	if err := router.AddBackend("one", &url.URL{Scheme: "file", Path: "/tmp"}); err == nil {
		t.Fatal("expected invalid target error")
	}
}

func TestRouterReturnsServiceUnavailableWithoutBackends(t *testing.T) {
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/socket.io/?EIO=4&transport=polling", nil))
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q", recorder.Code, recorder.Header().Get("Retry-After"))
	}
	webSocketRequest := httptest.NewRequest(http.MethodGet, "/socket.io/?EIO=4&transport=websocket", nil)
	webSocketRequest.Header.Set("Connection", "Upgrade")
	webSocketRequest.Header.Set("Upgrade", "websocket")
	webSocketRecorder := httptest.NewRecorder()
	router.ServeHTTP(webSocketRecorder, webSocketRequest)
	if webSocketRecorder.Code != http.StatusServiceUnavailable || webSocketRecorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("WebSocket status=%d retry-after=%q", webSocketRecorder.Code, webSocketRecorder.Header().Get("Retry-After"))
	}
}

func TestRoundRobinAndAutomaticSIDAffinity(t *testing.T) {
	makeBackend := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("X-Origin", id)
			if request.URL.Query().Get("sid") == "" {
				_, _ = io.WriteString(w, fmt.Sprintf(`0{"sid":"sid-%s","upgrades":["websocket"]}`, id))
				return
			}
			_, _ = io.WriteString(w, id)
		}))
	}
	one := makeBackend("one")
	two := makeBackend("two")
	defer one.Close()
	defer two.Close()

	router, err := New(Options{LoadBalancingMethod: RoundRobin, DebugHeader: "X-Selected"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("one", targetURL(t, one.URL)); err != nil {
		t.Fatal(err)
	}
	if err := router.AddBackend("two", targetURL(t, two.URL)); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(router)
	defer proxy.Close()

	for _, expected := range []string{"one", "two"} {
		response, err := http.Get(proxy.URL + "/socket.io/?EIO=4&transport=polling")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.Header.Get("X-Selected") != expected || !strings.Contains(string(body), "sid-"+expected) {
			t.Fatalf("selected=%q body=%q, want %q", response.Header.Get("X-Selected"), body, expected)
		}

		response, err = http.Get(proxy.URL + "/socket.io/?EIO=4&transport=polling&sid=sid-" + expected)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		_ = response.Body.Close()
		if string(body) != expected {
			t.Fatalf("sticky response=%q, want %q", body, expected)
		}
	}
}

func TestRandomRoutingAndCORSPassthrough(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if origin := request.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	router, err := New(Options{LoadBalancingMethod: Random})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if addErr := router.AddBackend("random", targetURL(t, backend.URL)); addErr != nil {
		t.Fatal(addErr)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	request, err := http.NewRequest(http.MethodOptions, server.URL+"/socket.io/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "https://example.com")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.Header.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("status=%d allow-origin=%q", response.StatusCode, response.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestSIDAffinityAcrossIndependentHTTPConnections(t *testing.T) {
	makeBackend := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("X-Origin", id)
			if request.URL.Query().Get("sid") == "" {
				_, _ = io.WriteString(w, fmt.Sprintf(`0{"sid":"sid-%s","upgrades":[]}`, id))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
	}
	one := makeBackend("one")
	two := makeBackend("two")
	defer one.Close()
	defer two.Close()
	router, err := New(Options{LoadBalancingMethod: RoundRobin})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if addErr := router.AddBackend("one", targetURL(t, one.URL)); addErr != nil {
		t.Fatal(addErr)
	}
	if addErr := router.AddBackend("two", targetURL(t, two.URL)); addErr != nil {
		t.Fatal(addErr)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	firstTransport := &http.Transport{DisableKeepAlives: false}
	secondTransport := &http.Transport{DisableKeepAlives: false}
	defer firstTransport.CloseIdleConnections()
	defer secondTransport.CloseIdleConnections()
	firstClient := &http.Client{Transport: firstTransport}
	secondClient := &http.Client{Transport: secondTransport}
	response, err := firstClient.Get(server.URL + "/socket.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	origin := response.Header.Get("X-Origin")
	var open struct {
		SID string `json:"sid"`
	}
	if decodeErr := json.Unmarshal(body[1:], &open); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/socket.io/?EIO=4&transport=polling&sid="+open.SID, strings.NewReader("40"))
	if err != nil {
		t.Fatal(err)
	}
	response, err = secondClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.Header.Get("X-Origin") != origin {
		t.Fatalf("second TCP connection reached %q, want session owner %q", response.Header.Get("X-Origin"), origin)
	}
}

func TestLargeContentLengthAndChunkedUploads(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Actual-Length", fmt.Sprint(len(body)))
		w.Header().Set("X-Content-Length", request.Header.Get("Content-Length"))
		w.Header().Set("X-Checksum", fmt.Sprintf("%x", sha256.Sum256(body)))
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("upload", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	for _, size := range []int{10_000, 1_000_000} {
		payload := bytes.Repeat([]byte{byte(size % 251)}, size)
		for _, chunked := range []bool{false, true} {
			name := fmt.Sprintf("size_%d/chunked_%t", size, chunked)
			t.Run(name, func(t *testing.T) {
				request, requestErr := http.NewRequest(http.MethodPost, server.URL+"/upload", bytes.NewReader(payload))
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				if chunked {
					request.ContentLength = -1
					request.TransferEncoding = []string{"chunked"}
				}
				response, requestErr := http.DefaultClient.Do(request)
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK || response.Header.Get("X-Actual-Length") != fmt.Sprint(size) ||
					response.Header.Get("X-Checksum") != fmt.Sprintf("%x", sha256.Sum256(payload)) {
					t.Fatalf("upload response status=%d headers=%v", response.StatusCode, response.Header)
				}
				if chunked && response.Header.Get("X-Content-Length") != "" {
					t.Fatalf("chunked upload forwarded Content-Length %q", response.Header.Get("X-Content-Length"))
				}
				if !chunked && response.Header.Get("X-Content-Length") != fmt.Sprint(size) {
					t.Fatalf("Content-Length = %q, want %d", response.Header.Get("X-Content-Length"), size)
				}
			})
		}
	}
}

func TestLeastConnectionBindingRemovalAndExpiration(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	router, err := New(Options{
		LoadBalancingMethod: LeastConnection,
		SessionTTL:          20 * time.Millisecond,
		SweepInterval:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if addErr := router.AddBackend("one", targetURL(t, backend.URL)); addErr != nil {
		t.Fatal(addErr)
	}
	if err := router.AddBackend("two", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	if !router.Bind("a", "one") || !router.Bind("b", "one") || !router.Bind("c", "two") {
		t.Fatal("bind failed")
	}
	stats := router.Stats()
	if stats[0].Sessions != 2 || stats[1].Sessions != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if selected := router.selectBackend(""); selected.id != "two" {
		t.Fatalf("selected %q, want two", selected.id)
	}
	router.RemoveBackend("one")
	if _, ok := router.BackendForSession("a"); ok {
		t.Fatal("removed backend retained its mapping")
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := router.BackendForSession("c"); ok {
		t.Fatal("expired mapping was not removed")
	}
}

func TestBindIsIdempotentAndCanMoveSession(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	_ = router.AddBackend("one", targetURL(t, backend.URL))
	_ = router.AddBackend("two", targetURL(t, backend.URL))
	router.Bind("sid", "one")
	router.Bind("sid", "one")
	router.Bind("sid", "two")
	stats := router.Stats()
	if stats[0].Sessions != 0 || stats[1].Sessions != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	router.Unbind("sid")
	if router.Stats()[1].Sessions != 0 {
		t.Fatal("session count was not decremented")
	}
}

func TestEngineClosePayloadDetection(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "EIO4 close", payload: "1", want: true},
		{name: "EIO4 payload", payload: "4hello\x1e1", want: true},
		{name: "EIO3 close", payload: "1:1", want: true},
		{name: "EIO3 payload", payload: "6:4hello1:1", want: true},
		{name: "Socket.IO disconnect", payload: "41", want: false},
		{name: "message containing one", payload: "4[1]", want: false},
		{name: "truncated EIO3 payload", payload: "5:1", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isEngineClosePayload([]byte(test.payload)); got != test.want {
				t.Fatalf("isEngineClosePayload(%q) = %t, want %t", test.payload, got, test.want)
			}
		})
	}
}

func TestPollingCloseRequestAndResponseUnbindSessions(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, request.Body)
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, "1")
	}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("one", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	for _, test := range []struct {
		name   string
		method string
		body   string
	}{
		{name: "client EIO4 close", method: http.MethodPost, body: "1"},
		{name: "client EIO3 close", method: http.MethodPost, body: "1:1"},
		{name: "server close", method: http.MethodGet},
	} {
		t.Run(test.name, func(t *testing.T) {
			sid := strings.ReplaceAll(test.name, " ", "-")
			if !router.Bind(sid, "one") {
				t.Fatal("Bind returned false")
			}
			request, requestErr := http.NewRequest(test.method, server.URL+"/?sid="+sid, strings.NewReader(test.body))
			if requestErr != nil {
				t.Fatal(requestErr)
			}
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr != nil {
				t.Fatal(requestErr)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if _, ok := router.BackendForSession(sid); ok {
				t.Fatal("closed polling session remains bound")
			}
		})
	}
}

func TestFailedWebSocketUpgradeKeepsPollingSession(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upgrade rejected", http.StatusBadRequest)
	}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("one", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	if !router.Bind("polling", "one") {
		t.Fatal("Bind returned false")
	}
	request := httptest.NewRequest(http.MethodGet, "/socket.io/?sid=polling&transport=websocket", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	router.ServeHTTP(httptest.NewRecorder(), request)
	if _, ok := router.BackendForSession("polling"); !ok {
		t.Fatal("failed WebSocket upgrade removed the active polling session")
	}
}

func TestSuccessfulWebSocketUpgradeUnbindsWhenTunnelCloses(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connection, readWriter, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = readWriter.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = readWriter.Flush()
		_ = connection.Close()
	}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if addErr := router.AddBackend("one", targetURL(t, backend.URL)); addErr != nil {
		t.Fatal(addErr)
	}
	if !router.Bind("upgrade", "one") {
		t.Fatal("Bind returned false")
	}
	server := httptest.NewServer(router)
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("tcp", serverURL.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	_, _ = fmt.Fprintf(connection, "GET /socket.io/?EIO=4&transport=websocket&sid=upgrade HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", serverURL.Host)
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("upgrade status = %q, want 101", strings.TrimSpace(status))
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil || line == "\r\n" {
			break
		}
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = reader.ReadByte()
	_ = connection.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := router.BackendForSession("upgrade"); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("closed WebSocket tunnel retained polling session mapping")
}

func TestCloseReleasesSessionAccounting(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddBackend("one", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	router.Bind("session", "one")
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := router.BackendForSession("session"); ok {
		t.Fatal("closed router retained session mapping")
	}
	if stats := router.Stats(); len(stats) != 1 || stats[0].Sessions != 0 {
		t.Fatalf("stats after Close = %#v, want zero sessions", stats)
	}
}

func TestConcurrentRoutingAndClose(t *testing.T) {
	var requests atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddBackend("one", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	done := make(chan struct{})
	for range 20 {
		go func() {
			defer func() { done <- struct{}{} }()
			response, requestErr := http.Get(server.URL)
			if requestErr == nil {
				_ = response.Body.Close()
			}
		}()
	}
	for range 20 {
		<-done
	}
	if requests.Load() != 20 {
		t.Fatalf("requests=%d, want 20", requests.Load())
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", recorder.Code)
	}
}

func TestWebSocketUpgradeDetectionAndAccounting(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/socket.io/?EIO=4&transport=websocket", nil)
	request.Header.Set("Connection", "keep-alive, Upgrade")
	request.Header.Set("Upgrade", "websocket")
	if !isWebSocketUpgrade(request) {
		t.Fatal("valid WebSocket upgrade was not detected")
	}
	request.Header.Set("Connection", "keep-alive")
	if isWebSocketUpgrade(request) {
		t.Fatal("request without Connection: upgrade was accepted")
	}

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	router, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.AddBackend("one", targetURL(t, backend.URL)); err != nil {
		t.Fatal(err)
	}
	router.changeWebSocketCount("one", 1)
	if got := router.Stats()[0].Sessions; got != 1 {
		t.Fatalf("active sessions=%d, want 1", got)
	}
	router.changeWebSocketCount("one", -1)
	if got := router.Stats()[0].Sessions; got != 0 {
		t.Fatalf("active sessions=%d, want 0", got)
	}
}
