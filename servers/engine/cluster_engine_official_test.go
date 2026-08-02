package engine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/gorilla/websocket"
)

type officialClusterFixture struct {
	engines []ClusterServer
	servers []*httptest.Server
	urls    []string
}

func newOfficialClusterFixture(t *testing.T, pingInterval time.Duration) *officialClusterFixture {
	t.Helper()
	bus := NewMemoryClusterBus()
	fixture := &officialClusterFixture{}
	for index := range 3 {
		options := config.DefaultServerOptions()
		if pingInterval > 0 && index == 2 {
			options.SetPingInterval(pingInterval)
		}
		server, err := NewClusterServer(bus, options, &ClusterOptions{ResponseTimeout: 150 * time.Millisecond})
		if err != nil {
			t.Fatalf("NewClusterServer() error = %v", err)
		}
		fixture.engines = append(fixture.engines, server)
		httpServer := httptest.NewServer(server)
		fixture.servers = append(fixture.servers, httpServer)
		fixture.urls = append(fixture.urls, httpServer.URL)
	}
	t.Cleanup(func() {
		for _, engine := range fixture.engines {
			engine.Close()
		}
		for _, server := range fixture.servers {
			server.CloseClientConnections()
			server.Close()
		}
		_ = bus.Close()
	})
	return fixture
}

func clusterPollingURL(baseURL, sid string) string {
	url := baseURL + "/engine.io/?EIO=4&transport=polling"
	if sid != "" {
		url += "&sid=" + sid
	}
	return url
}

func clusterHandshake(t *testing.T, baseURL string) string {
	t.Helper()
	response, err := http.Get(clusterPollingURL(baseURL, "")) //nolint:gosec,noctx
	if err != nil {
		t.Fatalf("handshake GET error = %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	if response.StatusCode != http.StatusOK || len(body) == 0 || body[0] != '0' {
		t.Fatalf("handshake = status %d body %q", response.StatusCode, body)
	}
	var open struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(body[1:], &open); err != nil {
		t.Fatalf("decode handshake %q: %v", body, err)
	}
	if open.SID == "" {
		t.Fatal("handshake SID is empty")
	}
	if len(open.SID) != 20 {
		t.Fatalf("cluster handshake SID length = %d, want official length 20", len(open.SID))
	}
	return open.SID
}

func clusterRequest(t *testing.T, method, baseURL, sid, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, clusterPollingURL(baseURL, sid), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s error = %v", method, baseURL, err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response.StatusCode, string(payload)
}

func packetReaderString(value any) string {
	reader, _ := value.(io.Reader)
	if reader == nil {
		return ""
	}
	payload, _ := io.ReadAll(reader)
	return string(payload)
}

func waitClusterCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestOfficialClusterEngine010InMemory(t *testing.T) {
	t.Run("should work (read)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			values[0].(Socket).Send(strings.NewReader("hello"), nil, nil)
		})
		sid := clusterHandshake(t, fixture.urls[0])
		status, body := clusterRequest(t, http.MethodGet, fixture.urls[1], sid, "")
		if status != http.StatusOK || body != "4hello" {
			t.Fatalf("remote read = status %d body %q", status, body)
		}
	})

	t.Run("should work (read - deferred)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			socket := values[0].(Socket)
			time.AfterFunc(200*time.Millisecond, func() {
				socket.Send(strings.NewReader("hello"), nil, nil)
			})
		})
		sid := clusterHandshake(t, fixture.urls[0])
		status, body := clusterRequest(t, http.MethodGet, fixture.urls[1], sid, "")
		if status != http.StatusOK || body != "4hello" {
			t.Fatalf("deferred remote read = status %d body %q", status, body)
		}
	})

	t.Run("should work (write)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		received := make(chan string, 1)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			_ = values[0].(Socket).On("message", func(message ...any) {
				received <- packetReaderString(message[0])
			})
		})
		sid := clusterHandshake(t, fixture.urls[0])
		status, _ := clusterRequest(t, http.MethodPost, fixture.urls[1], sid, "4hello")
		if status != http.StatusOK {
			t.Fatalf("remote write status = %d", status)
		}
		select {
		case value := <-received:
			if value != "hello" {
				t.Fatalf("message = %q", value)
			}
		case <-time.After(time.Second):
			t.Fatal("remote message not delivered")
		}
	})

	t.Run("should work (write - multiple)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		received := make(chan string, 6)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			_ = values[0].(Socket).On("message", func(message ...any) {
				received <- packetReaderString(message[0])
			})
		})
		sid := clusterHandshake(t, fixture.urls[0])
		for _, request := range []struct {
			base string
			body string
		}{
			{fixture.urls[1], "41\x1e42\x1e43"},
			{fixture.urls[0], "44\x1e45"},
			{fixture.urls[1], "46"},
		} {
			status, _ := clusterRequest(t, http.MethodPost, request.base, sid, request.body)
			if status != http.StatusOK {
				t.Fatalf("remote multiple write status = %d", status)
			}
		}
		values := make([]string, 0, 6)
		for len(values) < 6 {
			select {
			case value := <-received:
				values = append(values, value)
			case <-time.After(time.Second):
				t.Fatalf("messages = %v", values)
			}
		}
		if got := strings.Join(values, ""); got != "123456" {
			t.Fatalf("messages = %v", values)
		}
	})

	t.Run("should acquire read lock (different process)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		sid := clusterHandshake(t, fixture.urls[0])
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pending, _ := http.NewRequestWithContext(ctx, http.MethodGet, clusterPollingURL(fixture.urls[0], sid), nil)
		go func() { //nolint:bodyclose
			response, err := http.DefaultClient.Do(pending)
			if err == nil {
				_ = response.Body.Close()
			}
		}()
		waitClusterCondition(t, func() bool {
			client, ok := fixture.engines[0].Clients().Load(sid)
			return ok && client.Transport().Writable()
		})
		status, _ := clusterRequest(t, http.MethodGet, fixture.urls[1], sid, "")
		if status != http.StatusBadRequest {
			t.Fatalf("overlapping remote read status = %d", status)
		}
	})

	t.Run("should acquire read lock (same process)", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		sid := clusterHandshake(t, fixture.urls[0])
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pending, _ := http.NewRequestWithContext(ctx, http.MethodGet, clusterPollingURL(fixture.urls[1], sid), nil)
		go func() { //nolint:bodyclose
			response, err := http.DefaultClient.Do(pending)
			if err == nil {
				_ = response.Body.Close()
			}
		}()
		waitClusterCondition(t, func() bool {
			return fixture.engines[1].RemoteTransportCount() == 1
		})
		status, _ := clusterRequest(t, http.MethodGet, fixture.urls[0], sid, "")
		if status != http.StatusBadRequest {
			t.Fatalf("same-owner overlapping read status = %d", status)
		}
	})

	t.Run("should handle close from main process", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			socket := values[0].(Socket)
			time.AfterFunc(100*time.Millisecond, func() { socket.Close(false) })
		})
		sid := clusterHandshake(t, fixture.urls[0])
		status, body := clusterRequest(t, http.MethodGet, fixture.urls[1], sid, "")
		if status != http.StatusOK || body != "1" {
			t.Fatalf("remote close = status %d body %q", status, body)
		}
	})

	t.Run("should handle close from client", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		closed := make(chan string, 1)
		_ = fixture.engines[0].On("connection", func(values ...any) {
			_ = values[0].(Socket).On("close", func(reason ...any) {
				closed <- reason[0].(string)
			})
		})
		sid := clusterHandshake(t, fixture.urls[0])
		ctx, cancel := context.WithCancel(context.Background())
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, clusterPollingURL(fixture.urls[1], sid), nil)
		go func() { //nolint:bodyclose
			response, err := http.DefaultClient.Do(request)
			if err == nil {
				_ = response.Body.Close()
			}
		}()
		waitClusterCondition(t, func() bool {
			return fixture.engines[1].RemoteTransportCount() == 1
		})
		cancel()
		select {
		case reason := <-closed:
			if reason != "transport error" {
				t.Fatalf("close reason = %q", reason)
			}
		case <-time.After(time.Second):
			t.Fatal("owner did not observe remote transport close")
		}
	})

	t.Run("should ping/pong", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 50*time.Millisecond)
		sid := clusterHandshake(t, fixture.urls[2])
		for index := range 10 {
			status, body := clusterRequest(t, http.MethodGet, fixture.urls[index%3], sid, "")
			if status != http.StatusOK || body != "2" {
				t.Fatalf("ping %d = status %d body %q", index, status, body)
			}
			status, _ = clusterRequest(t, http.MethodPost, fixture.urls[(index+1)%3], sid, "3")
			if status != http.StatusOK {
				t.Fatalf("pong %d status = %d", index, status)
			}
		}
		waitClusterCondition(t, func() bool {
			for _, engine := range fixture.engines {
				if engine.PendingClusterRequests() != 0 || engine.RemoteTransportCount() != 0 {
					return false
				}
			}
			return true
		})
	})

	t.Run("should reject an invalid id", func(t *testing.T) {
		fixture := newOfficialClusterFixture(t, 0)
		status, _ := clusterRequest(t, http.MethodGet, fixture.urls[1], "01234567890123456789", "")
		if status != http.StatusBadRequest {
			t.Fatalf("unknown SID status = %d", status)
		}
	})

	t.Run("should upgrade", func(t *testing.T) {
		testOfficialRemoteUpgrade(t, false)
	})

	t.Run("should upgrade and send buffered messages", func(t *testing.T) {
		testOfficialRemoteUpgrade(t, true)
	})
}

func testOfficialRemoteUpgrade(t *testing.T, buffered bool) {
	t.Helper()
	fixture := newOfficialClusterFixture(t, 0)
	serverReceived := make(chan string, 1)
	_ = fixture.engines[1].On("connection", func(values ...any) {
		socket := values[0].(Socket)
		_ = socket.On("upgrade", func(...any) {
			socket.Send(strings.NewReader("hello"), nil, nil)
		})
		_ = socket.On("message", func(message ...any) {
			serverReceived <- packetReaderString(message[0])
			socket.Close(false)
		})
	})

	sid := clusterHandshake(t, fixture.urls[0])
	if buffered {
		status, _ := clusterRequest(t, http.MethodPost, fixture.urls[1], sid, "4hi")
		if status != http.StatusOK {
			t.Fatalf("buffered pre-upgrade write status = %d", status)
		}
	}
	wsURL := "ws" + strings.TrimPrefix(fixture.urls[1], "http") +
		"/engine.io/?EIO=4&transport=websocket&sid=" + sid
	connection, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("WebSocket dial status %d: %v", response.StatusCode, err)
		}
		t.Fatalf("WebSocket dial: %v", err)
	}
	defer func() { _ = connection.Close() }()
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("2probe")); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, message, err := connection.ReadMessage()
	if err != nil || string(message) != "3probe" {
		t.Fatalf("probe response = %q, %v", message, err)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, []byte("5")); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, message, err = connection.ReadMessage()
	if err != nil || string(message) != "4hello" {
		t.Fatalf("post-upgrade message = %q, %v", message, err)
	}
	if !buffered {
		if err := connection.WriteMessage(websocket.TextMessage, []byte("4hi")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case value := <-serverReceived:
		if value != "hi" {
			t.Fatalf("server message = %q", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upgraded socket did not receive hi")
	}
}

func TestOfficialClusterEngine010NodeCluster(t *testing.T) {
	for _, test := range []struct {
		name   string
		binary bool
	}{
		{name: "should ping/pong"},
		{name: "should send and receive binary", binary: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOfficialClusterFixture(t, 50*time.Millisecond)
			for _, engine := range fixture.engines {
				engine.Opts().SetPingInterval(50 * time.Millisecond)
			}
			if test.binary {
				for _, engine := range fixture.engines {
					_ = engine.On("connection", func(values ...any) {
						socket := values[0].(Socket)
						_ = socket.On("message", func(message ...any) {
							reader, _ := message[0].(io.Reader)
							socket.Send(reader, nil, nil)
						})
					})
				}
			}
			var sequence atomic.Uint64
			front := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				index := int((sequence.Add(1) - 1) % uint64(len(fixture.engines)))
				fixture.engines[index].ServeHTTP(response, request)
			}))
			t.Cleanup(front.Close)
			sid := clusterHandshake(t, front.URL)
			if !test.binary {
				for index := range 10 {
					status, body := clusterRequest(t, http.MethodGet, front.URL, sid, "")
					if status != http.StatusOK || body != "2" {
						t.Fatalf("cluster ping %d = status %d body %q", index, status, body)
					}
					status, _ = clusterRequest(t, http.MethodPost, front.URL, sid, "3")
					if status != http.StatusOK {
						t.Fatalf("cluster pong %d status = %d", index, status)
					}
				}
				return
			}

			status, _ := clusterRequest(t, http.MethodPost, front.URL, sid, "bAQIDBA==")
			if status != http.StatusOK {
				t.Fatalf("binary POST status = %d", status)
			}
			for range 100 {
				status, body := clusterRequest(t, http.MethodGet, front.URL, sid, "")
				if status != http.StatusOK {
					t.Fatalf("binary poll status = %d", status)
				}
				if body == "bAQIDBA==" {
					return
				}
				if body != "2" {
					t.Fatalf("binary poll body = %q", body)
				}
			}
			t.Fatalf("binary echo not observed for SID %s", sid)
		})
	}
}

func TestClusterEngineConcurrentRemoteReadLock(t *testing.T) {
	fixture := newOfficialClusterFixture(t, 0)
	sid := clusterHandshake(t, fixture.urls[0])

	type result struct {
		status int
		body   string
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			request, err := http.NewRequest(http.MethodGet, clusterPollingURL(fixture.urls[1], sid), nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			ready.Done()
			<-start
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				results <- result{err: err}
				return
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil {
				err = readErr
			} else if closeErr != nil {
				err = closeErr
			}
			results <- result{status: response.StatusCode, body: string(body), err: err}
		}()
	}
	ready.Wait()
	close(start)

	var rejected result
	select {
	case rejected = <-results:
	case <-time.After(time.Second):
		t.Fatal("concurrent remote polling requests both remained pending")
	}
	if rejected.err != nil || rejected.status != http.StatusBadRequest {
		t.Fatalf("first completed request = status %d body %q error %v, want one 400", rejected.status, rejected.body, rejected.err)
	}

	client, found := fixture.engines[0].Clients().Load(sid)
	if !found {
		t.Fatalf("owner does not contain SID %s", sid)
	}
	client.Send(strings.NewReader("winner"), nil, nil)

	var accepted result
	select {
	case accepted = <-results:
	case <-time.After(time.Second):
		t.Fatal("successful concurrent remote polling request did not receive its drain")
	}
	if accepted.err != nil || accepted.status != http.StatusOK || accepted.body != "4winner" {
		t.Fatalf("successful request = status %d body %q error %v", accepted.status, accepted.body, accepted.err)
	}

	requester := fixture.engines[1].(*clusterServer)
	waitClusterCondition(t, func() bool { return clusterRequestStateClean(requester) })
}

func TestClusterEngineCanceledRemoteRequestCleanup(t *testing.T) {
	fixture := newOfficialClusterFixture(t, 0)
	sid := clusterHandshake(t, fixture.urls[0])
	requestContext, cancel := context.WithCancel(t.Context())
	request := httptest.NewRequest(
		http.MethodGet,
		clusterPollingURL(fixture.urls[1], sid),
		nil,
	).WithContext(requestContext)
	httpContext := types.NewHttpContext(httptest.NewRecorder(), request)
	requester := fixture.engines[1].(*clusterServer)

	codeMessage, errorContext := requester.Verify(httpContext, false)
	if codeMessage != nil {
		t.Fatalf("remote Verify() = %v, %v", codeMessage, errorContext)
	}
	cancel()
	select {
	case <-httpContext.Done():
	case <-time.After(time.Second):
		t.Fatal("canceled HTTP context did not close")
	}
	waitClusterCondition(t, func() bool { return clusterRequestStateClean(requester) })

	// Cancellation after grant must actively release the owner-side forwarder;
	// waiting for a future business packet would silently discard that packet.
	waitClusterCondition(t, func() bool {
		_, found := fixture.engines[0].Clients().Load(sid)
		return !found
	})
}

func clusterRequestStateClean(server *clusterServer) bool {
	server.earlyDrainMu.Lock()
	drainsClean := len(server.expectedDrains) == 0 && len(server.earlyDrains) == 0
	server.earlyDrainMu.Unlock()
	if !drainsClean {
		return false
	}
	requestsClean := true
	server.remoteRequests.Range(func(_, _ any) bool {
		requestsClean = false
		return false
	})
	return requestsClean && server.PendingClusterRequests() == 0 && server.RemoteTransportCount() == 0
}

func TestClusterServerCustomSIDStaysLocal(t *testing.T) {
	bus := NewMemoryClusterBus()
	var acquisitions atomic.Int64
	unsubscribe, err := bus.Subscribe("observer", func(message *ClusterMessage) {
		if message.Type == ClusterMessageAcquireLock {
			acquisitions.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	ownerOptions := config.DefaultServerOptions()
	ownerOptions.SetGenerateId(func(*types.HttpContext) (string, error) {
		return "custom-sid", nil
	})
	owner, err := NewClusterServer(bus, ownerOptions, &ClusterOptions{ResponseTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	requester, err := NewClusterServer(bus, config.DefaultServerOptions(), &ClusterOptions{ResponseTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ownerHTTP := httptest.NewServer(owner)
	requesterHTTP := httptest.NewServer(requester)
	t.Cleanup(func() {
		owner.Close()
		requester.Close()
		ownerHTTP.Close()
		requesterHTTP.Close()
		unsubscribe()
		_ = bus.Close()
	})

	response, err := http.Get(clusterPollingURL(ownerHTTP.URL, "")) //nolint:gosec,noctx
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close handshake = %v/%v", readErr, closeErr)
	}
	var open struct {
		SID string `json:"sid"`
	}
	if response.StatusCode != http.StatusOK || len(body) == 0 || body[0] != '0' {
		t.Fatalf("custom handshake = status %d body %q", response.StatusCode, body)
	}
	if err := json.Unmarshal(body[1:], &open); err != nil {
		t.Fatal(err)
	}
	if open.SID != "custom-sid" {
		t.Fatalf("custom SID = %q", open.SID)
	}

	status, _ := clusterRequest(t, http.MethodGet, requesterHTTP.URL, open.SID, "")
	if status != http.StatusBadRequest {
		t.Fatalf("cross-node custom SID status = %d, want 400", status)
	}
	if acquisitions.Load() != 0 {
		t.Fatalf("custom SID emitted %d cluster lock requests, want 0", acquisitions.Load())
	}
}
