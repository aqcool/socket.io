package socketio

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type testEchoRequest struct {
	Text string `json:"text"`
}

type testEchoResponse struct {
	Text string `json:"text"`
}

var testEchoEvent = NewEvent[testEchoRequest, testEchoResponse]("echo")

func TestV4PollingTypedAckAndSocketContext(t *testing.T) {
	ioServer, err := New(WithClientServing(false))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(ioServer.Handler())
	t.Cleanup(func() {
		_ = ioServer.Close()
		httpServer.Close()
	})

	connected := make(chan *Socket, 1)
	ioServer.OnConnection(func(socket *Socket) {
		Handle(socket, testEchoEvent, func(ctx context.Context, request testEchoRequest) (testEchoResponse, error) {
			if ctx != socket.Context() {
				t.Errorf("typed handler context is not the Socket lifetime context")
			}
			return testEchoResponse{Text: request.Text}, nil
		})
		connected <- socket
	})

	sid := pollingHandshake(t, httpServer.URL)
	pollingPush(t, httpServer.URL, sid, "40")
	if payload := pollingPoll(t, httpServer.URL, sid); !strings.HasPrefix(payload, `40{"sid":"`) {
		t.Fatalf("Socket.IO CONNECT packet = %q", payload)
	}

	var socket *Socket
	select {
	case socket = <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("v4 connection handler was not called")
	}
	if socket.ID() == "" || !socket.Connected() {
		t.Fatalf("invalid connected socket: id=%q connected=%t", socket.ID(), socket.Connected())
	}

	pollingPush(t, httpServer.URL, sid, `421["echo",{"text":"hello"}]`)
	ackPayload := pollingPoll(t, httpServer.URL, sid)
	if !strings.HasPrefix(ackPayload, "431[") {
		t.Fatalf("Socket.IO ACK packet = %q, want 431[...]", ackPayload)
	}
	var responses []testEchoResponse
	if err := json.Unmarshal([]byte(strings.TrimPrefix(ackPayload, "431")), &responses); err != nil {
		t.Fatalf("decoding typed ACK %q: %v", ackPayload, err)
	}
	if len(responses) != 1 || responses[0].Text != "hello" {
		t.Fatalf("typed ACK response = %#v", responses)
	}

	pollingPush(t, httpServer.URL, sid, "41")
	select {
	case <-socket.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Socket context was not cancelled after namespace disconnect")
	}
}

func pollingHandshake(t *testing.T, baseURL string) string {
	t.Helper()
	response := pollingRequest(t, http.MethodGet, pollingURL(baseURL, ""), "")
	if !strings.HasPrefix(response, "0{") {
		t.Fatalf("Engine.IO handshake payload = %q", response)
	}
	var open struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal([]byte(response[1:]), &open); err != nil {
		t.Fatalf("decoding Engine.IO handshake: %v", err)
	}
	if open.SID == "" {
		t.Fatal("Engine.IO handshake returned an empty sid")
	}
	return open.SID
}

func pollingPush(t *testing.T, baseURL, sid, payload string) {
	t.Helper()
	_ = pollingRequest(t, http.MethodPost, pollingURL(baseURL, sid), payload)
}

func pollingPoll(t *testing.T, baseURL, sid string) string {
	t.Helper()
	return pollingRequest(t, http.MethodGet, pollingURL(baseURL, sid), "")
}

func pollingURL(baseURL, sid string) string {
	values := url.Values{}
	values.Set("EIO", "4")
	values.Set("transport", "polling")
	if sid != "" {
		values.Set("sid", sid)
	}
	return strings.TrimRight(baseURL, "/") + "/socket.io/?" + values.Encode()
}

func pollingRequest(t *testing.T, method, target, payload string) string {
	t.Helper()
	var body io.Reader
	if payload != "" {
		body = strings.NewReader(payload)
	}
	request, err := http.NewRequest(method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("%s %s returned %d: %q", method, target, response.StatusCode, data)
	}
	return string(data)
}
