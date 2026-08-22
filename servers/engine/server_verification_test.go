package engine

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

func performVerificationRequest(t *testing.T, options config.ServerOptionsInterface, method string, target string) (*http.Response, types.CodeMessage, *types.ErrorMessage) {
	t.Helper()
	server := NewServer(options)
	errorEvents := make(chan *types.ErrorMessage, 1)
	_ = server.On("connection_error", func(args ...any) {
		if event, ok := args[0].(*types.ErrorMessage); ok {
			errorEvents <- event
		}
	})
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	request, err := http.NewRequest(method, httpServer.URL+target, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("performing request: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	var codeMessage types.CodeMessage
	if err := json.Unmarshal(body, &codeMessage); err != nil {
		t.Fatalf("decoding response %q: %v", body, err)
	}

	select {
	case event := <-errorEvents:
		return response, codeMessage, event
	case <-time.After(time.Second):
		t.Fatal("connection_error event was not emitted")
		return nil, types.CodeMessage{}, nil
	}
}

func TestServerVerificationMatchesOfficialErrors(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		target      string
		wantStatus  int
		wantCode    int
		wantMessage string
		contextKey  string
		context     any
	}{
		{name: "unknown transport", method: http.MethodGet, target: "/engine.io/?EIO=4&transport=tobi", wantStatus: http.StatusBadRequest, wantCode: 0, wantMessage: "Transport unknown", contextKey: "transport", context: "tobi"},
		{name: "constructor transport", method: http.MethodGet, target: "/engine.io/?EIO=4&transport=constructor", wantStatus: http.StatusBadRequest, wantCode: 0, wantMessage: "Transport unknown", contextKey: "transport", context: "constructor"},
		{name: "prototype transport", method: http.MethodGet, target: "/engine.io/?EIO=4&transport=__proto__", wantStatus: http.StatusBadRequest, wantCode: 0, wantMessage: "Transport unknown", contextKey: "transport", context: "__proto__"},
		{name: "unknown session", method: http.MethodGet, target: "/engine.io/?EIO=4&transport=polling&sid=test", wantStatus: http.StatusBadRequest, wantCode: 1, wantMessage: "Session ID unknown", contextKey: "sid", context: "test"},
		{name: "bad handshake method", method: http.MethodOptions, target: "/engine.io/?EIO=4&transport=polling", wantStatus: http.StatusBadRequest, wantCode: 2, wantMessage: "Bad handshake method", contextKey: "method", context: http.MethodOptions},
		{name: "unsupported protocol", method: http.MethodGet, target: "/engine.io/?EIO=3&transport=polling", wantStatus: http.StatusBadRequest, wantCode: 5, wantMessage: "Unsupported protocol version", contextKey: "protocol", context: 3},
		{name: "invalid session format", method: http.MethodGet, target: "/engine.io/?EIO=4&transport=polling&sid=%2Fetc%2Fpasswd", wantStatus: http.StatusBadRequest, wantCode: 3, wantMessage: "Bad request", contextKey: "name", context: "INVALID_SID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, body, event := performVerificationRequest(t, config.DefaultServerOptions(), test.method, test.target)
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			if body.Code != test.wantCode || body.Message != test.wantMessage {
				t.Fatalf("body = %#v, want code=%d message=%q", body, test.wantCode, test.wantMessage)
			}
			if event.Code != test.wantCode || event.Message != test.wantMessage {
				t.Fatalf("connection_error = %#v, want code=%d message=%q", event.CodeMessage, test.wantCode, test.wantMessage)
			}
			if got := event.Context[test.contextKey]; got != test.context {
				t.Fatalf("connection_error context[%q] = %#v, want %#v", test.contextKey, got, test.context)
			}
			if event.Req == nil {
				t.Fatal("connection_error request must not be nil")
			}
		})
	}
}

func TestServerVerificationAllowRequestRejection(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowRequest(func(*types.HttpContext) error {
		return errors.New("Thou shall not pass")
	})
	response, body, event := performVerificationRequest(t, options, http.MethodGet, "/engine.io/?EIO=4&transport=polling")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
	if body.Code != 4 || body.Message != "Thou shall not pass" {
		t.Fatalf("body = %#v, want forbidden message", body)
	}
	if event.Code != 4 || event.Context["message"] != "Thou shall not pass" {
		t.Fatalf("connection_error = %#v, context = %#v", event.CodeMessage, event.Context)
	}
}

func TestServerVerificationRejectsInvalidOrigin(t *testing.T) {
	server := NewServer(config.DefaultServerOptions())
	errorEvents := make(chan *types.ErrorMessage, 1)
	_ = server.On("connection_error", func(args ...any) {
		errorEvents <- args[0].(*types.ErrorMessage)
	})
	t.Cleanup(func() { server.Close() })

	request := httptest.NewRequest(http.MethodGet, "/engine.io/?EIO=4&transport=polling", nil)
	invalidOrigin := "http://engine.io/\n"
	// net/http rejects this value at the network parser. Injecting it after
	// request construction exercises Engine.IO's own validation path, matching
	// the official test hook.
	request.Header["Origin"] = []string{invalidOrigin}
	recorder := httptest.NewRecorder()
	ctx := types.NewHttpContext(recorder, request)
	server.HandleRequest(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var body types.CodeMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if body.Code != BAD_REQUEST.Code || body.Message != BAD_REQUEST.Message {
		t.Fatalf("body = %#v, want Bad request", body)
	}
	select {
	case event := <-errorEvents:
		if event.Context["name"] != "INVALID_ORIGIN" || event.Context["origin"] != invalidOrigin {
			t.Fatalf("connection_error context = %#v", event.Context)
		}
		if event.Req != ctx {
			t.Fatal("connection_error request does not match rejected request")
		}
	case <-time.After(time.Second):
		t.Fatal("connection_error event was not emitted")
	}
	if request.Header.Get("Origin") != "" {
		t.Fatal("invalid Origin was not removed before further processing")
	}
}
