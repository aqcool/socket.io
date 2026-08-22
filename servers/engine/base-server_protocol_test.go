package engine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

func protocolTestContext(target string) *types.HttpContext {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	return types.NewHttpContext(httptest.NewRecorder(), request)
}

func TestVerifyStrictEIO(t *testing.T) {
	tests := []struct {
		name      string
		eio       string
		allowEIO3 bool
		wantError bool
	}{
		{name: "EIO4", eio: "4"},
		{name: "EIO3 allowed", eio: "3", allowEIO3: true},
		{name: "EIO3 disabled", eio: "3", wantError: true},
		{name: "missing", wantError: true},
		{name: "empty", eio: "", wantError: true},
		{name: "too old", eio: "2", allowEIO3: true, wantError: true},
		{name: "future", eio: "5", allowEIO3: true, wantError: true},
		{name: "not numeric", eio: "latest", allowEIO3: true, wantError: true},
		{name: "whitespace", eio: " 4", allowEIO3: true, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := config.DefaultServerOptions()
			options.SetAllowEIO3(test.allowEIO3)
			server := NewServer(options)

			target := "/engine.io/?transport=polling"
			if test.eio != "" {
				target += "&EIO=" + url.QueryEscape(test.eio)
			} else if test.name == "empty" {
				target += "&EIO="
			}
			codeMessage, context := server.Verify(protocolTestContext(target), false)

			if !test.wantError {
				if codeMessage != nil {
					t.Fatalf("Verify() error = %#v, context = %#v", codeMessage, context)
				}
				return
			}
			if codeMessage != UNSUPPORTED_PROTOCOL_VERSION {
				t.Fatalf("Verify() error = %#v, want UNSUPPORTED_PROTOCOL_VERSION", codeMessage)
			}
		})
	}

	t.Run("duplicate", func(t *testing.T) {
		options := config.DefaultServerOptions()
		options.SetAllowEIO3(true)
		server := NewServer(options)
		codeMessage, _ := server.Verify(protocolTestContext("/engine.io/?transport=polling&EIO=4&EIO=4"), false)
		if codeMessage != UNSUPPORTED_PROTOCOL_VERSION {
			t.Fatalf("Verify() error = %#v, want UNSUPPORTED_PROTOCOL_VERSION", codeMessage)
		}
	})
}

func TestHandshakeStrictEIOWhenCalledDirectly(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowEIO3(true)
	server := NewServer(options)

	for _, eio := range []string{"", "2", "5", "invalid"} {
		t.Run("EIO="+eio, func(t *testing.T) {
			target := "/engine.io/?transport=polling"
			if eio != "" {
				target += "&EIO=" + url.QueryEscape(eio)
			}
			codeMessage, transport := server.Handshake("polling", protocolTestContext(target))
			if codeMessage != UNSUPPORTED_PROTOCOL_VERSION {
				t.Fatalf("Handshake() error = %#v, want UNSUPPORTED_PROTOCOL_VERSION", codeMessage)
			}
			if transport != nil {
				t.Fatal("Handshake() unexpectedly created a transport")
			}
		})
	}
}

func TestExistingSessionRejectsProtocolMismatch(t *testing.T) {
	options := config.DefaultServerOptions()
	options.SetAllowEIO3(true)
	server := NewServer(options)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		httpServer.Close()
	})

	response, err := http.Get(httpServer.URL + "/engine.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("initial handshake failed: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("reading initial handshake failed: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initial handshake status = %d, body = %q", response.StatusCode, body)
	}

	var openPacket struct {
		SID string `json:"sid"`
	}
	if decodeErr := json.Unmarshal([]byte(strings.TrimPrefix(string(body), "0")), &openPacket); decodeErr != nil {
		t.Fatalf("decoding initial handshake %q failed: %v", body, decodeErr)
	}
	if openPacket.SID == "" {
		t.Fatalf("initial handshake %q did not include a sid", body)
	}

	mismatchURL := httpServer.URL + "/engine.io/?EIO=3&transport=polling&sid=" + url.QueryEscape(openPacket.SID)
	response, err = http.Get(mismatchURL)
	if err != nil {
		t.Fatalf("mismatched follow-up request failed: %v", err)
	}
	body, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("reading mismatched response failed: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("mismatched follow-up status = %d, want %d; body = %q", response.StatusCode, http.StatusBadRequest, body)
	}

	var protocolError types.CodeMessage
	if err := json.Unmarshal(body, &protocolError); err != nil {
		t.Fatalf("decoding mismatched response %q failed: %v", body, err)
	}
	if protocolError.Code != 5 {
		t.Fatalf("mismatched follow-up error code = %d, want 5", protocolError.Code)
	}
}
