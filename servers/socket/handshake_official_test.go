package socket

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/v4/pkg/types"
)

func newOfficialHandshakeServer(t *testing.T, options *ServerOptions) *httptest.Server {
	t.Helper()
	server := NewServer(nil, options)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	return httpServer
}

func performOfficialHandshakeRequest(t *testing.T, method, target, origin string) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatalf("creating %s handshake request: %v", method, err)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("performing %s handshake request: %v", method, err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading %s handshake response: %v", method, readErr)
	}
	return response, string(body)
}

func TestOfficialHandshakeCORSHeaders(t *testing.T) {
	const origin = "http://localhost:54023"
	options := DefaultServerOptions()
	options.SetCors(&types.Cors{
		Origin:         origin,
		Methods:        []string{http.MethodGet, http.MethodPost},
		AllowedHeaders: []string{"content-type"},
		Credentials:    true,
	})
	httpServer := newOfficialHandshakeServer(t, options)
	target := httpServer.URL + "/socket.io/default/?transport=polling&EIO=4"

	t.Run("OPTIONS", func(t *testing.T) {
		response, body := performOfficialHandshakeRequest(t, http.MethodOptions, target, origin)
		if response.StatusCode != http.StatusNoContent || body != "" {
			t.Fatalf("OPTIONS status/body = %d/%q, want 204/empty", response.StatusCode, body)
		}
		for header, want := range map[string]string{
			"Access-Control-Allow-Origin":      origin,
			"Access-Control-Allow-Methods":     "GET,POST",
			"Access-Control-Allow-Headers":     "content-type",
			"Access-Control-Allow-Credentials": "true",
		} {
			if got := response.Header.Get(header); got != want {
				t.Errorf("%s = %q, want %q", header, got, want)
			}
		}
	})

	t.Run("GET", func(t *testing.T) {
		response, body := performOfficialHandshakeRequest(t, http.MethodGet, target, origin)
		if response.StatusCode != http.StatusOK || !strings.HasPrefix(body, `0{`) || !strings.Contains(body, `"sid":"`) {
			t.Fatalf("GET status/body = %d/%q, want 200/Engine.IO OPEN", response.StatusCode, body)
		}
		for header, want := range map[string]string{
			"Access-Control-Allow-Origin":      origin,
			"Access-Control-Allow-Credentials": "true",
		} {
			if got := response.Header.Get(header); got != want {
				t.Errorf("%s = %q, want %q", header, got, want)
			}
		}
	})
}

func TestOfficialHandshakeAllowRequest(t *testing.T) {
	for _, test := range []struct {
		name       string
		allow      bool
		wantStatus int
	}{
		{name: "allowed", allow: true, wantStatus: http.StatusOK},
		{name: "disallowed", allow: false, wantStatus: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := DefaultServerOptions()
			options.SetAllowRequest(func(*types.HttpContext) error {
				if test.allow {
					return nil
				}
				return errors.New("request rejected")
			})
			httpServer := newOfficialHandshakeServer(t, options)
			response, body := performOfficialHandshakeRequest(
				t,
				http.MethodGet,
				httpServer.URL+"/socket.io/default/?transport=polling&EIO=4",
				"http://foo.example",
			)
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status/body = %d/%q, want status %d", response.StatusCode, body, test.wantStatus)
			}
			if test.allow && (!strings.HasPrefix(body, `0{`) || !strings.Contains(body, `"sid":"`)) {
				t.Fatalf("allowed handshake body = %q, want Engine.IO OPEN", body)
			}
		})
	}
}
