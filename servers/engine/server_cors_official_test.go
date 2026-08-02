package engine

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v3/config"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestOfficialServerCORSResponses(t *testing.T) {
	t.Run("current origin preflight", func(t *testing.T) {
		server, httpServer := newOfficialCORSServer(t, &types.Cors{
			Origin:         true,
			AllowedHeaders: []string{"my-header"},
			Credentials:    true,
		})
		defer closeOfficialCORSServer(server, httpServer)

		request, _ := http.NewRequest(http.MethodOptions, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
		request.Header.Set("Origin", "http://engine.io")
		response := doOfficialCORSRequest(t, request)
		defer func() { _ = response.Body.Close() }()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusNoContent || len(body) != 0 {
			t.Fatalf("preflight response = %d/%q", response.StatusCode, body)
		}
		assertOfficialHeader(t, response, "Access-Control-Allow-Origin", "http://engine.io")
		assertOfficialHeader(t, response, "Access-Control-Allow-Methods", "GET,HEAD,PUT,PATCH,POST,DELETE")
		assertOfficialHeader(t, response, "Access-Control-Allow-Headers", "my-header")
		assertOfficialHeader(t, response, "Access-Control-Allow-Credentials", "true")
	})

	t.Run("current origin actual request", func(t *testing.T) {
		server, httpServer := newOfficialCORSServer(t, &types.Cors{
			Origin:         true,
			AllowedHeaders: []string{"my-header"},
			Credentials:    true,
		})
		defer closeOfficialCORSServer(server, httpServer)
		request, _ := http.NewRequest(http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
		request.Header.Set("Origin", "http://engine.io")
		response := doOfficialCORSRequest(t, request)
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("actual CORS status = %d", response.StatusCode)
		}
		assertOfficialHeader(t, response, "Access-Control-Allow-Origin", "http://engine.io")
		assertOfficialHeader(t, response, "Access-Control-Allow-Credentials", "true")
		if response.Header.Get("Access-Control-Allow-Methods") != "" || response.Header.Get("Access-Control-Allow-Headers") != "" {
			t.Fatalf("preflight headers leaked to actual response: %v", response.Header)
		}
	})

	t.Run("bad origin", func(t *testing.T) {
		server, httpServer := newOfficialCORSServer(t, &types.Cors{Origin: []string{"http://good-domain.com"}})
		defer closeOfficialCORSServer(server, httpServer)
		request, _ := http.NewRequest(http.MethodOptions, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
		request.Header.Set("Origin", "http://bad-domain.com")
		response := doOfficialCORSRequest(t, request)
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("bad-origin preflight status = %d", response.StatusCode)
		}
		if response.Header.Get("Access-Control-Allow-Origin") != "" || response.Header.Get("Access-Control-Allow-Credentials") != "" {
			t.Fatalf("bad origin received permission headers: %v", response.Header)
		}
	})

	t.Run("custom configuration", func(t *testing.T) {
		server, httpServer := newOfficialCORSServer(t, &types.Cors{
			Origin:               "http://good-domain.com",
			Methods:              []string{"GET", "PUT", "POST"},
			AllowedHeaders:       []string{"my-header"},
			ExposedHeaders:       []string{"my-exposed-header"},
			Credentials:          true,
			MaxAge:               "123",
			OptionsSuccessStatus: http.StatusOK,
		})
		defer closeOfficialCORSServer(server, httpServer)
		request, _ := http.NewRequest(http.MethodOptions, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
		request.Header.Set("Origin", "http://good-domain.com")
		response := doOfficialCORSRequest(t, request)
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("custom preflight status = %d", response.StatusCode)
		}
		for header, want := range map[string]string{
			"Access-Control-Allow-Origin":      "http://good-domain.com",
			"Access-Control-Allow-Methods":     "GET,PUT,POST",
			"Access-Control-Allow-Headers":     "my-header",
			"Access-Control-Allow-Credentials": "true",
			"Access-Control-Max-Age":           "123",
		} {
			assertOfficialHeader(t, response, header, want)
		}
		if response.Header.Get("Access-Control-Expose-Headers") != "" {
			t.Fatalf("exposed headers must not appear on preflight: %v", response.Header)
		}
	})
}

func TestOfficialServerWorksWithCORSEnabled(t *testing.T) {
	server, httpServer := newOfficialCORSServer(t, &types.Cors{Origin: true, AllowedHeaders: []string{"my-header"}, Credentials: true})
	defer closeOfficialCORSServer(server, httpServer)
	received := make(chan string, 1)
	_ = server.On("connection", func(args ...any) {
		_ = args[0].(Socket).Once("message", func(messageArgs ...any) {
			received <- messageArgs[0].(types.BufferInterface).String()
		})
	})

	handshake, _ := http.NewRequest(http.MethodGet, httpServer.URL+"/engine.io/?EIO=4&transport=polling", nil)
	handshake.Header.Set("Origin", "http://engine.io")
	response := doOfficialCORSRequest(t, handshake)
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	marker := []byte(`"sid":"`)
	start := bytes.Index(body, marker)
	if start == -1 {
		t.Fatalf("CORS handshake has no SID: %q", body)
	}
	start += len(marker)
	end := bytes.IndexByte(body[start:], '"')
	sid := string(body[start : start+end])
	post, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/engine.io/?EIO=4&transport=polling&sid="+url.QueryEscape(sid), strings.NewReader("4hey"))
	post.Header.Set("Origin", "http://engine.io")
	post.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	postResponse := doOfficialCORSRequest(t, post)
	_, _ = io.Copy(io.Discard, postResponse.Body)
	_ = postResponse.Body.Close()
	assertOfficialHeader(t, postResponse, "Access-Control-Allow-Origin", "http://engine.io")
	select {
	case got := <-received:
		if got != "hey" {
			t.Fatalf("CORS message = %q, want hey", got)
		}
	case <-time.After(time.Second):
		t.Fatal("message did not arrive with CORS enabled")
	}
}

func newOfficialCORSServer(t *testing.T, cors *types.Cors) (*server, *httptest.Server) {
	t.Helper()
	options := config.DefaultServerOptions()
	options.SetCors(cors)
	engineServer := NewServer(options).(*server)
	return engineServer, httptest.NewServer(engineServer)
}

func closeOfficialCORSServer(server *server, httpServer *httptest.Server) {
	server.Close()
	httpServer.Close()
}

func doOfficialCORSRequest(t *testing.T, request *http.Request) *http.Response {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("CORS request: %v", err)
	}
	return response
}

func assertOfficialHeader(t *testing.T, response *http.Response, header, want string) {
	t.Helper()
	if got := response.Header.Get(header); got != want {
		t.Fatalf("%s = %q, want %q", header, got, want)
	}
}
