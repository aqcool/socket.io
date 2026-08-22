package socket

import (
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

func newOfficialAttachedHTTPServer(t *testing.T, options *ServerOptions) *httptest.Server {
	t.Helper()
	if options == nil {
		options = DefaultServerOptions()
	}
	httpServer := types.NewWebServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	server := NewServer(httpServer, options)
	testServer := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close(nil)
		testServer.Close()
	})
	return testServer
}

func getOfficialStaticAsset(t *testing.T, client *http.Client, target, acceptEncoding, origin, etag string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("creating static asset request: %v", err)
	}
	if acceptEncoding != "" {
		request.Header.Set("Accept-Encoding", acceptEncoding)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("requesting static asset: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("reading static asset: %v", readErr)
	}
	return response, body
}

func TestOfficialServerAttachmentServesEmbeddedClientBundles(t *testing.T) {
	httpServer := newOfficialAttachedHTTPServer(t, nil)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	tests := []struct {
		name        string
		path        string
		contentType string
	}{
		{name: "client", path: "socket.io.js", contentType: "application/javascript; charset=utf-8"},
		{name: "client with query", path: "socket.io.js?buster=123", contentType: "application/javascript; charset=utf-8"},
		{name: "source map", path: "socket.io.js.map", contentType: "application/json; charset=utf-8"},
		{name: "minified client", path: "socket.io.min.js", contentType: "application/javascript; charset=utf-8"},
		{name: "minified source map", path: "socket.io.min.js.map", contentType: "application/json; charset=utf-8"},
		{name: "msgpack client", path: "socket.io.msgpack.min.js", contentType: "application/javascript; charset=utf-8"},
		{name: "msgpack source map", path: "socket.io.msgpack.min.js.map", contentType: "application/json; charset=utf-8"},
		{name: "ESM client", path: "socket.io.esm.min.js", contentType: "application/javascript; charset=utf-8"},
		{name: "ESM source map", path: "socket.io.esm.min.js.map", contentType: "application/json; charset=utf-8"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, body := getOfficialStaticAsset(t, client, httpServer.URL+"/socket.io/"+test.path, "identity", "", "")
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status/body = %d/%q", response.StatusCode, body)
			}
			if got := response.Header.Get("Content-Type"); got != test.contentType {
				t.Errorf("Content-Type = %q, want %q", got, test.contentType)
			}
			if got := response.Header.Get("ETag"); got != `"`+EmbeddedClientVersion+`"` {
				t.Errorf("ETag = %q, want embedded client version", got)
			}
			if got := response.Header.Get("X-SourceMap"); got != "" {
				t.Errorf("X-SourceMap = %q, want absent", got)
			}
			if !strings.Contains(string(body), "engine.io") {
				t.Fatal("served asset does not contain the official Engine.IO client")
			}
		})
	}
}

func TestOfficialServerAttachmentCompression(t *testing.T) {
	httpServer := newOfficialAttachedHTTPServer(t, nil)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	for _, test := range []struct {
		name           string
		acceptEncoding string
		wantEncoding   string
		decode         func(io.Reader) (io.Reader, error)
	}{
		{
			name:           "gzip preferred from official list",
			acceptEncoding: "gzip,br,deflate",
			wantEncoding:   "gzip",
			decode: func(reader io.Reader) (io.Reader, error) {
				return gzip.NewReader(reader)
			},
		},
		{
			name:           "brotli",
			acceptEncoding: "br",
			wantEncoding:   "br",
			decode: func(reader io.Reader) (io.Reader, error) {
				return brotli.NewReader(reader), nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, compressed := getOfficialStaticAsset(t, client, httpServer.URL+"/socket.io/socket.io.js", test.acceptEncoding, "", "")
			if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != test.wantEncoding {
				t.Fatalf("status/encoding = %d/%q", response.StatusCode, response.Header.Get("Content-Encoding"))
			}
			if !strings.Contains(response.Header.Get("Vary"), "Accept-Encoding") {
				t.Errorf("Vary = %q, want Accept-Encoding", response.Header.Get("Vary"))
			}
			reader, err := test.decode(strings.NewReader(string(compressed)))
			if err != nil {
				t.Fatalf("creating decoder: %v", err)
			}
			decoded, err := io.ReadAll(reader)
			if closer, ok := reader.(io.Closer); ok {
				_ = closer.Close()
			}
			if err != nil || !strings.Contains(string(decoded), "engine.io") {
				t.Fatalf("decoded asset is invalid: %v", err)
			}
		})
	}
}

func TestOfficialServerAttachmentCORSAndConditionalRequests(t *testing.T) {
	options := DefaultServerOptions()
	options.SetCors(&types.Cors{Origin: "https://good-origin.com"})
	httpServer := newOfficialAttachedHTTPServer(t, options)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	target := httpServer.URL + "/socket.io/socket.io.js"

	response, _ := getOfficialStaticAsset(t, client, target, "identity", "https://good-origin.com", "")
	if response.StatusCode != http.StatusOK || response.Header.Get("Access-Control-Allow-Origin") != "https://good-origin.com" {
		t.Fatalf("CORS status/origin = %d/%q", response.StatusCode, response.Header.Get("Access-Control-Allow-Origin"))
	}

	for _, etag := range []string{`"` + EmbeddedClientVersion + `"`, `W/"` + EmbeddedClientVersion + `"`} {
		response, body := getOfficialStaticAsset(t, client, target, "identity", "", etag)
		if response.StatusCode != http.StatusNotModified || len(body) != 0 {
			t.Fatalf("If-None-Match %q status/body = %d/%q", etag, response.StatusCode, body)
		}
		if response.Header.Get("ETag") != `"`+EmbeddedClientVersion+`"` {
			t.Errorf("304 ETag = %q", response.Header.Get("ETag"))
		}
	}
}

func TestOfficialServerAttachmentCanDisableClientServing(t *testing.T) {
	options := DefaultServerOptions()
	options.SetServeClient(false)
	httpServer := newOfficialAttachedHTTPServer(t, options)
	response, body := getOfficialStaticAsset(t, http.DefaultClient, httpServer.URL+"/socket.io/socket.io.js", "identity", "", "")
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("disabled static serving status/body = %d/%q, want 400", response.StatusCode, body)
	}
}

func TestOfficialServerAttachmentServeHandler(t *testing.T) {
	server := NewServer(nil, nil)
	httpServer := httptest.NewServer(server.ServeHandler(nil))
	t.Cleanup(func() {
		server.Close(nil)
		httpServer.Close()
	})
	response, body := getOfficialStaticAsset(t, http.DefaultClient, httpServer.URL+"/socket.io/socket.io.min.js", "identity", "", "")
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "engine.io") {
		t.Fatalf("ServeHandler static status/body = %d/%q", response.StatusCode, body)
	}
}

func TestOfficialServerAttachmentExplicitAttach(t *testing.T) {
	server := NewServer(nil, nil)
	httpServer := types.NewWebServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	server.Attach(httpServer, nil)
	testServer := httptest.NewServer(httpServer)
	t.Cleanup(func() {
		server.Close(nil)
		testServer.Close()
	})
	response, body := getOfficialStaticAsset(t, http.DefaultClient, testServer.URL+"/socket.io/socket.io.js", "identity", "", "")
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "engine.io") {
		t.Fatalf("explicit Attach static status/body = %d/%q", response.StatusCode, body)
	}
}

func TestOfficialServerAttachmentMergesOptions(t *testing.T) {
	baseOptions := DefaultServerOptions()
	baseOptions.SetPingTimeout(6 * time.Second)
	server := NewServer(nil, baseOptions)
	httpServer := types.NewWebServer(nil)
	attachOptions := DefaultServerOptions()
	attachOptions.SetPingInterval(24 * time.Second)
	server.Attach(httpServer, attachOptions)
	t.Cleanup(func() { server.Close(nil) })

	if server.Engine().Opts().PingTimeout() != 6*time.Second || server.Engine().Opts().PingInterval() != 24*time.Second {
		t.Fatalf("merged ping timeout/interval = %v/%v", server.Engine().Opts().PingTimeout(), server.Engine().Opts().PingInterval())
	}
}

func reserveOfficialAttachmentAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving attachment address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing attachment address: %v", err)
	}
	return address
}

func assertOfficialStaticServerBound(t *testing.T, address string) {
	t.Helper()
	target := "http://" + address + "/socket.io/socket.io.js"
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := http.Get(target)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && strings.Contains(string(body), "engine.io") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("static client server at %s did not become ready: %v", target, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOfficialServerAttachmentConstructorAndListenBind(t *testing.T) {
	t.Run("constructor", func(t *testing.T) {
		address := reserveOfficialAttachmentAddress(t)
		server := NewServer(address, nil)
		t.Cleanup(func() { server.Close(nil) })
		assertOfficialStaticServerBound(t, address)
	})

	t.Run("Listen", func(t *testing.T) {
		address := reserveOfficialAttachmentAddress(t)
		server := NewServer(nil, nil).Listen(address, nil)
		t.Cleanup(func() { server.Close(nil) })
		assertOfficialStaticServerBound(t, address)
	})
}
