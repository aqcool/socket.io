package transports

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aqcool/socket.io/parsers/engine/v3/packet"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

func writePollingResponse(t *testing.T, compression *types.HttpCompression, payload string, acceptEncoding string, compress bool, userAgent string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "http://example.test/engine.io/?EIO=4&transport=polling", nil)
	request.Header.Set("Accept-Encoding", acceptEncoding)
	if userAgent != "" {
		request.Header.Set("User-Agent", userAgent)
	}
	recorder := httptest.NewRecorder()
	ctx := types.NewHttpContext(recorder, request)
	ctx.SetCleanup(func() {})
	transport := NewPolling(ctx).(*polling)
	transport.SetHttpCompression(compression)

	var callbackErr error
	transport.DoWrite(ctx, types.NewStringBuffer([]byte(payload)), &packet.Options{Compress: utils.Ptr(compress)}, func(err error) {
		callbackErr = err
	})
	if callbackErr != nil {
		t.Fatalf("DoWrite() error = %v", callbackErr)
	}
	return recorder
}

func decodedPollingBody(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var reader io.ReadCloser
	var err error
	switch recorder.Header().Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(recorder.Body)
	case "deflate":
		reader = flate.NewReader(recorder.Body)
	default:
		return recorder.Body.String()
	}
	if err != nil {
		t.Fatalf("creating decompressor: %v", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Errorf("closing decompressor: %v", closeErr)
		}
	}()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading compressed response: %v", err)
	}
	return string(decoded)
}

func TestPollingHTTPCompressionMatchesOfficialBehavior(t *testing.T) {
	large := strings.Repeat("a", 1024)
	tests := []struct {
		name           string
		compression    *types.HttpCompression
		payload        string
		acceptEncoding string
		compress       bool
		wantEncoding   string
	}{
		{name: "compress by default", compression: &types.HttpCompression{Threshold: 1024}, payload: large, acceptEncoding: "gzip, deflate", compress: true, wantEncoding: "gzip"},
		{name: "deflate", compression: &types.HttpCompression{Threshold: 1024}, payload: large, acceptEncoding: "deflate", compress: true, wantEncoding: "deflate"},
		{name: "custom threshold", compression: &types.HttpCompression{Threshold: 0}, payload: "small", acceptEncoding: "gzip, deflate", compress: true, wantEncoding: "gzip"},
		{name: "compression disabled", compression: nil, payload: large, acceptEncoding: "gzip, deflate", compress: true},
		{name: "per-message disabled", compression: &types.HttpCompression{Threshold: 0}, payload: large, acceptEncoding: "gzip, deflate", compress: false},
		{name: "below threshold", compression: &types.HttpCompression{Threshold: 1024}, payload: "small", acceptEncoding: "gzip, deflate", compress: true},
		{name: "honors quality zero", compression: &types.HttpCompression{Threshold: 0}, payload: large, acceptEncoding: "gzip;q=0, deflate;q=0.5", compress: true, wantEncoding: "deflate"},
		{name: "prefers highest quality", compression: &types.HttpCompression{Threshold: 0}, payload: large, acceptEncoding: "gzip;q=0.3, br;q=0.8", compress: true, wantEncoding: "br"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := writePollingResponse(t, test.compression, test.payload, test.acceptEncoding, test.compress, "")
			if got := recorder.Header().Get("Content-Encoding"); got != test.wantEncoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, test.wantEncoding)
			}
			if test.wantEncoding == "gzip" || test.wantEncoding == "deflate" {
				if got := decodedPollingBody(t, recorder); got != test.payload {
					t.Fatalf("decoded body length = %d, want %d", len(got), len(test.payload))
				}
			}
		})
	}
}

func TestPollingResponseHeadersMatchOfficialBehavior(t *testing.T) {
	for _, userAgent := range []string{
		"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)",
		"Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
	} {
		recorder := writePollingResponse(t, nil, "payload", "", true, userAgent)
		if got := recorder.Header().Get("X-XSS-Protection"); got != "0" {
			t.Fatalf("X-XSS-Protection = %q, want 0", got)
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
	}
}
