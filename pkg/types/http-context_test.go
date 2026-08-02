package types

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHttpContext(t *testing.T) {
	t.Run("WriteOnce", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := NewHttpContext(rec, req)

		payload := []byte("hello")
		n, err := ctx.Write(payload)
		if err != nil {
			t.Fatalf("first write failed unexpectedly: %v", err)
		}
		if n != len(payload) {
			t.Fatalf("expected to write %d bytes, but wrote %d", len(payload), n)
		}

		if !bytes.Equal(rec.Body.Bytes(), payload) {
			t.Fatalf("expected body to be %q, got %q", payload, rec.Body.Bytes())
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
		}

		_, err = ctx.Write([]byte("world"))
		if !errors.Is(err, ErrResponseAlreadyWritten) {
			t.Fatalf("expected second write to fail with %q, got %v", ErrResponseAlreadyWritten, err)
		}

		if !bytes.Equal(rec.Body.Bytes(), payload) {
			t.Fatalf("body should not change after second write, got %q", rec.Body.Bytes())
		}
	})

	t.Run("QueryParsing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?foo=bar&foo=baz&x=1", nil)
		ctx := NewHttpContext(httptest.NewRecorder(), req)

		expectedSingle := "baz"
		if val := ctx.Query().Peek("foo"); val != expectedSingle {
			t.Errorf("expected Get(\"foo\") to be %q, got %q", expectedSingle, val)
		}

		expectedMulti := []string{"bar", "baz"}
		if vals, ok := ctx.Query().Gets("foo"); !ok || !reflect.DeepEqual(vals, expectedMulti) {
			t.Errorf("expected Gets(\"foo\") to be %v, got %v", expectedMulti, vals)
		}
	})

	t.Run("MultipleResponseHeaders", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := NewHttpContext(rec, req)

		cookies := []string{"a=1", "b=2"}
		ctx.ResponseHeaders().Add("Set-Cookie", cookies[0])
		ctx.ResponseHeaders().Add("Set-Cookie", cookies[1])

		if _, err := ctx.Write([]byte("ok")); err != nil {
			t.Fatalf("write failed unexpectedly: %v", err)
		}

		actualCookies := rec.Header()["Set-Cookie"]
		if !reflect.DeepEqual(actualCookies, cookies) {
			t.Fatalf("expected Set-Cookie headers %v, got %v", cookies, actualCookies)
		}
	})

	t.Run("FlushIdempotent", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Flush panicked on second call: %v", r)
			}
		}()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := NewHttpContext(rec, req)

		ctx.Flush()
	})

	t.Run("RequestHelpers", func(t *testing.T) {
		u := &url.URL{Path: "/foo/bar"}
		req := &http.Request{
			Method: http.MethodPost,
			Host:   "example.com",
			URL:    u,
			Header: http.Header{"User-Agent": []string{"GoTest"}},
		}
		ctx := NewHttpContext(httptest.NewRecorder(), req)

		testCases := []struct {
			name     string
			actual   any
			expected any
		}{
			{"Method", ctx.Method(), http.MethodPost},
			{"Path", ctx.Path(), "foo/bar"},
			{"UserAgent", ctx.UserAgent(), "GoTest"},
			{"IsSecure", ctx.Secure(), false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if !reflect.DeepEqual(tc.actual, tc.expected) {
					t.Errorf("expected %v, but got %v", tc.expected, tc.actual)
				}
			})
		}

		t.Run("Host", func(t *testing.T) {
			host := ctx.Host()
			expectedHost := "example.com"
			if host != expectedHost {
				t.Errorf("expected host %q, got %q", expectedHost, host)
			}
		})
	})

	t.Run("ConcurrentWriteSafety", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := NewHttpContext(rec, req)

		var (
			wg                   sync.WaitGroup
			goroutineCount       = 20
			successCount         int32
			expectedFailureCount int32
		)

		wg.Add(goroutineCount)
		for range goroutineCount {
			go func() {
				defer wg.Done()
				_, err := ctx.Write([]byte("x"))
				if err == nil {
					atomic.AddInt32(&successCount, 1)
				} else if errors.Is(err, ErrResponseAlreadyWritten) {
					atomic.AddInt32(&expectedFailureCount, 1)
				} else {
					t.Errorf("received unexpected error from Write: %v", err)
				}
			}()
		}
		wg.Wait()

		if successCount != 1 {
			t.Errorf("expected exactly 1 successful write, but got %d", successCount)
		}
		if expectedFailureCount != int32(goroutineCount-1) {
			t.Errorf("expected %d repeated-write errors, but got %d", goroutineCount-1, expectedFailureCount)
		}
		expectedBody := "x"
		if body := rec.Body.String(); body != expectedBody {
			t.Errorf("expected final body to be %q, but got %q", expectedBody, body)
		}
	})

	t.Run("NilRequest", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic for nil request")
			}
			if !errors.Is(r.(error), ErrNilRequest) {
				t.Fatalf("expected ErrNilRequest, got %v", r)
			}
		}()
		NewHttpContext(httptest.NewRecorder(), nil)
	})

	t.Run("NilResponseWriter", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic for nil response writer")
			}
			if !errors.Is(r.(error), ErrNilResponseWriter) {
				t.Fatalf("expected ErrNilResponseWriter, got %v", r)
			}
		}()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		NewHttpContext(nil, req)
	})

	t.Run("InvalidStatusCode", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := NewHttpContext(rec, req)

		if err := ctx.SetStatusCode(99); !errors.Is(err, ErrInvalidStatusCode) {
			t.Errorf("expected ErrInvalidStatusCode for 99, got %v", err)
		}
		if err := ctx.SetStatusCode(600); !errors.Is(err, ErrInvalidStatusCode) {
			t.Errorf("expected ErrInvalidStatusCode for 600, got %v", err)
		}
		if err := ctx.SetStatusCode(200); err != nil {
			t.Errorf("expected nil for 200, got %v", err)
		}
	})
}

func TestHttpContextWriteResponseIsAtomicWithCancellation(t *testing.T) {
	t.Run("response wins", func(t *testing.T) {
		requestContext, cancel := context.WithCancel(context.Background())
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestContext)
		ctx := NewHttpContext(recorder, request)
		headers := NewParameterBag(map[string][]string{"X-Test": {"yes"}})

		n, err := ctx.WriteResponse(http.StatusCreated, headers, bytes.NewBufferString("body"))
		cancel()
		if err != nil || n != 4 {
			t.Fatalf("WriteResponse result = %d/%v", n, err)
		}
		if recorder.Code != http.StatusCreated || recorder.Header().Get("X-Test") != "yes" || recorder.Body.String() != "body" {
			t.Fatalf("response = %d/%q/%q", recorder.Code, recorder.Header().Get("X-Test"), recorder.Body.String())
		}
	})

	t.Run("cancellation wins", func(t *testing.T) {
		requestContext, cancel := context.WithCancel(context.Background())
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestContext)
		ctx := NewHttpContext(recorder, request)
		cancel()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("canceled context was not finalized")
		}

		_, err := ctx.WriteResponse(
			http.StatusCreated,
			NewParameterBag(map[string][]string{"X-Test": {"unexpected"}}),
			bytes.NewBufferString("unexpected"),
		)
		if !errors.Is(err, ErrResponseAlreadyWritten) {
			t.Fatalf("WriteResponse error = %v, want ErrResponseAlreadyWritten", err)
		}
		if recorder.Header().Get("X-Test") != "" || recorder.Body.Len() != 0 {
			t.Fatalf("response was touched after cancellation: headers %v, body %q", recorder.Header(), recorder.Body.String())
		}
	})
}

func TestHttpContextCleanupConcurrentFinalization(t *testing.T) {
	for range 100 {
		ctx := NewHttpContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		var (
			cleanupCalls atomic.Int32
			wg           sync.WaitGroup
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			ctx.SetCleanup(func() {
				cleanupCalls.Add(1)
			})
		}()
		go func() {
			defer wg.Done()
			ctx.Flush()
		}()
		wg.Wait()
		<-ctx.Done()
		if calls := cleanupCalls.Load(); calls != 1 {
			t.Fatalf("cleanup calls = %d, want 1", calls)
		}
	}
}

func TestHttpContextCancellationEmitsCloseBeforeCleanupRemoval(t *testing.T) {
	requestContext, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestContext)
	ctx := NewHttpContext(httptest.NewRecorder(), request)
	closed := make(chan error, 1)
	listener := EventListener(func(args ...any) {
		closed <- args[0].(error)
	})
	_ = ctx.Once("close", listener)
	ctx.SetCleanup(func() { ctx.RemoveListener("close", listener) })
	cancel()

	select {
	case err := <-closed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("close error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not emit close")
	}
}
