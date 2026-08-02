package types

import (
	"errors"
	"testing"
)

func TestHttpServerCloseReportsNotRunning(t *testing.T) {
	server := NewWebServer(nil)
	var callbackErr error

	err := server.Close(func(closeErr error) {
		callbackErr = closeErr
	})

	if !errors.Is(err, ErrServerNotRunning) {
		t.Fatalf("Close error = %v, want ErrServerNotRunning", err)
	}
	if !errors.Is(callbackErr, ErrServerNotRunning) {
		t.Fatalf("Close callback error = %v, want ErrServerNotRunning", callbackErr)
	}
}
