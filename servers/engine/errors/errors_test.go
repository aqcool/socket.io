package errors

import (
	stderrors "errors"
	"strings"
	"testing"
)

func TestNewTransportErrorWrapsSentinelAndDescription(t *testing.T) {
	description := stderrors.New("connection reset")
	err := NewTransportError("websocket read failed", description)
	if !stderrors.Is(err, ErrTransportFailure) || !stderrors.Is(err, description) {
		t.Fatalf("transport error did not preserve causes: %v", err)
	}
	if !strings.Contains(err.Error(), "websocket read failed") {
		t.Fatalf("transport error did not preserve reason: %v", err)
	}
}
