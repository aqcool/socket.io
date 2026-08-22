package typed

import (
	"context"
	"testing"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

type contextTestRegistrar struct {
	listener types.EventListener
}

func (r *contextTestRegistrar) On(_ string, listeners ...types.EventListener) error {
	if len(listeners) > 0 {
		r.listener = listeners[0]
	}
	return nil
}

func TestOnContextPropagatesCallerContext(t *testing.T) {
	type key string
	const contextKey key = "request-id"

	registrar := &contextTestRegistrar{}
	event := Event[string, struct{}]{Name: "message"}
	ctx := context.WithValue(context.Background(), contextKey, "abc")

	var got string
	if err := OnContext(ctx, registrar, event, func(handlerCtx context.Context, request string) error {
		got, _ = handlerCtx.Value(contextKey).(string)
		if request != "payload" {
			t.Fatalf("request = %q", request)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	registrar.listener("payload")
	if got != "abc" {
		t.Fatalf("context value = %q, want abc", got)
	}
}
