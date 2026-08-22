package compilefail

import (
	"context"

	socket "github.com/aqcool/socket.io/servers/socket/v4"
	typed "github.com/aqcool/socket.io/typed/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

type request struct {
	Message string
}

type response struct {
	Accepted bool
}

type endpoint struct{}

func (*endpoint) Emit(string, ...any) error { return nil }

func (*endpoint) On(string, ...types.EventListener) error { return nil }

func (*endpoint) ServerSideEmit(string, ...any) error { return nil }

var ackEvent = typed.Event[request, response]{Name: "ack"}

func mustNotCompile() {
	ep := &endpoint{}
	typed.Emit(ep, ackEvent, "wrong request type")
	typed.On(ep, ackEvent, func(context.Context, string) error { return nil })
	var wrongResponse string
	wrongResponse, _ = typed.EmitAck(context.Background(), ep, ackEvent, request{})
	typed.ServerSideEmit(ep, ackEvent, 42)
	socket.DefaultServerOptions().SetAdapter("not an adapter")
	_ = wrongResponse
}
