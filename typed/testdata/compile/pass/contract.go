package compilepass

import (
	"context"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	typed "github.com/aqcool/socket.io/typed/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
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

var (
	plainEvent   = typed.Event[request, typed.NoAck]{Name: "plain"}
	ackEvent     = typed.Event[request, response]{Name: "ack"}
	clusterEvent = typed.Event[request, response]{Name: "cluster", Direction: typed.ServerToServer}
)

// compileContract is intentionally not executed. Compiling this package proves
// the positive half of the official Socket.IO type contract against the real
// exported generic API.
func compileContract(server *socket.Server, namespace socket.Namespace, socketValue *socket.Socket, broadcast *socket.BroadcastOperator) {
	ep := &endpoint{}
	options := socket.DefaultServerOptions()
	options.SetAdapter(&socket.AdapterBuilder{})
	_ = typed.Emit(ep, plainEvent, request{Message: "hello"})
	_ = typed.Emit(server, plainEvent, request{Message: "hello"})
	_ = typed.Emit(namespace, plainEvent, request{Message: "hello"})
	_ = typed.Emit(socketValue, plainEvent, request{Message: "hello"})
	_ = typed.Emit(broadcast, plainEvent, request{Message: "hello"})
	_ = typed.On(ep, plainEvent, func(context.Context, request) error { return nil })
	_, _ = typed.EmitAck(context.Background(), ep, ackEvent, request{Message: "hello"})
	_ = typed.Handle(ep, ackEvent, func(context.Context, request) (response, error) {
		return response{Accepted: true}, nil
	})
	_ = typed.On(ep, typed.DisconnectEvent, func(_ context.Context, reason typed.DisconnectReason) error {
		_ = reason
		return nil
	})
	_ = typed.ServerSideEmit(ep, clusterEvent, request{Message: "hello"})
	_, _ = typed.ServerSideEmitAck(context.Background(), ep, clusterEvent, request{Message: "hello"})
	var _ response
}
