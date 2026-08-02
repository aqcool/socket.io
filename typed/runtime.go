package typed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

// Emitter is any Socket.IO value that exposes Emit. Socket, Namespace,
// BroadcastOperator and the client return error from Emit, while Server
// follows the official fluent API and returns itself. emit normalizes both
// method shapes without weakening the request and ACK types carried by Event.
type Emitter = any

type Registrar = any

type ServerSideEmitter interface {
	ServerSideEmit(string, ...any) error
}

// OnConnect registers a statically typed listener for the reserved connect
// event, whose official contract has no listener parameters.
func OnConnect(registrar Registrar, handler func(context.Context) error) error {
	if handler == nil {
		return errors.New("typed socket.io: handler is required")
	}
	return register(registrar, ConnectEvent.Name, func(...any) {
		_ = handler(context.Background())
	})
}

// OnConnectError registers a statically typed listener for the reserved
// connect_error event.
func OnConnectError(registrar Registrar, handler func(context.Context, error) error) error {
	return On(registrar, ConnectErrorEvent, handler)
}

// OnDisconnect registers a statically typed listener for the reserved
// disconnect event and its official reason union.
func OnDisconnect(registrar Registrar, handler func(context.Context, DisconnectReason) error) error {
	return On(registrar, DisconnectEvent, handler)
}

func Emit[Request, Response any](emitter Emitter, event Event[Request, Response], request Request) error {
	return emit(emitter, event.Name, request)
}

func EmitAck[Request, Response any](
	ctx context.Context,
	emitter Emitter,
	event Event[Request, Response],
	request Request,
) (Response, error) {
	var zero Response
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan ackResult[Response], 1)
	var once sync.Once
	ack := func(values []any, err error) {
		once.Do(func() {
			if err != nil {
				result <- ackResult[Response]{err: err}
				return
			}
			value, decodeErr := decodeFirst[Response](values)
			result <- ackResult[Response]{value: value, err: decodeErr}
		})
	}
	if err := emit(emitter, event.Name, request, ack); err != nil {
		return zero, err
	}
	select {
	case response := <-result:
		return response.value, response.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func On[Request, Response any](
	registrar Registrar,
	event Event[Request, Response],
	handler func(context.Context, Request) error,
) error {
	if handler == nil {
		return errors.New("typed socket.io: handler is required")
	}
	return register(registrar, event.Name, func(args ...any) {
		request, err := decodeValue[Request](first(args))
		if err == nil {
			_ = handler(context.Background(), request)
		}
	})
}

func Handle[Request, Response any](
	registrar Registrar,
	event Event[Request, Response],
	handler func(context.Context, Request) (Response, error),
) error {
	if handler == nil {
		return errors.New("typed socket.io: handler is required")
	}
	return register(registrar, event.Name, func(args ...any) {
		request, err := decodeValue[Request](first(args))
		ack := findAck(args)
		if err != nil {
			if ack != nil {
				ack(nil, err)
			}
			return
		}
		response, handlerErr := handler(context.Background(), request)
		if ack != nil {
			if handlerErr != nil {
				ack(nil, handlerErr)
			} else {
				ack([]any{response}, nil)
			}
		}
	})
}

func ServerSideEmit[Request, Response any](
	emitter ServerSideEmitter,
	event Event[Request, Response],
	request Request,
) error {
	return emitter.ServerSideEmit(event.Name, request)
}

func ServerSideEmitAck[Request, Response any](
	ctx context.Context,
	emitter ServerSideEmitter,
	event Event[Request, Response],
	request Request,
) ([]Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan serverAckResult[Response], 1)
	ack := func(values []any, err error) {
		responses := make([]Response, 0, len(values))
		for _, raw := range values {
			value, decodeErr := decodeValue[Response](raw)
			if decodeErr != nil {
				result <- serverAckResult[Response]{err: decodeErr}
				return
			}
			responses = append(responses, value)
		}
		result <- serverAckResult[Response]{values: responses, err: err}
	}
	if err := emitter.ServerSideEmit(event.Name, request, ack); err != nil {
		return nil, err
	}
	select {
	case response := <-result:
		return response.values, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type ackResult[T any] struct {
	value T
	err   error
}

type serverAckResult[T any] struct {
	values []T
	err    error
}

func decodeFirst[T any](values []any) (T, error) {
	if len(values) == 0 {
		var zero T
		return zero, errors.New("typed socket.io: ACK response is empty")
	}
	return decodeValue[T](values[0])
}

func decodeValue[T any](value any) (T, error) {
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	var result T
	payload, err := json.Marshal(value)
	if err != nil {
		return result, fmt.Errorf("typed socket.io: encode value: %w", err)
	}
	if err = json.Unmarshal(payload, &result); err != nil {
		return result, fmt.Errorf("typed socket.io: decode value: %w", err)
	}
	return result, nil
}

func findAck(args []any) socket.Ack {
	if len(args) == 0 {
		return nil
	}
	ack, _ := args[len(args)-1].(socket.Ack)
	return ack
}

func register(registrar Registrar, event string, listener types.EventListener) error {
	switch typed := registrar.(type) {
	case interface {
		On(string, ...types.EventListener) error
	}:
		return typed.On(event, listener)
	case interface {
		On(types.EventName, ...types.EventListener) error
	}:
		return typed.On(types.EventName(event), listener)
	default:
		return errors.New("typed socket.io: registrar does not support On")
	}
}

func first(args []any) any {
	if len(args) == 0 {
		return nil
	}
	return args[0]
}

func emit(emitter Emitter, event string, args ...any) error {
	switch typed := emitter.(type) {
	case interface {
		Emit(string, ...any) error
	}:
		return typed.Emit(event, args...)
	case interface {
		Emit(string, ...any) *socket.Server
	}:
		typed.Emit(event, args...)
		return nil
	default:
		return errors.New("typed socket.io: emitter does not support Emit")
	}
}
