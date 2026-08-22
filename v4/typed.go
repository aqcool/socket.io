package socketio

import (
	"context"
	"errors"
	"fmt"
)

type NoAck = struct{}

type Event[Request, Response any] struct {
	name string
}

func NewEvent[Request, Response any](name string) Event[Request, Response] {
	return Event[Request, Response]{name: name}
}

func (e Event[Request, Response]) Name() string {
	return e.name
}

type AckEmitter interface {
	EmitAck(context.Context, string, ...any) ([]any, error)
}

type MultiAckEmitter interface {
	EmitAcks(context.Context, string, ...any) ([][]any, error)
}

func Emit[Request, Response any](emitter RawEmitter, event Event[Request, Response], request Request) error {
	if emitter == nil {
		return errors.New("socketio: emitter is required")
	}
	if event.Name() == "" {
		return ErrInvalidEvent
	}
	return emitter.Emit(event.Name(), request)
}

func EmitAck[Request, Response any](
	ctx context.Context,
	emitter AckEmitter,
	event Event[Request, Response],
	request Request,
) (Response, error) {
	var zero Response
	if emitter == nil {
		return zero, errors.New("socketio: ACK emitter is required")
	}
	if event.Name() == "" {
		return zero, ErrInvalidEvent
	}
	if ctx == nil {
		ctx = context.Background()
	}
	values, err := emitter.EmitAck(ctx, event.Name(), request)
	if err != nil {
		return zero, err
	}
	if len(values) == 0 {
		return zero, errors.New("socketio: ACK response is empty")
	}
	return decodeValue[Response](emitter, values[0])
}

func EmitAcks[Request, Response any](
	ctx context.Context,
	emitter MultiAckEmitter,
	event Event[Request, Response],
	request Request,
) ([]Response, error) {
	if emitter == nil {
		return nil, errors.New("socketio: multi ACK emitter is required")
	}
	if event.Name() == "" {
		return nil, ErrInvalidEvent
	}
	if ctx == nil {
		ctx = context.Background()
	}
	results, err := emitter.EmitAcks(ctx, event.Name(), request)
	if err != nil {
		return nil, err
	}
	responses := make([]Response, 0, len(results))
	for _, values := range results {
		if len(values) == 0 {
			return nil, errors.New("socketio: ACK response is empty")
		}
		response, decodeErr := decodeValue[Response](emitter, values[0])
		if decodeErr != nil {
			return nil, decodeErr
		}
		responses = append(responses, response)
	}
	return responses, nil
}

func On[Request, Response any](
	registrar RawRegistrar,
	event Event[Request, Response],
	handler func(context.Context, Request) error,
) Subscription {
	if registrar == nil || handler == nil || event.Name() == "" {
		return closedSubscription{}
	}
	return registrar.On(event.Name(), func(ctx context.Context, args ...any) error {
		request, err := decodeValue[Request](registrar, first(args))
		if err != nil {
			return err
		}
		return handler(ctx, request)
	})
}

func Handle[Request, Response any](
	registrar RawRegistrar,
	event Event[Request, Response],
	handler func(context.Context, Request) (Response, error),
) Subscription {
	if registrar == nil || handler == nil || event.Name() == "" {
		return closedSubscription{}
	}
	return registrar.On(event.Name(), func(ctx context.Context, args ...any) error {
		request, err := decodeValue[Request](registrar, first(args))
		ack := findAck(args)
		if err != nil {
			if ack != nil {
				ack(nil, err)
			}
			return err
		}
		response, handlerErr := handler(ctx, request)
		if ack != nil {
			if handlerErr != nil {
				ack(nil, handlerErr)
			} else {
				ack([]any{response}, nil)
			}
		}
		return handlerErr
	})
}

func decodeValue[T any](owner any, value any) (T, error) {
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	var result T
	decoder, ok := owner.(ValueDecoder)
	if !ok {
		return result, fmt.Errorf("socketio: cannot decode %T into requested event type", value)
	}
	if err := decoder.DecodeValue(value, &result); err != nil {
		return result, fmt.Errorf("socketio: decode typed event value: %w", err)
	}
	return result, nil
}

func first(args []any) any {
	if len(args) == 0 {
		return nil
	}
	return args[0]
}

func findAck(args []any) Ack {
	if len(args) == 0 {
		return nil
	}
	ack, _ := args[len(args)-1].(Ack)
	return ack
}
