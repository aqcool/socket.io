package socketio

import "errors"

var (
	ErrClosed          = errors.New("socketio: closed")
	ErrAlreadyServing  = errors.New("socketio: server is already serving")
	ErrNotConnected    = errors.New("socketio: not connected")
	ErrQueueFull       = errors.New("socketio: event queue full")
	ErrUnsupported     = errors.New("socketio: unsupported")
	ErrInvalidEvent    = errors.New("socketio: invalid event")
	ErrInvalidArgument = errors.New("socketio: invalid argument")
)
