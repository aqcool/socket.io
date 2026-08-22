package socketio

// ReceiveServerSideEvent dispatches an event received from another Socket.IO
// server to this namespace. It is intended for Adapter implementations; normal
// application code should use ServerSideEmit/ServerSideEmitAck instead.
func (n *Namespace) ReceiveServerSideEvent(event string, args ...any) error {
	if n == nil || n.hub == nil {
		return ErrClosed
	}
	if event == "" {
		return ErrInvalidEvent
	}
	n.hub.dispatch(event, append([]any(nil), args...))
	return nil
}
