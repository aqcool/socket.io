package socketio

import "context"

func (*memoryAdapter) ServerSideEmitAck(ctx context.Context, _ []any) ([]any, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	// server-side emit targets the other servers in a cluster. The in-process
	// adapter has no remote peers, so there are no acknowledgement values.
	return nil, nil
}
