package socketio

// NewLocalAdapter creates the native in-process adapter used as the local
// storage/broadcast primitive by distributed adapter implementations.
//
// The returned adapter owns room membership, local broadcasts, socket queries
// and connection-state-recovery state for the given namespace. Distributed
// adapters can embed or compose it and override only the cross-node operations.
func NewLocalAdapter(namespace *Namespace) (Adapter, error) {
	if namespace == nil {
		return nil, ErrInvalidArgument
	}
	return newMemoryAdapter(namespace), nil
}

// Server returns the server that owns this namespace.
func (n *Namespace) Server() *Server {
	if n == nil {
		return nil
	}
	return n.server
}
