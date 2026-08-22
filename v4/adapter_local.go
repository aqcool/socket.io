package socketio

import (
	"context"
	"time"
)

// LocalAdapter is the native in-process adapter primitive used by distributed
// adapter implementations. It owns room membership, local broadcasts, socket
// queries and connection-state-recovery state for one namespace.
type LocalAdapter struct {
	*memoryAdapter
}

// NewLocalAdapter creates the native local adapter for a namespace.
func NewLocalAdapter(namespace *Namespace) (*LocalAdapter, error) {
	if namespace == nil {
		return nil, ErrInvalidArgument
	}
	return &LocalAdapter{memoryAdapter: newMemoryAdapter(namespace)}, nil
}

// BroadcastWithOffset performs a local broadcast while using a cluster-provided
// recovery offset. Distributed adapters must use this after publishing a packet
// when their transport provides a stable message offset; using one shared
// offset is what makes state recovery portable across nodes.
func (a *LocalAdapter) BroadcastWithOffset(
	ctx context.Context,
	packet Packet,
	opts *BroadcastOptions,
	offset string,
) error {
	if a == nil || a.memoryAdapter == nil {
		return ErrClosed
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if opts == nil {
		opts = &BroadcastOptions{}
	}
	if offset == "" || a.nsp.server.cfg.Recovery == nil || packet.Type != PacketEvent || packet.ID != nil || opts.Flags.Volatile {
		return a.Broadcast(ctx, packet, opts)
	}

	data, ok := packet.Data.([]any)
	if !ok {
		return a.Broadcast(ctx, packet, opts)
	}
	data = append(append([]any(nil), data...), offset)
	packet.Data = data

	now := time.Now()
	a.mu.Lock()
	a.cleanupExpiredLocked(now)
	a.packets = append(a.packets, persistedPacket{
		id:        offset,
		emittedAt: now,
		data:      append([]any(nil), data...),
		opts:      cloneBroadcastOptions(opts),
	})
	a.mu.Unlock()

	var firstErr error
	for _, id := range a.matchingIDs(opts) {
		if socket, ok := a.nsp.Socket(id); ok {
			if err := socket.sendPacket(packet, opts.Flags); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Server returns the server that owns this namespace.
func (n *Namespace) Server() *Server {
	if n == nil {
		return nil
	}
	return n.server
}
