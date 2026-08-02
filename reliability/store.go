package reliability

import (
	"context"
	"time"
)

type Offset string
type TargetKind string

const (
	TargetNamespace TargetKind = "namespace"
	TargetRoom      TargetKind = "room"
	TargetSocket    TargetKind = "socket"
	TargetUser      TargetKind = "user"
)

type Target struct {
	Kind      TargetKind `json:"kind" msgpack:"kind"`
	Namespace string     `json:"namespace" msgpack:"namespace"`
	ID        string     `json:"id,omitempty" msgpack:"id,omitempty"`
}

type Event struct {
	ID        string    `json:"id" msgpack:"id"`
	Offset    Offset    `json:"offset" msgpack:"offset"`
	Target    Target    `json:"target" msgpack:"target"`
	Name      string    `json:"name" msgpack:"name"`
	Args      []any     `json:"args" msgpack:"args"`
	CreatedAt time.Time `json:"createdAt" msgpack:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt,omitzero" msgpack:"expiresAt,omitempty"`
	Size      int64     `json:"size" msgpack:"size"`
}

type Limits struct {
	Retention time.Duration
	MaxEvents int64
	MaxBytes  int64
}

// EventStore persists long-lived delivery state. Append must atomically assign
// an increasing, store-wide offset.
type EventStore interface {
	Append(context.Context, Target, *Event) (Offset, error)
	Replay(context.Context, Target, Offset, int) ([]Event, error)
	Ack(context.Context, string, Offset) error
	LastAck(context.Context, string) (Offset, error)
	// MarkInbound returns false when eventID was already processed for clientID.
	MarkInbound(context.Context, string, string, time.Time) (bool, error)
	Prune(context.Context, Limits) error
}
