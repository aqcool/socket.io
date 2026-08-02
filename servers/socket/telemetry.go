package socket

import "time"

type TelemetryKind string

const (
	TelemetryConnection       TelemetryKind = "connection"
	TelemetryDisconnection    TelemetryKind = "disconnection"
	TelemetryTransportUpgrade TelemetryKind = "transport_upgrade"
	TelemetryEventReceived    TelemetryKind = "event_received"
	TelemetryEventSent        TelemetryKind = "event_sent"
	TelemetryAckCompleted     TelemetryKind = "ack_completed"
	TelemetryAckTimeout       TelemetryKind = "ack_timeout"
)

// TelemetryEvent is emitted through Server's "telemetry" event. SocketID is
// supplied for tracing correlation, but metrics integrations must not use it as
// a label by default.
type TelemetryEvent struct {
	Kind              TelemetryKind
	Namespace         string
	SocketID          SocketId
	Transport         string
	Event             string
	Reason            string
	Bytes             int
	Duration          time.Duration
	Recovered         bool
	RecoveryAttempted bool
	Success           bool
	At                time.Time
	TraceMetadata     map[string]string
}

type AdapterTelemetryEvent struct {
	Operation string
	Namespace string
	Duration  time.Duration
	Success   bool
	At        time.Time
}

func (s *Socket) emitTelemetry(event *TelemetryEvent) {
	event.Namespace = s.nsp.Name()
	event.SocketID = s.id
	event.At = time.Now()
	if s.client != nil && s.client.conn != nil && s.client.conn.Transport() != nil {
		event.Transport = s.client.conn.Transport().Name()
	}
	s.server.EmitReserved("telemetry", *event)
}
