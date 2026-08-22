package socket

import (
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

type (
	// SessionAwareAdapterBuilder is a builder for creating session-aware Adapter instances
	// that support connection state recovery.
	SessionAwareAdapterBuilder struct {
	}

	sessionAwareAdapter struct {
		Adapter

		maxDisconnectionDuration int64

		sessions     *types.Map[PrivateSessionId, *SessionWithTimestamp]
		packets      *types.Slice[*PersistedPacket]
		cleanupTimer *utils.Timer
	}
)

// New creates a new SessionAwareAdapter for the given Namespace.
func (*SessionAwareAdapterBuilder) New(nsp Namespace) Adapter {
	return NewSessionAwareAdapter(nsp)
}

func (*SessionAwareAdapterBuilder) SupportsConnectionStateRecovery() bool {
	return true
}

func (*SessionAwareAdapterBuilder) Capabilities() AdapterCapabilities {
	capabilities := (&AdapterBuilder{}).Capabilities()
	capabilities.ConnectionStateRecovery = true
	return capabilities
}

func MakeSessionAwareAdapter() SessionAwareAdapter {
	s := &sessionAwareAdapter{
		Adapter: MakeAdapter(),

		sessions: &types.Map[PrivateSessionId, *SessionWithTimestamp]{},
		packets:  types.NewSlice[*PersistedPacket](),
	}

	s.Prototype(s)

	return s
}

func NewSessionAwareAdapter(nsp Namespace) SessionAwareAdapter {
	s := MakeSessionAwareAdapter()

	s.Construct(nsp)

	return s
}

func (s *sessionAwareAdapter) SupportsConnectionStateRecovery() bool {
	return true
}

func (s *sessionAwareAdapter) Capabilities() AdapterCapabilities {
	return (&SessionAwareAdapterBuilder{}).Capabilities()
}

func (s *sessionAwareAdapter) Construct(nsp Namespace) {
	s.Adapter.Construct(nsp)

	cleanupInterval := DefaultSessionCleanupInterval
	if connectionStateRecovery := nsp.Server().Opts().ConnectionStateRecovery(); connectionStateRecovery != nil {
		s.maxDisconnectionDuration = connectionStateRecovery.MaxDisconnectionDuration()
		if connectionStateRecovery.GetRawSessionCleanupInterval() != nil {
			cleanupInterval = connectionStateRecovery.SessionCleanupInterval()
		}
	} else {
		s.maxDisconnectionDuration = DefaultMaxDisconnectionDuration
	}

	s.cleanupTimer = utils.SetInterval(func() {
		threshold := time.Now().UnixMilli() - s.maxDisconnectionDuration
		s.sessions.Range(func(sessionId PrivateSessionId, session *SessionWithTimestamp) bool {
			if session.DisconnectedAt < threshold {
				s.sessions.Delete(sessionId)
			}
			return true
		})
		_, _ = s.packets.RangeAndSplice(func(packet *PersistedPacket, i int) (bool, int, int, []*PersistedPacket) {
			return packet.EmittedAt < threshold, 0, i + 1, nil
		}, true)
	}, cleanupInterval)
	// prevents the timer from keeping the process alive
	s.cleanupTimer.Unref()
}

// Close stops the session cleanup timer and releases resources.
func (s *sessionAwareAdapter) Close() {
	utils.ClearInterval(s.cleanupTimer)
	s.Adapter.Close()
}

func (s *sessionAwareAdapter) PersistSession(session *SessionToPersist) {
	s.sessions.Store(session.Pid, &SessionWithTimestamp{SessionToPersist: session, DisconnectedAt: time.Now().UnixMilli()})
}

func (s *sessionAwareAdapter) RestoreSession(pid PrivateSessionId, offset string) (*Session, error) {
	session, ok := s.sessions.Load(pid)
	if !ok {
		// the session may have expired
		return nil, nil
	}

	hasExpired := session.DisconnectedAt+s.maxDisconnectionDuration < time.Now().UnixMilli()
	if hasExpired {
		// the session has expired
		s.sessions.Delete(pid)
		return nil, nil
	}

	// Find the index of the packet with the given offset
	index := s.packets.FindIndex(func(packet *PersistedPacket) bool {
		return packet.Id == offset
	})

	if index == -1 {
		// the offset may be too old
		return nil, nil
	}

	// Use a pre-allocated slice to avoid memory allocation in the loop
	missedPackets := make([]any, 0, s.packets.Len()-index-1)
	missedNum := 0
	// Iterate over the packets and append the data of those that should be included
	for i := index + 1; i < s.packets.Len(); i++ {
		packet, err := s.packets.Get(i)
		if err != nil {
			break
		}
		if ShouldIncludePacket(session.Rooms, packet.Opts) {
			missedPackets = append(missedPackets, packet.Data)
			missedNum++
		}
	}

	// Create a new Session object and return it
	return &Session{
		SessionToPersist: session.SessionToPersist,
		MissedPackets:    missedPackets[:missedNum],
	}, nil
}

func (s *sessionAwareAdapter) Broadcast(packet *parser.Packet, opts *BroadcastOptions) {
	isEventPacket := packet.Type == parser.EVENT
	// packets with acknowledgement are not stored because the acknowledgement function cannot be serialized and
	// restored on another server upon reconnection
	withoutAcknowledgement := packet.Id == nil
	notVolatile := opts == nil || opts.Flags == nil || !opts.Flags.Volatile
	if isEventPacket && withoutAcknowledgement && notVolatile {
		id := utils.YeastDate()
		// the offset is stored at the end of the data array, so the client knows the ID of the last packet it has
		// processed (and the format is backward-compatible)
		packet.Data = append(utils.TryCast[[]any](packet.Data), id)

		s.packets.Push(&PersistedPacket{
			Id:        id,
			EmittedAt: time.Now().UnixMilli(),
			Data:      packet.Data,
			Opts:      opts,
		})
	}
	s.Adapter.Broadcast(packet, opts)
}

// ShouldIncludePacket reports whether a persisted broadcast matches a
// recovered session's rooms.
func ShouldIncludePacket(sessionRooms *types.Set[Room], opts *BroadcastOptions) bool {
	if opts == nil {
		return true
	}
	rooms := opts.Rooms
	if rooms == nil {
		rooms = types.NewSet[Room]()
	}
	except := opts.Except
	if except == nil {
		except = types.NewSet[Room]()
	}
	if sessionRooms == nil {
		sessionRooms = types.NewSet[Room]()
	}

	included := rooms.Len() == 0
	notExcluded := true
	for _, room := range sessionRooms.Keys() {
		if included && !notExcluded {
			break
		}
		if !included && rooms.Has(room) {
			included = true
		}
		if notExcluded && except.Has(room) {
			notExcluded = false
		}
	}
	return included && notExcluded
}
