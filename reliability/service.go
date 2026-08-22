package reliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	serversocket "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/servers/socket/v4/presence"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

const (
	DeliveryEvent = "__socketio_reliable_event"
	AckEvent      = "__socketio_reliable_ack"
	PublishEvent  = "__socketio_reliable_publish"
)

type Options struct {
	Limits          Limits
	ReplayBatchSize int
	ClientID        func(*serversocket.Socket) string
	UserID          func(*serversocket.Socket) string
	ReplayTargets   func(*serversocket.Socket) []Target
	OnError         func(error)
}

type Service struct {
	server *serversocket.Server
	store  EventStore
	opts   Options
	stop   chan struct{}
	once   sync.Once
}

func New(server *serversocket.Server, store EventStore, opts *Options) (*Service, error) {
	if server == nil {
		return nil, errors.New("reliability: server is required")
	}
	if store == nil {
		return nil, errors.New("reliability: event store is required")
	}
	options := Options{ReplayBatchSize: 500}
	if opts != nil {
		options = *opts
	}
	if options.ReplayBatchSize <= 0 {
		options.ReplayBatchSize = 500
	}
	if options.ClientID == nil {
		options.ClientID = defaultClientID
	}
	if options.UserID == nil {
		options.UserID = defaultUserID
	}
	service := &Service{server: server, store: store, opts: options, stop: make(chan struct{})}
	for _, namespace := range server.Namespaces() {
		service.instrumentNamespace(namespace)
	}
	_ = server.On("new_namespace", func(args ...any) {
		if namespace, ok := first(args).(serversocket.Namespace); ok {
			service.instrumentNamespace(namespace)
		}
	})
	if options.Limits.Retention > 0 || options.Limits.MaxEvents > 0 || options.Limits.MaxBytes > 0 {
		go service.pruneLoop()
	}
	return service, nil
}

func (s *Service) Close() {
	s.once.Do(func() { close(s.stop) })
}

func (s *Service) Emit(ctx context.Context, target Target, name string, args ...any) (Event, error) {
	if name == "" {
		return Event{}, errors.New("reliability: event name is required")
	}
	if target.Namespace == "" {
		target.Namespace = "/"
	}
	if target.Kind == "" {
		target.Kind = TargetNamespace
	}
	if target.Kind != TargetNamespace && target.ID == "" {
		return Event{}, errors.New("reliability: target ID is required")
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return Event{}, fmt.Errorf("reliability: encode event: %w", err)
	}
	now := time.Now()
	event := Event{
		ID:        utils.Base64Id().GenerateId(),
		Target:    target,
		Name:      name,
		Args:      args,
		CreatedAt: now,
		Size:      int64(len(payload)),
	}
	if s.opts.Limits.Retention > 0 {
		event.ExpiresAt = now.Add(s.opts.Limits.Retention)
	}
	event.Offset, err = s.store.Append(ctx, target, &event)
	if err != nil {
		return Event{}, fmt.Errorf("reliability: append event: %w", err)
	}
	if err = s.deliver(&event); err != nil {
		return event, err
	}
	return event, nil
}

func (s *Service) instrumentNamespace(namespace serversocket.Namespace) {
	_ = namespace.On("connection", func(args ...any) {
		socket, ok := first(args).(*serversocket.Socket)
		if !ok {
			return
		}
		clientID := s.opts.ClientID(socket)
		_ = socket.On(AckEvent, func(values ...any) {
			offset := stringValue(first(values))
			if clientID != "" && offset != "" {
				s.report(s.store.Ack(context.Background(), clientID, Offset(offset)))
			}
		})
		_ = socket.On(PublishEvent, func(values ...any) {
			s.handlePublish(socket, clientID, first(values))
		})
		go s.replay(socket, clientID)
	})
}

func (s *Service) replay(socket *serversocket.Socket, clientID string) {
	if clientID == "" {
		return
	}
	ctx := context.Background()
	after, err := s.store.LastAck(ctx, clientID)
	if err != nil {
		s.report(err)
		return
	}
	targets := []Target{
		{Kind: TargetNamespace, Namespace: socket.Nsp().Name()},
		{Kind: TargetSocket, Namespace: socket.Nsp().Name(), ID: string(socket.Id())},
	}
	if userID := s.opts.UserID(socket); userID != "" {
		targets = append(targets, Target{Kind: TargetUser, Namespace: socket.Nsp().Name(), ID: userID})
	}
	if s.opts.ReplayTargets != nil {
		targets = append(targets, s.opts.ReplayTargets(socket)...)
	}
	targets = append(targets, authRoomTargets(socket)...)
	for _, room := range socket.Rooms().Keys() {
		targets = append(targets, Target{Kind: TargetRoom, Namespace: socket.Nsp().Name(), ID: string(room)})
	}
	events := make([]Event, 0)
	seen := make(map[string]struct{})
	for _, target := range targets {
		replay, replayErr := s.store.Replay(ctx, target, after, s.opts.ReplayBatchSize)
		if replayErr != nil {
			s.report(replayErr)
			return
		}
		for i := range replay {
			if _, exists := seen[replay[i].ID]; !exists {
				seen[replay[i].ID] = struct{}{}
				events = append(events, replay[i])
			}
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].Offset < events[j].Offset
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
	for i := range events {
		if socket.Connected() {
			_ = socket.Emit(DeliveryEvent, &events[i])
		}
	}
}

func (s *Service) handlePublish(socket *serversocket.Socket, clientID string, raw any) {
	message, ok := raw.(map[string]any)
	if !ok {
		return
	}
	id, name := stringValue(message["id"]), stringValue(message["name"])
	if id == "" || name == "" || clientID == "" {
		return
	}
	expiresAt := time.Time{}
	if s.opts.Limits.Retention > 0 {
		expiresAt = time.Now().Add(s.opts.Limits.Retention)
	}
	fresh, err := s.store.MarkInbound(context.Background(), clientID, id, expiresAt)
	if err != nil {
		s.report(err)
		return
	}
	if fresh {
		args, _ := message["args"].([]any)
		socket.EmitUntyped(name, args...)
	}
}

func (s *Service) deliver(event *Event) error {
	namespace := s.server.Of(event.Target.Namespace, nil)
	switch event.Target.Kind {
	case TargetNamespace:
		return namespace.Emit(DeliveryEvent, event)
	case TargetRoom, TargetSocket:
		return namespace.To(serversocket.Room(event.Target.ID)).Emit(DeliveryEvent, event)
	case TargetUser:
		room := serversocket.Room(presence.DefaultUserRoomPrefix + event.Target.ID)
		return namespace.To(room).Emit(DeliveryEvent, event)
	default:
		return fmt.Errorf("reliability: unsupported target kind %q", event.Target.Kind)
	}
}

func (s *Service) pruneLoop() {
	interval := time.Minute
	if retention := s.opts.Limits.Retention; retention > 0 && retention < interval {
		interval = retention
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.report(s.store.Prune(context.Background(), s.opts.Limits))
		case <-s.stop:
			return
		}
	}
}

func (s *Service) report(err error) {
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

func defaultClientID(socket *serversocket.Socket) string {
	for _, key := range []string{"clientId", "userId"} {
		if value := stringValue(socket.Handshake().Auth[key]); value != "" {
			return value
		}
	}
	return string(socket.Id())
}

func defaultUserID(socket *serversocket.Socket) string {
	if value, ok := presence.StandardUserID(socket); ok {
		return value
	}
	return ""
}

func authRoomTargets(socket *serversocket.Socket) []Target {
	namespace := socket.Nsp().Name()
	value := socket.Handshake().Auth["reliableRooms"]
	result := make([]Target, 0)
	switch rooms := value.(type) {
	case []string:
		for _, room := range rooms {
			result = append(result, Target{Kind: TargetRoom, Namespace: namespace, ID: room})
		}
	case []any:
		for _, raw := range rooms {
			if room := stringValue(raw); room != "" {
				result = append(result, Target{Kind: TargetRoom, Namespace: namespace, ID: room})
			}
		}
	}
	return result
}

func first(args []any) any {
	if len(args) == 0 {
		return nil
	}
	return args[0]
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case Offset:
		return string(typed)
	default:
		return ""
	}
}
