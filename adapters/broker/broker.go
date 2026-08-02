package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	cluster "github.com/aqcool/socket.io/adapters/adapter/v3"
	socket "github.com/aqcool/socket.io/servers/socket/v3"
)

type Message struct {
	ID   string
	Data []byte
	Ack  func() error
	Nack func() error
}

type Handler func(context.Context, Message) error

type Subscription interface {
	Close() error
}

type Broker interface {
	Publish(context.Context, string, []byte) (string, error)
	Subscribe(context.Context, string, Handler) (Subscription, error)
}

type DeliverySemantics string

const (
	AtMostOnce  DeliverySemantics = "at-most-once"
	AtLeastOnce DeliverySemantics = "at-least-once"
)

type Options struct {
	ChannelPrefix       string
	DeliverySemantics   DeliverySemantics
	Ordered             bool
	Cluster             cluster.ClusterAdapterOptionsInterface
	DeduplicationWindow time.Duration
	MaxDedupEntries     int
}

type Builder struct {
	Broker  Broker
	Options Options
}

func (b *Builder) SupportsConnectionStateRecovery() bool { return false }

func (b *Builder) Capabilities() socket.AdapterCapabilities {
	return socket.AdapterCapabilities{
		Broadcast: true, RoomBroadcast: true, BroadcastAck: true,
		FetchSockets: true, SocketManagement: true, ServerSideEmit: true,
		NodeDiscovery: true, OrderedDelivery: b.Options.Ordered,
		DuplicateSuppression: true, ExternalEmitter: true,
	}
}

func (b *Builder) New(namespace socket.Namespace) socket.Adapter {
	options := b.Options
	if options.ChannelPrefix == "" {
		options.ChannelPrefix = "socket.io"
	}
	adapter := newAdapter(namespace, b.Broker, options)
	return adapter
}

type Adapter struct {
	cluster.ClusterAdapterWithHeartbeat
	broker       Broker
	options      Options
	subscription Subscription
	context      context.Context
	cancel       context.CancelFunc
	closeOnce    sync.Once
	deliveryMu   sync.Mutex
	seenMu       sync.Mutex
	seen         map[string]time.Time
}

func newAdapter(namespace socket.Namespace, broker Broker, options Options) *Adapter {
	ctx, cancel := context.WithCancel(context.Background())
	result := &Adapter{
		ClusterAdapterWithHeartbeat: cluster.MakeClusterAdapterWithHeartbeat(),
		broker:                      broker,
		options:                     options,
		context:                     ctx,
		cancel:                      cancel,
		seen:                        make(map[string]time.Time),
	}
	if result.options.DeduplicationWindow <= 0 {
		result.options.DeduplicationWindow = 2 * time.Minute
	}
	if result.options.MaxDedupEntries <= 0 {
		result.options.MaxDedupEntries = 10_000
	}
	result.Prototype(result)
	result.SetOpts(options.Cluster)
	result.Construct(namespace)
	return result
}

func (a *Adapter) Init() {
	if a.broker == nil {
		a.Emit("error", errors.New("broker adapter: broker is required"))
		return
	}
	subscription, err := a.broker.Subscribe(a.context, a.subject(), func(ctx context.Context, message Message) error {
		handleErr := a.onMessage(ctx, message)
		if handleErr != nil {
			a.Emit("error", handleErr)
		}
		return handleErr
	})
	if err != nil {
		a.Emit("error", err)
		return
	}
	a.subscription = subscription
	a.ClusterAdapterWithHeartbeat.Init()
}

func (a *Adapter) DoPublish(message *cluster.ClusterMessage) (cluster.Offset, error) {
	payload, err := cluster.EncodeClusterMessage(message)
	if err != nil {
		return "", err
	}
	id, err := a.broker.Publish(a.context, a.subject(), payload)
	return cluster.Offset(id), err
}

func (a *Adapter) DoPublishResponse(_ cluster.ServerId, response *cluster.ClusterResponse) error {
	_, err := a.DoPublish(response)
	return err
}

func (a *Adapter) Close() {
	a.closeOnce.Do(func() {
		a.ClusterAdapterWithHeartbeat.Close()
		a.cancel()
		if a.subscription != nil {
			if err := a.subscription.Close(); err != nil {
				a.Emit("error", err)
			}
		}
	})
}

func (a *Adapter) onMessage(_ context.Context, incoming Message) error {
	if a.options.Ordered {
		a.deliveryMu.Lock()
		defer a.deliveryMu.Unlock()
	}
	if incoming.ID != "" && !a.markMessage(incoming.ID) {
		if incoming.Ack != nil {
			return incoming.Ack()
		}
		return nil
	}
	message, err := cluster.DecodeClusterMessage(incoming.Data)
	if err != nil {
		if incoming.Nack != nil {
			_ = incoming.Nack()
		}
		return err
	}
	a.OnMessage(message, cluster.Offset(incoming.ID))
	if a.options.DeliverySemantics == AtLeastOnce && incoming.Ack != nil {
		return incoming.Ack()
	}
	return nil
}

func (a *Adapter) markMessage(id string) bool {
	now := time.Now()
	a.seenMu.Lock()
	defer a.seenMu.Unlock()
	if expiry, exists := a.seen[id]; exists && expiry.After(now) {
		return false
	}
	if len(a.seen) >= a.options.MaxDedupEntries {
		for key, expiry := range a.seen {
			if !expiry.After(now) {
				delete(a.seen, key)
			}
		}
	}
	if len(a.seen) >= a.options.MaxDedupEntries {
		var oldestKey string
		var oldest time.Time
		for key, expiry := range a.seen {
			if oldestKey == "" || expiry.Before(oldest) {
				oldestKey, oldest = key, expiry
			}
		}
		delete(a.seen, oldestKey)
	}
	a.seen[id] = now.Add(a.options.DeduplicationWindow)
	return true
}

func (a *Adapter) subject() string {
	return subject(a.options.ChannelPrefix, a.Nsp().Name())
}

func subject(prefix, namespaceName string) string {
	namespace := strings.TrimPrefix(namespaceName, "/")
	if namespace == "" {
		namespace = "_root"
	}
	return prefix + "." + namespace
}

var _ socket.Adapter = (*Adapter)(nil)
var _ cluster.ClusterAdapterWithHeartbeat = (*Adapter)(nil)
