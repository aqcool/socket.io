package nats

import (
	"context"
	"errors"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v4"
	natsgo "github.com/nats-io/nats.go"
)

const defaultFlushTimeout = 5 * time.Second

type coreSubscription interface {
	Unsubscribe() error
}

type coreClient interface {
	PublishMsg(*natsgo.Msg) error
	Subscribe(string, natsgo.MsgHandler) (coreSubscription, error)
	FlushWithContext(context.Context) error
}

type natsCoreClient struct {
	connection *natsgo.Conn
}

func (c *natsCoreClient) PublishMsg(message *natsgo.Msg) error {
	return c.connection.PublishMsg(message)
}

func (c *natsCoreClient) Subscribe(subject string, handler natsgo.MsgHandler) (coreSubscription, error) {
	return c.connection.Subscribe(subject, handler)
}

func (c *natsCoreClient) FlushWithContext(ctx context.Context) error {
	return c.connection.FlushWithContext(ctx)
}

// CoreBroker implements the common Broker contract with NATS Core Pub/Sub.
// NATS Core is at-most-once and does not persist messages.
type CoreBroker struct {
	client       coreClient
	flushTimeout time.Duration
}

func NewCoreBroker(connection *natsgo.Conn) (*CoreBroker, error) {
	if connection == nil {
		return nil, errors.New("NATS Core broker: connection is required")
	}
	return newCoreBroker(&natsCoreClient{connection: connection}, defaultFlushTimeout), nil
}

func newCoreBroker(client coreClient, flushTimeout time.Duration) *CoreBroker {
	if flushTimeout <= 0 {
		flushTimeout = defaultFlushTimeout
	}
	return &CoreBroker{client: client, flushTimeout: flushTimeout}
}

func (b *CoreBroker) Publish(ctx context.Context, subject string, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	messageID, err := newMessageID()
	if err != nil {
		return "", err
	}
	message := natsgo.NewMsg(subject)
	message.Data = append([]byte(nil), payload...)
	message.Header.Set(natsgo.MsgIdHdr, messageID)
	if err = b.client.PublishMsg(message); err != nil {
		return "", err
	}
	flushContext, cancel := withTimeout(ctx, b.flushTimeout)
	defer cancel()
	if err = b.client.FlushWithContext(flushContext); err != nil {
		return "", err
	}
	return messageID, nil
}

func (b *CoreBroker) Subscribe(ctx context.Context, subject string, handler broker.Handler) (broker.Subscription, error) {
	if handler == nil {
		return nil, errors.New("NATS Core broker: handler is required")
	}
	subscription, err := b.client.Subscribe(subject, func(message *natsgo.Msg) {
		_ = handler(ctx, broker.Message{
			ID:   message.Header.Get(natsgo.MsgIdHdr),
			Data: append([]byte(nil), message.Data...),
		})
	})
	if err != nil {
		return nil, err
	}
	flushContext, cancel := withTimeout(ctx, b.flushTimeout)
	defer cancel()
	if err = b.client.FlushWithContext(flushContext); err != nil {
		_ = subscription.Unsubscribe()
		return nil, err
	}
	return &coreBrokerSubscription{subscription: subscription}, nil
}

type coreBrokerSubscription struct {
	subscription coreSubscription
}

func (s *coreBrokerSubscription) Close() error {
	return s.subscription.Unsubscribe()
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

var _ broker.Broker = (*CoreBroker)(nil)
