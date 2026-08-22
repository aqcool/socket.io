package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v4"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const fanoutExchange = "fanout"

type Options struct {
	DurableExchange   bool
	Prefetch          int
	PublishPersistent bool
	ExchangeArguments amqp091.Table
	QueueArguments    amqp091.Table
	OnError           func(error)
}

type channel interface {
	DeclareExchange(string, Options) error
	PublishConfirmed(context.Context, string, *amqp091.Publishing) error
	Subscribe(context.Context, string, Options) (<-chan amqp091.Delivery, <-chan *amqp091.Error, error)
	Close() error
}

type channelFactory func(bool) (channel, error)

type rabbitChannel struct {
	channel *amqp091.Channel
}

func (c *rabbitChannel) DeclareExchange(name string, options Options) error {
	return c.channel.ExchangeDeclare(
		name,
		fanoutExchange,
		options.DurableExchange,
		false,
		false,
		false,
		options.ExchangeArguments,
	)
}

func (c *rabbitChannel) PublishConfirmed(
	ctx context.Context,
	exchange string,
	message *amqp091.Publishing,
) error {
	confirmation, err := c.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		"",
		false,
		false,
		*message,
	)
	if err != nil {
		return err
	}
	if confirmation == nil {
		return errors.New("AMQP publisher confirm mode is not enabled")
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acknowledged {
		return errors.New("AMQP broker rejected published message")
	}
	return nil
}

func (c *rabbitChannel) Subscribe(
	ctx context.Context,
	exchange string,
	options Options,
) (<-chan amqp091.Delivery, <-chan *amqp091.Error, error) {
	if options.Prefetch > 0 {
		if err := c.channel.Qos(options.Prefetch, 0, false); err != nil {
			return nil, nil, err
		}
	}
	queue, err := c.channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		options.QueueArguments,
	)
	if err != nil {
		return nil, nil, err
	}
	if err = c.channel.QueueBind(queue.Name, "", exchange, false, nil); err != nil {
		return nil, nil, err
	}
	deliveries, err := c.channel.ConsumeWithContext(
		ctx,
		queue.Name,
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}
	return deliveries, c.channel.NotifyClose(make(chan *amqp091.Error, 1)), nil
}

func (c *rabbitChannel) Close() error {
	return c.channel.Close()
}

// Broker implements fanout cluster messaging over RabbitMQ/AMQP 0-9-1.
type Broker struct {
	publisher  channel
	newChannel channelFactory
	options    Options
	publishMu  sync.Mutex
	closeOnce  sync.Once
	closeErr   error
}

func NewBroker(connection *amqp091.Connection, options Options) (*Broker, error) {
	if connection == nil {
		return nil, errors.New("AMQP broker: connection is required")
	}
	factory := func(publisher bool) (channel, error) {
		instance, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		if publisher {
			if err = instance.Confirm(false); err != nil {
				_ = instance.Close()
				return nil, err
			}
		}
		return &rabbitChannel{channel: instance}, nil
	}
	publisher, err := factory(true)
	if err != nil {
		return nil, fmt.Errorf("create AMQP publisher channel: %w", err)
	}
	return newBroker(publisher, factory, options), nil
}

func newBroker(publisher channel, factory channelFactory, options Options) *Broker {
	if options.Prefetch <= 0 {
		options.Prefetch = 100
	}
	return &Broker{
		publisher:  publisher,
		newChannel: factory,
		options:    options,
	}
}

func (b *Broker) Publish(ctx context.Context, subject string, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	messageID, err := newMessageID()
	if err != nil {
		return "", err
	}
	deliveryMode := amqp091.Transient
	if b.options.PublishPersistent {
		deliveryMode = amqp091.Persistent
	}
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	if err = b.publisher.DeclareExchange(subject, b.options); err != nil {
		return "", fmt.Errorf("declare AMQP exchange: %w", err)
	}
	err = b.publisher.PublishConfirmed(ctx, subject, &amqp091.Publishing{
		ContentType:  "application/msgpack",
		DeliveryMode: deliveryMode,
		MessageId:    messageID,
		Timestamp:    time.Now(),
		Body:         append([]byte(nil), payload...),
	})
	if err != nil {
		return "", fmt.Errorf("publish AMQP message: %w", err)
	}
	return messageID, nil
}

func (b *Broker) Subscribe(
	ctx context.Context,
	subject string,
	handler broker.Handler,
) (broker.Subscription, error) {
	if handler == nil {
		return nil, errors.New("AMQP broker: handler is required")
	}
	instance, err := b.newChannel(false)
	if err != nil {
		return nil, fmt.Errorf("create AMQP consumer channel: %w", err)
	}
	if err = instance.DeclareExchange(subject, b.options); err != nil {
		_ = instance.Close()
		return nil, fmt.Errorf("declare AMQP exchange: %w", err)
	}
	consumeContext, cancel := context.WithCancel(ctx)
	deliveries, channelErrors, err := instance.Subscribe(consumeContext, subject, b.options)
	if err != nil {
		cancel()
		_ = instance.Close()
		return nil, fmt.Errorf("subscribe AMQP exchange: %w", err)
	}
	subscription := &amqpSubscription{
		channel: instance,
		cancel:  cancel,
	}
	subscription.waitGroup.Add(1)
	go b.consume(consumeContext, deliveries, channelErrors, handler, &subscription.waitGroup)
	return subscription, nil
}

func (b *Broker) consume(
	ctx context.Context,
	deliveries <-chan amqp091.Delivery,
	channelErrors <-chan *amqp091.Error,
	handler broker.Handler,
	waitGroup *sync.WaitGroup,
) {
	defer waitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case channelErr, open := <-channelErrors:
			if !open {
				channelErrors = nil
				continue
			}
			if channelErr != nil && b.options.OnError != nil {
				b.options.OnError(channelErr)
			}
		case delivery, open := <-deliveries:
			if !open {
				return
			}
			var terminal sync.Once
			var terminalErr error
			runTerminal := func(action func() error) error {
				terminal.Do(func() { terminalErr = action() })
				return terminalErr
			}
			ack := func() error {
				return runTerminal(func() error { return delivery.Ack(false) })
			}
			nack := func() error {
				return runTerminal(func() error { return delivery.Nack(false, true) })
			}
			handleErr := handler(ctx, broker.Message{
				ID:   delivery.MessageId,
				Data: append([]byte(nil), delivery.Body...),
				Ack:  ack,
				Nack: nack,
			})
			if handleErr != nil {
				_ = nack()
				if b.options.OnError != nil {
					b.options.OnError(handleErr)
				}
			}
		}
	}
}

func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.publisher.Close()
	})
	return b.closeErr
}

type amqpSubscription struct {
	channel   channel
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func (s *amqpSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.channel.Close()
		s.waitGroup.Wait()
	})
	return s.closeErr
}

var _ broker.Broker = (*Broker)(nil)
var _ broker.Subscription = (*amqpSubscription)(nil)
var _ interface{ Close() error } = (*Broker)(nil)
