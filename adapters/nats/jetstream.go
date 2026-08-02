package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v3"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type JetStreamOptions struct {
	Stream            string
	ConsumerPrefix    string
	AckWait           time.Duration
	MaxDeliver        int
	InactiveThreshold time.Duration
	DoubleAck         bool
	OnError           func(error)
}

type jetStreamConsumer interface {
	Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error)
}

type jetStreamClient interface {
	Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	CreateOrUpdateConsumer(context.Context, string, *jetstream.ConsumerConfig) (jetStreamConsumer, error)
	DeleteConsumer(context.Context, string, string) error
}

type natsJetStreamClient struct {
	client jetstream.JetStream
}

func (c *natsJetStreamClient) Publish(
	ctx context.Context,
	subject string,
	payload []byte,
	options ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	return c.client.Publish(ctx, subject, payload, options...)
}

func (c *natsJetStreamClient) CreateOrUpdateConsumer(
	ctx context.Context,
	stream string,
	config *jetstream.ConsumerConfig,
) (jetStreamConsumer, error) {
	return c.client.CreateOrUpdateConsumer(ctx, stream, *config)
}

func (c *natsJetStreamClient) DeleteConsumer(ctx context.Context, stream, consumer string) error {
	return c.client.DeleteConsumer(ctx, stream, consumer)
}

// JetStreamBroker provides at-least-once delivery through an existing
// JetStream stream. Every Socket.IO node creates its own consumer so all nodes
// receive each cluster command.
type JetStreamBroker struct {
	client  jetStreamClient
	options JetStreamOptions
}

func NewJetStreamBroker(client jetstream.JetStream, options JetStreamOptions) (*JetStreamBroker, error) {
	if client == nil {
		return nil, errors.New("NATS JetStream broker: client is required")
	}
	return newJetStreamBroker(&natsJetStreamClient{client: client}, options)
}

func NewJetStreamBrokerFromConn(connection *natsgo.Conn, options JetStreamOptions) (*JetStreamBroker, error) {
	if connection == nil {
		return nil, errors.New("NATS JetStream broker: connection is required")
	}
	client, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("create NATS JetStream client: %w", err)
	}
	return NewJetStreamBroker(client, options)
}

func newJetStreamBroker(client jetStreamClient, options JetStreamOptions) (*JetStreamBroker, error) {
	if strings.TrimSpace(options.Stream) == "" {
		return nil, errors.New("NATS JetStream broker: stream is required")
	}
	if options.ConsumerPrefix == "" {
		options.ConsumerPrefix = "socket-io"
	}
	if options.AckWait <= 0 {
		options.AckWait = 30 * time.Second
	}
	if options.MaxDeliver == 0 {
		options.MaxDeliver = 5
	}
	if options.InactiveThreshold <= 0 {
		options.InactiveThreshold = 5 * time.Minute
	}
	return &JetStreamBroker{client: client, options: options}, nil
}

func (b *JetStreamBroker) Publish(ctx context.Context, subject string, payload []byte) (string, error) {
	messageID, err := newMessageID()
	if err != nil {
		return "", err
	}
	ack, err := b.client.Publish(ctx, subject, append([]byte(nil), payload...), jetstream.WithMsgID(messageID))
	if err != nil {
		return "", err
	}
	if ack == nil {
		return "", errors.New("NATS JetStream broker: publish returned no acknowledgement")
	}
	return messageID, nil
}

func (b *JetStreamBroker) Subscribe(
	ctx context.Context,
	subject string,
	handler broker.Handler,
) (broker.Subscription, error) {
	if handler == nil {
		return nil, errors.New("NATS JetStream broker: handler is required")
	}
	consumerName, err := b.consumerName()
	if err != nil {
		return nil, err
	}
	consumer, err := b.client.CreateOrUpdateConsumer(ctx, b.options.Stream, &jetstream.ConsumerConfig{
		Name:              consumerName,
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           b.options.AckWait,
		MaxDeliver:        b.options.MaxDeliver,
		FilterSubject:     subject,
		InactiveThreshold: b.options.InactiveThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("create NATS JetStream consumer: %w", err)
	}
	consumeOptions := make([]jetstream.PullConsumeOpt, 0, 1)
	if b.options.OnError != nil {
		consumeOptions = append(consumeOptions, jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, consumeErr error) {
				b.options.OnError(consumeErr)
			},
		))
	}
	consumeContext, err := consumer.Consume(func(message jetstream.Msg) {
		messageID := message.Headers().Get(jetstream.MsgIDHeader)
		if messageID == "" {
			if metadata, metadataErr := message.Metadata(); metadataErr == nil {
				messageID = metadata.Stream + ":" + strconv.FormatUint(metadata.Sequence.Stream, 10)
			}
		}
		var terminal sync.Once
		var terminalErr error
		runTerminal := func(action func() error) error {
			terminal.Do(func() { terminalErr = action() })
			return terminalErr
		}
		ack := func() error { return runTerminal(message.Ack) }
		if b.options.DoubleAck {
			ack = func() error {
				return runTerminal(func() error { return message.DoubleAck(ctx) })
			}
		}
		nack := func() error { return runTerminal(message.Nak) }
		handleErr := handler(ctx, broker.Message{
			ID:   messageID,
			Data: append([]byte(nil), message.Data()...),
			Ack:  ack,
			Nack: nack,
		})
		if handleErr != nil {
			_ = nack()
		}
	}, consumeOptions...)
	if err != nil {
		_ = b.client.DeleteConsumer(context.Background(), b.options.Stream, consumerName)
		return nil, fmt.Errorf("consume NATS JetStream messages: %w", err)
	}
	return &jetStreamSubscription{
		consumeContext: consumeContext,
		deleteConsumer: func() error {
			deleteContext, cancel := context.WithTimeout(context.Background(), defaultFlushTimeout)
			defer cancel()
			return b.client.DeleteConsumer(deleteContext, b.options.Stream, consumerName)
		},
	}, nil
}

func (b *JetStreamBroker) consumerName() (string, error) {
	id, err := newMessageID()
	if err != nil {
		return "", err
	}
	prefix := sanitizeConsumerPrefix(b.options.ConsumerPrefix)
	if prefix == "" {
		prefix = "socket-io"
	}
	return prefix + "-" + id, nil
}

func sanitizeConsumerPrefix(prefix string) string {
	var result strings.Builder
	result.Grow(len(prefix))
	for _, character := range prefix {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9',
			character == '-', character == '_':
			result.WriteRune(character)
		default:
			result.WriteByte('-')
		}
	}
	return strings.Trim(result.String(), "-")
}

type jetStreamSubscription struct {
	consumeContext jetstream.ConsumeContext
	deleteConsumer func() error
}

func (s *jetStreamSubscription) Close() error {
	s.consumeContext.Stop()
	return s.deleteConsumer()
}

var _ broker.Broker = (*JetStreamBroker)(nil)
