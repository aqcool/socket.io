package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/IBM/sarama"
	broker "github.com/aqcool/socket.io/adapters/broker/v4"
)

type Options struct {
	PartitionKey string
	OnError      func(error)
}

type partitionConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	AsyncClose()
}

type consumer interface {
	Partitions(string) ([]int32, error)
	ConsumePartition(string, int32, int64) (partitionConsumer, error)
	Close() error
}

type saramaConsumer struct {
	consumer sarama.Consumer
}

func (c *saramaConsumer) Partitions(topic string) ([]int32, error) {
	return c.consumer.Partitions(topic)
}

func (c *saramaConsumer) ConsumePartition(
	topic string,
	partition int32,
	offset int64,
) (partitionConsumer, error) {
	return c.consumer.ConsumePartition(topic, partition, offset)
}

func (c *saramaConsumer) Close() error {
	return c.consumer.Close()
}

type consumerFactory func() (consumer, error)

// Broker gives every Socket.IO node an independent Kafka consumer. Each node
// reads every topic partition, so Kafka does not load-balance cluster commands
// between Socket.IO nodes.
type Broker struct {
	producer        sarama.SyncProducer
	consumerFactory consumerFactory
	options         Options
	closeOnce       sync.Once
	closeErr        error
}

func NewBroker(
	producer sarama.SyncProducer,
	factory func() (sarama.Consumer, error),
	options Options,
) (*Broker, error) {
	if producer == nil {
		return nil, errors.New("kafka broker: producer is required")
	}
	if factory == nil {
		return nil, errors.New("kafka broker: consumer factory is required")
	}
	return newBroker(producer, func() (consumer, error) {
		instance, err := factory()
		if err != nil {
			return nil, err
		}
		return &saramaConsumer{consumer: instance}, nil
	}, options), nil
}

func newBroker(producer sarama.SyncProducer, factory consumerFactory, options Options) *Broker {
	if options.PartitionKey == "" {
		options.PartitionKey = "socket.io"
	}
	return &Broker{producer: producer, consumerFactory: factory, options: options}
}

func Dial(brokers []string, config *sarama.Config, options Options) (*Broker, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka broker: at least one broker address is required")
	}
	if config == nil {
		config = sarama.NewConfig()
	}
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	result, err := NewBroker(producer, func() (sarama.Consumer, error) {
		return sarama.NewConsumer(brokers, config)
	}, options)
	if err != nil {
		_ = producer.Close()
		return nil, err
	}
	return result, nil
}

func (b *Broker) Publish(ctx context.Context, subject string, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	partition, offset, err := b.producer.SendMessage(&sarama.ProducerMessage{
		Topic: subject,
		Key:   sarama.StringEncoder(b.options.PartitionKey),
		Value: sarama.ByteEncoder(append([]byte(nil), payload...)),
	})
	if err != nil {
		return "", err
	}
	return messageID(subject, partition, offset), nil
}

func (b *Broker) Subscribe(
	ctx context.Context,
	subject string,
	handler broker.Handler,
) (broker.Subscription, error) {
	if handler == nil {
		return nil, errors.New("kafka broker: handler is required")
	}
	instance, err := b.consumerFactory()
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	partitions, err := instance.Partitions(subject)
	if err != nil {
		_ = instance.Close()
		return nil, fmt.Errorf("list Kafka topic partitions: %w", err)
	}
	if len(partitions) == 0 {
		_ = instance.Close()
		return nil, fmt.Errorf("kafka topic %q has no partitions", subject)
	}

	subscriptionContext, cancel := context.WithCancel(ctx)
	subscription := &kafkaSubscription{consumer: instance, cancel: cancel}
	for _, partition := range partitions {
		partitionInstance, consumeErr := instance.ConsumePartition(subject, partition, sarama.OffsetNewest)
		if consumeErr != nil {
			_ = subscription.Close()
			return nil, fmt.Errorf("consume Kafka partition %d: %w", partition, consumeErr)
		}
		subscription.partitions = append(subscription.partitions, partitionInstance)
		subscription.waitGroup.Add(1)
		go b.consumePartition(subscriptionContext, partitionInstance, handler, &subscription.waitGroup)
	}
	return subscription, nil
}

func (b *Broker) consumePartition(
	ctx context.Context,
	partition partitionConsumer,
	handler broker.Handler,
	waitGroup *sync.WaitGroup,
) {
	defer waitGroup.Done()
	messages := partition.Messages()
	errorsChannel := partition.Errors()
	for {
		select {
		case <-ctx.Done():
			return
		case consumeErr, open := <-errorsChannel:
			if !open {
				errorsChannel = nil
				continue
			}
			if consumeErr != nil && b.options.OnError != nil {
				b.options.OnError(consumeErr)
			}
		case message, open := <-messages:
			if !open {
				return
			}
			if message == nil {
				continue
			}
			if err := handler(ctx, broker.Message{
				ID:   messageID(message.Topic, message.Partition, message.Offset),
				Data: append([]byte(nil), message.Value...),
			}); err != nil && b.options.OnError != nil {
				b.options.OnError(err)
			}
		}
	}
}

func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.producer.Close()
	})
	return b.closeErr
}

type kafkaSubscription struct {
	consumer   consumer
	partitions []partitionConsumer
	cancel     context.CancelFunc
	waitGroup  sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
}

func (s *kafkaSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		for _, partition := range s.partitions {
			partition.AsyncClose()
		}
		s.waitGroup.Wait()
		s.closeErr = s.consumer.Close()
	})
	return s.closeErr
}

func messageID(topic string, partition int32, offset int64) string {
	return topic + ":" + strconv.FormatInt(int64(partition), 10) + ":" + strconv.FormatInt(offset, 10)
}

var _ broker.Broker = (*Broker)(nil)
var _ broker.Subscription = (*kafkaSubscription)(nil)
var _ interface{ Close() error } = (*Broker)(nil)
