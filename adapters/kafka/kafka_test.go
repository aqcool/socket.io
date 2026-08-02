package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	broker "github.com/aqcool/socket.io/adapters/broker/v3"
)

type fakeConsumer struct {
	partitions []int32
	consumers  map[int32]*fakePartitionConsumer
	closed     bool
}

func (c *fakeConsumer) Partitions(string) ([]int32, error) {
	return append([]int32(nil), c.partitions...), nil
}

func (c *fakeConsumer) ConsumePartition(
	_ string,
	partition int32,
	offset int64,
) (partitionConsumer, error) {
	if offset != sarama.OffsetNewest {
		return nil, errors.New("consumer did not start at newest offset")
	}
	return c.consumers[partition], nil
}

func (c *fakeConsumer) Close() error {
	c.closed = true
	return nil
}

type fakePartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	closed   bool
}

func newFakePartitionConsumer() *fakePartitionConsumer {
	return &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, 1),
		errors:   make(chan *sarama.ConsumerError, 1),
	}
}

func (c *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
func (c *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError     { return c.errors }
func (c *fakePartitionConsumer) AsyncClose()                              { c.closed = true }

func TestKafkaBrokerPublishAndSubscribeAllPartitions(t *testing.T) {
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(message *sarama.ProducerMessage) error {
		if message.Topic != "socket.io._root" {
			return errors.New("unexpected topic")
		}
		key, err := message.Key.Encode()
		if err != nil || string(key) != "cluster-order" {
			return errors.New("unexpected partition key")
		}
		return nil
	})
	firstPartition := newFakePartitionConsumer()
	secondPartition := newFakePartitionConsumer()
	consumerInstance := &fakeConsumer{
		partitions: []int32{0, 1},
		consumers: map[int32]*fakePartitionConsumer{
			0: firstPartition,
			1: secondPartition,
		},
	}
	transport := newBroker(producer, func() (consumer, error) {
		return consumerInstance, nil
	}, Options{PartitionKey: "cluster-order"})

	messageID, err := transport.Publish(t.Context(), "socket.io._root", []byte("outgoing"))
	if err != nil {
		t.Fatal(err)
	}
	if messageID != "socket.io._root:0:1" {
		t.Fatalf("unexpected published message ID %q", messageID)
	}

	received := make(chan broker.Message, 2)
	subscription, err := transport.Subscribe(t.Context(), "socket.io._root", func(
		_ context.Context,
		message broker.Message,
	) error {
		received <- message
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	firstPartition.messages <- &sarama.ConsumerMessage{
		Topic: "socket.io._root", Partition: 0, Offset: 12, Value: []byte("first"),
	}
	secondPartition.messages <- &sarama.ConsumerMessage{
		Topic: "socket.io._root", Partition: 1, Offset: 3, Value: []byte("second"),
	}
	ids := map[string]bool{}
	for range 2 {
		message := <-received
		ids[message.ID] = true
	}
	if !ids["socket.io._root:0:12"] || !ids["socket.io._root:1:3"] {
		t.Fatalf("unexpected consumed IDs: %v", ids)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if !consumerInstance.closed || !firstPartition.closed || !secondPartition.closed {
		t.Fatal("Kafka consumers were not closed")
	}
	if err = transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaBrokerReportsConsumerErrors(t *testing.T) {
	producer := mocks.NewSyncProducer(t, nil)
	partition := newFakePartitionConsumer()
	consumerInstance := &fakeConsumer{
		partitions: []int32{0},
		consumers:  map[int32]*fakePartitionConsumer{0: partition},
	}
	reported := make(chan error, 1)
	transport := newBroker(producer, func() (consumer, error) {
		return consumerInstance, nil
	}, Options{OnError: func(err error) { reported <- err }})
	subscription, err := transport.Subscribe(t.Context(), "topic", func(
		context.Context,
		broker.Message,
	) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := errors.New("partition failed")
	partition.errors <- &sarama.ConsumerError{Topic: "topic", Partition: 0, Err: expected}
	if consumeErr := <-reported; !errors.Is(consumeErr, expected) {
		t.Fatalf("unexpected consumer error: %v", consumeErr)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if err = transport.Close(); err != nil {
		t.Fatal(err)
	}
}
