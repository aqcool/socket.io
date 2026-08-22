package amqp

import (
	"context"
	"errors"
	"testing"

	broker "github.com/aqcool/socket.io/adapters/broker/v4"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type fakeChannel struct {
	declaredExchange string
	published        amqp091.Publishing
	deliveries       chan amqp091.Delivery
	channelErrors    chan *amqp091.Error
	closed           bool
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{
		deliveries:    make(chan amqp091.Delivery, 2),
		channelErrors: make(chan *amqp091.Error, 1),
	}
}

func (c *fakeChannel) DeclareExchange(exchange string, _ Options) error {
	c.declaredExchange = exchange
	return nil
}

func (c *fakeChannel) PublishConfirmed(
	_ context.Context,
	_ string,
	message *amqp091.Publishing,
) error {
	c.published = *message
	return nil
}

func (c *fakeChannel) Subscribe(
	_ context.Context,
	_ string,
	_ Options,
) (<-chan amqp091.Delivery, <-chan *amqp091.Error, error) {
	return c.deliveries, c.channelErrors, nil
}

func (c *fakeChannel) Close() error {
	c.closed = true
	return nil
}

type fakeAcknowledger struct {
	acks  int
	nacks int
}

func (a *fakeAcknowledger) Ack(uint64, bool) error {
	a.acks++
	return nil
}

func (a *fakeAcknowledger) Nack(uint64, bool, bool) error {
	a.nacks++
	return nil
}

func (a *fakeAcknowledger) Reject(uint64, bool) error {
	return nil
}

func TestAMQPBrokerPublishSubscribeAndClose(t *testing.T) {
	publisher := newFakeChannel()
	consumer := newFakeChannel()
	transport := newBroker(publisher, func(bool) (channel, error) {
		return consumer, nil
	}, Options{PublishPersistent: true})

	payload := []byte("outgoing")
	messageID, err := transport.Publish(t.Context(), "socket.io._root", payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if messageID == "" || publisher.declaredExchange != "socket.io._root" ||
		publisher.published.MessageId != messageID ||
		string(publisher.published.Body) != "outgoing" ||
		publisher.published.DeliveryMode != amqp091.Persistent {
		t.Fatalf("unexpected AMQP publication: %#v", publisher.published)
	}

	received := make(chan broker.Message, 1)
	subscription, err := transport.Subscribe(t.Context(), "socket.io._root", func(
		_ context.Context,
		message broker.Message,
	) error {
		if ackErr := message.Ack(); ackErr != nil {
			return ackErr
		}
		received <- message
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledger := &fakeAcknowledger{}
	consumer.deliveries <- amqp091.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  7,
		MessageId:    "remote-id",
		Body:         []byte("incoming"),
	}
	message := <-received
	if message.ID != "remote-id" || string(message.Data) != "incoming" || acknowledger.acks != 1 {
		t.Fatalf("unexpected AMQP delivery: %#v", message)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if !consumer.closed {
		t.Fatal("consumer channel was not closed")
	}
	if err = transport.Close(); err != nil {
		t.Fatal(err)
	}
	if !publisher.closed {
		t.Fatal("publisher channel was not closed")
	}
}

func TestAMQPBrokerNacksHandlerFailureOnce(t *testing.T) {
	publisher := newFakeChannel()
	consumer := newFakeChannel()
	reported := make(chan error, 1)
	transport := newBroker(publisher, func(bool) (channel, error) {
		return consumer, nil
	}, Options{OnError: func(err error) { reported <- err }})
	subscription, err := transport.Subscribe(t.Context(), "socket.io._root", func(
		_ context.Context,
		message broker.Message,
	) error {
		_ = message.Nack()
		return errors.New("decode failed")
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledger := &fakeAcknowledger{}
	consumer.deliveries <- amqp091.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  8,
		MessageId:    "invalid-id",
		Body:         []byte("invalid"),
	}
	if reportedErr := <-reported; reportedErr.Error() != "decode failed" {
		t.Fatalf("unexpected handler error: %v", reportedErr)
	}
	if acknowledger.nacks != 1 {
		t.Fatalf("delivery was nacked %d times", acknowledger.nacks)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAMQPBrokerRequiresConnection(t *testing.T) {
	if _, err := NewBroker(nil, Options{}); err == nil {
		t.Fatal("expected nil connection error")
	}
}
