package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v4"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeJetStreamClient struct {
	publishedSubject string
	publishedPayload []byte
	config           jetstream.ConsumerConfig
	consumer         *fakeJetStreamConsumer
	deleted          string
}

func (c *fakeJetStreamClient) Publish(
	_ context.Context,
	subject string,
	payload []byte,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	c.publishedSubject = subject
	c.publishedPayload = append([]byte(nil), payload...)
	return &jetstream.PubAck{Stream: "SOCKET_IO", Sequence: 7}, nil
}

func (c *fakeJetStreamClient) CreateOrUpdateConsumer(
	_ context.Context,
	_ string,
	config *jetstream.ConsumerConfig,
) (jetStreamConsumer, error) {
	c.config = *config
	c.consumer = &fakeJetStreamConsumer{context: &fakeConsumeContext{}}
	return c.consumer, nil
}

func (c *fakeJetStreamClient) DeleteConsumer(_ context.Context, _, consumer string) error {
	c.deleted = consumer
	return nil
}

type fakeJetStreamConsumer struct {
	handler jetstream.MessageHandler
	context *fakeConsumeContext
}

func (c *fakeJetStreamConsumer) Consume(
	handler jetstream.MessageHandler,
	_ ...jetstream.PullConsumeOpt,
) (jetstream.ConsumeContext, error) {
	c.handler = handler
	return c.context, nil
}

type fakeConsumeContext struct {
	stopped bool
}

func (c *fakeConsumeContext) Stop()  { c.stopped = true }
func (c *fakeConsumeContext) Drain() { c.stopped = true }

type fakeJetStreamMessage struct {
	data       []byte
	headers    natsgo.Header
	metadata   *jetstream.MsgMetadata
	acked      int
	nacked     int
	doubleAckd int
}

func (m *fakeJetStreamMessage) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metadata == nil {
		return nil, errors.New("metadata unavailable")
	}
	return m.metadata, nil
}
func (m *fakeJetStreamMessage) Data() []byte           { return m.data }
func (m *fakeJetStreamMessage) Headers() natsgo.Header { return m.headers }
func (m *fakeJetStreamMessage) Subject() string        { return "socket.io._root" }
func (m *fakeJetStreamMessage) Reply() string          { return "" }
func (m *fakeJetStreamMessage) Ack() error             { m.acked++; return nil }
func (m *fakeJetStreamMessage) Nak() error             { m.nacked++; return nil }
func (m *fakeJetStreamMessage) NakWithDelay(time.Duration) error {
	m.nacked++
	return nil
}
func (m *fakeJetStreamMessage) DoubleAck(context.Context) error {
	m.doubleAckd++
	return nil
}
func (m *fakeJetStreamMessage) InProgress() error           { return nil }
func (m *fakeJetStreamMessage) Term() error                 { return nil }
func (m *fakeJetStreamMessage) TermWithReason(string) error { return nil }

func TestJetStreamBrokerContract(t *testing.T) {
	client := &fakeJetStreamClient{}
	transport, err := newJetStreamBroker(client, JetStreamOptions{
		Stream:         "SOCKET_IO",
		ConsumerPrefix: "socket.io node",
	})
	if err != nil {
		t.Fatal(err)
	}
	messageID, err := transport.Publish(t.Context(), "socket.io._root", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if messageID == "" || client.publishedSubject != "socket.io._root" ||
		string(client.publishedPayload) != "payload" {
		t.Fatal("unexpected JetStream publish")
	}

	received := make(chan broker.Message, 1)
	subscription, err := transport.Subscribe(t.Context(), "socket.io._root", func(
		_ context.Context,
		message broker.Message,
	) error {
		received <- message
		return message.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.config.DeliverPolicy != jetstream.DeliverNewPolicy ||
		client.config.AckPolicy != jetstream.AckExplicitPolicy ||
		client.config.FilterSubject != "socket.io._root" {
		t.Fatalf("unexpected consumer config: %#v", client.config)
	}
	message := &fakeJetStreamMessage{
		data:    []byte("remote"),
		headers: natsgo.Header{jetstream.MsgIDHeader: []string{"remote-id"}},
	}
	client.consumer.handler(message)
	receivedMessage := <-received
	if receivedMessage.ID != "remote-id" || message.acked != 1 {
		t.Fatalf("message was not delivered and acknowledged: %#v", receivedMessage)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if !client.consumer.context.stopped || client.deleted != client.config.Name {
		t.Fatal("consumer was not stopped and deleted")
	}
}

func TestJetStreamBrokerNacksHandlerFailureOnce(t *testing.T) {
	client := &fakeJetStreamClient{}
	transport, err := newJetStreamBroker(client, JetStreamOptions{Stream: "SOCKET_IO"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = transport.Subscribe(t.Context(), "socket.io._root", func(
		_ context.Context,
		message broker.Message,
	) error {
		_ = message.Nack()
		return errors.New("decode failed")
	})
	if err != nil {
		t.Fatal(err)
	}
	message := &fakeJetStreamMessage{
		data: []byte("invalid"),
		metadata: &jetstream.MsgMetadata{
			Stream:   "SOCKET_IO",
			Sequence: jetstream.SequencePair{Stream: 42},
		},
	}
	client.consumer.handler(message)
	if message.nacked != 1 {
		t.Fatalf("message was nacked %d times", message.nacked)
	}
}

func TestJetStreamBrokerRequiresStream(t *testing.T) {
	if _, err := newJetStreamBroker(&fakeJetStreamClient{}, JetStreamOptions{}); err == nil {
		t.Fatal("expected missing stream error")
	}
}
