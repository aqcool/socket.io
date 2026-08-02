package amqp

import (
	"context"
	"os"
	"testing"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v3"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQFanoutIntegration(t *testing.T) {
	uri := os.Getenv("SOCKET_IO_AMQP_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_AMQP_TEST_URI is not set")
	}
	connection, err := amqp091.Dial(uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	first, err := NewBroker(connection, Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewBroker(connection, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close()
		_ = second.Close()
	})

	id, err := newMessageID()
	if err != nil {
		t.Fatal(err)
	}
	subject := "socket.io.integration." + id
	firstReceived := make(chan broker.Message, 1)
	secondReceived := make(chan broker.Message, 1)
	firstSubscription, err := first.Subscribe(t.Context(), subject, ackTo(firstReceived))
	if err != nil {
		t.Fatal(err)
	}
	secondSubscription, err := second.Subscribe(t.Context(), subject, ackTo(secondReceived))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = firstSubscription.Close()
		_ = secondSubscription.Close()
	})

	publishedID, err := first.Publish(t.Context(), subject, []byte("fanout"))
	if err != nil {
		t.Fatal(err)
	}
	for index, messages := range []<-chan broker.Message{firstReceived, secondReceived} {
		select {
		case message := <-messages:
			if message.ID != publishedID || string(message.Data) != "fanout" {
				t.Fatalf("subscriber %d received unexpected message: %#v", index, message)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("subscriber %d did not receive fanout message", index)
		}
	}
}

func ackTo(destination chan<- broker.Message) broker.Handler {
	return func(_ context.Context, message broker.Message) error {
		if err := message.Ack(); err != nil {
			return err
		}
		destination <- message
		return nil
	}
}
