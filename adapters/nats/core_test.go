package nats

import (
	"context"
	"testing"
	"time"

	broker "github.com/aqcool/socket.io/adapters/broker/v4"
	natsgo "github.com/nats-io/nats.go"
)

type fakeCoreClient struct {
	published *natsgo.Msg
	handler   natsgo.MsgHandler
	flushed   int
	closed    bool
}

func (c *fakeCoreClient) PublishMsg(message *natsgo.Msg) error {
	c.published = message
	return nil
}

func (c *fakeCoreClient) Subscribe(_ string, handler natsgo.MsgHandler) (coreSubscription, error) {
	c.handler = handler
	return &fakeCoreSubscription{close: func() { c.closed = true }}, nil
}

func (c *fakeCoreClient) FlushWithContext(context.Context) error {
	c.flushed++
	return nil
}

type fakeCoreSubscription struct {
	close func()
}

func (s *fakeCoreSubscription) Unsubscribe() error {
	s.close()
	return nil
}

func TestCoreBrokerPublishAndSubscribe(t *testing.T) {
	client := &fakeCoreClient{}
	transport := newCoreBroker(client, time.Second)
	payload := []byte("payload")
	messageID, err := transport.Publish(t.Context(), "socket.io._root", payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if messageID == "" || client.published.Header.Get(natsgo.MsgIdHdr) != messageID {
		t.Fatal("publish did not attach a stable message ID")
	}
	if string(client.published.Data) != "payload" || client.flushed != 1 {
		t.Fatalf("unexpected published message: data=%q flushes=%d", client.published.Data, client.flushed)
	}

	received := make(chan broker.Message, 1)
	subscription, err := transport.Subscribe(t.Context(), "socket.io._root", func(_ context.Context, message broker.Message) error {
		received <- message
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client.handler(&natsgo.Msg{
		Header: natsgo.Header{natsgo.MsgIdHdr: []string{"remote-id"}},
		Data:   []byte("remote"),
	})
	message := <-received
	if message.ID != "remote-id" || string(message.Data) != "remote" {
		t.Fatalf("unexpected received message: %#v", message)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if !client.closed {
		t.Fatal("subscription was not unsubscribed")
	}
}

func TestCoreBrokerRequiresConnection(t *testing.T) {
	if _, err := NewCoreBroker(nil); err == nil {
		t.Fatal("expected nil connection error")
	}
}
