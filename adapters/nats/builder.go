package nats

import (
	broker "github.com/aqcool/socket.io/adapters/broker/v3"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewCoreAdapterBuilder(connection *natsgo.Conn, options broker.Options) (*broker.Builder, error) {
	transport, err := NewCoreBroker(connection)
	if err != nil {
		return nil, err
	}
	options.DeliverySemantics = broker.AtMostOnce
	options.Ordered = true
	return &broker.Builder{Broker: transport, Options: options}, nil
}

func NewJetStreamAdapterBuilder(
	client jetstream.JetStream,
	jetStreamOptions JetStreamOptions,
	options broker.Options,
) (*broker.Builder, error) {
	transport, err := NewJetStreamBroker(client, jetStreamOptions)
	if err != nil {
		return nil, err
	}
	options.DeliverySemantics = broker.AtLeastOnce
	options.Ordered = true
	return &broker.Builder{Broker: transport, Options: options}, nil
}
