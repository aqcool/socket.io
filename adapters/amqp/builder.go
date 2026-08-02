package amqp

import (
	broker "github.com/aqcool/socket.io/adapters/broker/v3"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func NewAdapterBuilder(
	connection *amqp091.Connection,
	amqpOptions Options,
	options broker.Options,
) (*broker.Builder, *Broker, error) {
	transport, err := NewBroker(connection, amqpOptions)
	if err != nil {
		return nil, nil, err
	}
	options.DeliverySemantics = broker.AtLeastOnce
	options.Ordered = true
	return &broker.Builder{Broker: transport, Options: options}, transport, nil
}
