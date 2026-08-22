package kafka

import (
	"github.com/IBM/sarama"
	broker "github.com/aqcool/socket.io/adapters/broker/v4"
)

func NewAdapterBuilder(
	producer sarama.SyncProducer,
	consumerFactory func() (sarama.Consumer, error),
	kafkaOptions Options,
	options broker.Options,
) (*broker.Builder, *Broker, error) {
	transport, err := NewBroker(producer, consumerFactory, kafkaOptions)
	if err != nil {
		return nil, nil, err
	}
	options.DeliverySemantics = broker.AtMostOnce
	options.Ordered = true
	return &broker.Builder{Broker: transport, Options: options}, transport, nil
}

func DialAdapterBuilder(
	brokers []string,
	config *sarama.Config,
	kafkaOptions Options,
	options broker.Options,
) (*broker.Builder, *Broker, error) {
	transport, err := Dial(brokers, config, kafkaOptions)
	if err != nil {
		return nil, nil, err
	}
	options.DeliverySemantics = broker.AtMostOnce
	options.Ordered = true
	return &broker.Builder{Broker: transport, Options: options}, transport, nil
}
