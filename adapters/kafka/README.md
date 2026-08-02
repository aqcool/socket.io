# Kafka Adapter

Kafka Adapter 基于 Sarama 和通用 Broker 集群协议。

每个 Socket.IO 节点都会创建独立的 Kafka Consumer，并消费目标 Topic 的全部分区。这样每条集群命令都会发送到所有 Socket.IO 节点，不会像共享 Consumer Group 那样只由其中一个节点处理。

## 使用

需要提前创建与 Channel subject 同名的 Topic。根 Namespace 的默认 Topic 为 `socket.io._root`，例如 `/orders` 对应 `socket.io.orders`。

```go
config := sarama.NewConfig()
config.Version = sarama.V3_6_0_0
config.Producer.RequiredAcks = sarama.WaitForAll

builder, transport, err := kafkaadapter.DialAdapterBuilder(
    []string{"kafka-1:9092", "kafka-2:9092"},
    config,
    kafkaadapter.Options{
        PartitionKey: "socket.io",
        OnError: func(err error) {
            log.Printf("Kafka adapter: %v", err)
        },
    },
    broker.Options{ChannelPrefix: "socket.io"},
)
if err != nil {
    return err
}
defer transport.Close()

options := socket.DefaultServerOptions()
options.SetAdapter(builder)
server := socket.NewServer(nil, options)
```

## 投递和顺序语义

- 每个节点从订阅时的最新 offset 开始，不执行节点启动前的过期集群控制命令。
- Kafka 消息 ID 由 Topic、Partition 和 Offset 组成，用于重复消息抑制。
- 使用固定 `PartitionKey`，同一 Topic 的集群命令在采用 Hash Partitioner 时会进入同一分区，从而保持顺序。
- 如果自定义 Partitioner 把消息分散到多个分区，只保证分区内有序，不保证跨分区全局顺序。
- 当前实现为至多一次控制消息消费，不声明 Socket.IO 连接状态恢复能力。

Kafka Topic 的分区扩容会改变 Hash Partitioner 的映射。需要严格保持全局顺序时，建议每个 Socket.IO Topic 使用一个分区，或在扩容期间停止发布。

## 外部 Emitter

`transport` 实现了通用 `broker.Broker`，可直接用于外部 Emitter：

```go
emitter, err := broker.NewEmitter(transport, "socket.io")
if err != nil {
    return err
}
err = emitter.Of("/orders").To("warehouse").Emit("order-created", orderID)
```
