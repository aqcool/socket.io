# Broker Adapter

`broker` 模块提供消息系统 Adapter 的统一集群协议。具体的 NATS、Kafka 和 AMQP 模块只需实现 `Broker` 接口，即可复用广播、跨节点操作、心跳、去重和外部 Emitter。

## 接入消息系统

```go
type Broker interface {
    Publish(context.Context, string, []byte) (messageID string, err error)
    Subscribe(context.Context, string, Handler) (Subscription, error)
}
```

消息系统实现需要为每次投递提供稳定且唯一的 `Message.ID`。至少一次投递模式下可以设置 `Ack` 和 `Nack`；Adapter 会在消息处理成功后确认，解码失败时拒绝消息。

```go
builder := &broker.Builder{
    Broker: transport,
    Options: broker.Options{
        ChannelPrefix:       "socket.io",
        DeliverySemantics:   broker.AtLeastOnce,
        Ordered:             true,
        DeduplicationWindow: 2 * time.Minute,
        MaxDedupEntries:     10_000,
    },
}

options := socket.DefaultServerOptions()
options.SetAdapter(builder)
server := socket.NewServer(nil, options)
```

`MemoryBroker` 只用于测试和单进程验证，不提供跨进程通信或持久化。

## 外部 Emitter

外部进程可以使用相同的 Broker 连接向集群发送命令：

```go
emitter, err := broker.NewEmitter(transport, "socket.io")
if err != nil {
    return err
}

err = emitter.
    Of("/orders").
    To("warehouse").
    Except("paused").
    Emit("order-created", orderID)
```

Emitter 还支持 `SocketsJoin`、`SocketsLeave`、`DisconnectSockets` 和 `ServerSideEmit`。

## 能力范围

通用层已经覆盖普通广播、Room 定向广播、广播 ACK、`fetchSockets`、批量加入/离开房间、批量断开、`serverSideEmit`、节点发现与心跳、顺序处理、重复消息抑制和外部 Emitter。

连接状态恢复依赖消息系统提供可按偏移量读取的持久化日志，因此不会由通用 Broker 自动声明支持。具体 Adapter 必须实现并验证恢复语义后，才能将该能力标记为可用。
