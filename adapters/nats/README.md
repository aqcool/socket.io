# NATS Adapter

本模块通过统一 Broker 协议提供两种 NATS Adapter：

- NATS Core：低延迟、至多一次投递，不持久化消息。
- NATS JetStream：至少一次投递、显式 ACK/NACK、服务端去重和独立节点消费者。

## NATS Core

```go
connection, err := nats.Connect(nats.DefaultURL)
if err != nil {
    return err
}

builder, err := natsadapter.NewCoreAdapterBuilder(
    connection,
    broker.Options{ChannelPrefix: "socket.io"},
)
if err != nil {
    return err
}

options := socket.DefaultServerOptions()
options.SetAdapter(builder)
server := socket.NewServer(nil, options)
```

Core Adapter 使用消息 Header 传递全局消息 ID，用于通用层的重复消息抑制。NATS Core 本身不提供重投或持久化，因此不支持连接状态恢复。

## NATS JetStream

先创建覆盖 Socket.IO subject 的 Stream，例如：

```go
js, err := jetstream.New(connection)
if err != nil {
    return err
}

_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:       "SOCKET_IO",
    Subjects:   []string{"socket.io.>"},
    Retention:  jetstream.LimitsPolicy,
    Storage:    jetstream.FileStorage,
    Duplicates: 2 * time.Minute,
})
if err != nil {
    return err
}
```

然后创建 Adapter：

```go
builder, err := natsadapter.NewJetStreamAdapterBuilder(
    js,
    natsadapter.JetStreamOptions{
        Stream:         "SOCKET_IO",
        ConsumerPrefix: "socket-io",
        AckWait:        30 * time.Second,
        MaxDeliver:     5,
        OnError:        func(err error) { log.Printf("JetStream: %v", err) },
    },
    broker.Options{ChannelPrefix: "socket.io"},
)
```

每个 Socket.IO 节点创建独立消费者，因此每条集群命令都会广播到全部节点，而不是在节点之间负载均衡。消费者启动策略为 `DeliverNewPolicy`，不会把节点启动前的过期集群控制命令重新执行。

JetStream 提供消息持久化，但当前 Adapter 仍明确声明不支持 Socket.IO 连接状态恢复。恢复需要保存 Session，并按 Socket.IO offset 精确重放事件，不能用 JetStream 的集群命令重投直接替代。

## 外部 Emitter

Core 或 JetStream transport 都可以传给通用 Emitter：

```go
emitter, err := broker.NewEmitter(transport, "socket.io")
if err != nil {
    return err
}
err = emitter.Of("/orders").To("warehouse").Emit("order-created", orderID)
```
