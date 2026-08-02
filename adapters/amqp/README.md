# RabbitMQ / AMQP Adapter

本模块使用 RabbitMQ 官方 `amqp091-go` SDK 和通用 Broker 集群协议。

每个 Socket.IO Namespace 对应一个 fanout exchange。每个 Socket.IO 节点创建独占、自动删除的队列并绑定到该 exchange，因此每条集群命令都会到达全部在线节点。

## 使用

```go
connection, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
if err != nil {
    return err
}
defer connection.Close()

builder, transport, err := amqpadapter.NewAdapterBuilder(
    connection,
    amqpadapter.Options{
        DurableExchange:   true,
        Prefetch:          100,
        PublishPersistent: false,
        OnError: func(err error) {
            log.Printf("AMQP adapter: %v", err)
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

同名 exchange 的类型和 durable 参数必须一致。如果 exchange 已由其他应用创建，请让 `DurableExchange` 与现有声明保持一致。

## 投递语义

- Publisher Channel 开启 Confirm 模式；发布操作只有在 RabbitMQ ACK 后才成功。
- Consumer 使用手动 ACK。处理失败时 NACK 并重新入队。
- 每条消息包含全局 Message ID，通用层据此抑制重复投递。
- 单 Channel、单 Queue 内保持消息顺序；发生重新入队时，失败消息可能与后续消息重新排序。
- 独占队列只保留在线节点需要的集群控制命令，不重放节点离线期间的过期命令。
- 当前 Adapter 不保存 Socket.IO Session，因此不支持连接状态恢复。

## 外部 Emitter

`transport` 可直接用于通用 Emitter：

```go
emitter, err := broker.NewEmitter(transport, "socket.io")
if err != nil {
    return err
}
err = emitter.Of("/orders").To("warehouse").Emit("order-created", orderID)
```

应用负责监控连接关闭并重新建立 RabbitMQ Connection 和 Socket.IO Server。官方 SDK 本身不会自动恢复连接、Channel、exchange 或队列。
