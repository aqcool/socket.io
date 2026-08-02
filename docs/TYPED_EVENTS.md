# Go 强类型事件 API

`typed` 模块在现有动态 `Emit`/`On` API 之上增加可选的泛型层，不改变原有接口。客户端和服务端可以逐个事件迁移。

## 定义事件

```go
type CreateOrder struct {
    ProductID string `json:"productId"`
    Quantity  int    `json:"quantity"`
}

type OrderResult struct {
    OrderID string `json:"orderId"`
}

var CreateOrderEvent = typed.Event[CreateOrder, OrderResult]{
    Name:        "create-order",
    Direction:   typed.ClientToServer,
    Description: "创建订单",
}

var Orders = typed.NewNamespace("/orders", CreateOrderEvent.Definition())
```

不需要 ACK 的事件将响应类型设为 `typed.NoAck`。方向支持 `ClientToServer`、`ServerToClient`、`ServerToServer` 和 `Bidirectional`。

## 强类型请求与 ACK

服务端：

```go
err := typed.Handle(socket, CreateOrderEvent,
    func(ctx context.Context, request CreateOrder) (OrderResult, error) {
        return createOrder(ctx, request)
    },
)
```

客户端：

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

result, err := typed.EmitAck(ctx, client, CreateOrderEvent, CreateOrder{
    ProductID: "sku-42",
    Quantity:  2,
})
```

`EmitAck` 在 Context 超时或取消后返回 `context.DeadlineExceeded`/`context.Canceled`。动态 Parser 解码出的 `map[string]any` 会通过 JSON 标签转换为对应 Go struct。

普通强类型事件使用 `typed.Emit` 和 `typed.On`。集群节点事件使用 `typed.ServerSideEmit` 或 `typed.ServerSideEmitAck`。

`typed.Emit` 可直接接收服务端、Namespace、Socket、BroadcastOperator 或客户端。Go 中 `Server.Emit` 为链式 API、其余 emitter 返回 `error`，强类型层会统一这两种官方兼容的方法形状。

保留断开事件也有强类型定义：

```go
err := typed.On(socket, typed.DisconnectEvent,
    func(ctx context.Context, reason typed.DisconnectReason) error {
        // reason 是官方 DisconnectReason 集合中的值
        return nil
    },
)
```

## 生成 TypeScript、Go 封装和文档

```go
files, err := typed.Generate(
    []typed.Namespace{Orders},
    typed.GenerateOptions{Package: "events"},
)
if err != nil {
    panic(err)
}
if err = typed.WriteFiles("./events", files); err != nil {
    panic(err)
}
```

生成结果：

- `socketio_events.gen.ts`：TypeScript interface 和事件映射。
- `socketio_client.gen.go`：强类型客户端发送/监听函数。
- `socketio_server.gen.go`：强类型服务端处理/发送函数。
- `socketio_events.md`：Namespace、事件方向、请求和 ACK 文档。

Go 生成文件应输出到定义请求/响应 struct 的同一个 package。生成器保留 JSON 字段名以及 `omitempty`/`omitzero` 的 TypeScript 可选属性。

生成器会按方向和 ACK 类型选择 API：

- ClientToServer：客户端生成 `Emit*`/`EmitAck*`，服务端生成 `On*`/`Handle*`；
- ServerToClient：服务端生成 `Send*`，客户端生成 `On*` 或返回 ACK 的 `Handle*`；
- ServerToServer：生成 `Broadcast*`，有 ACK 时返回 `[]Response`；
- `NoAck` 事件只生成普通发送函数，有响应类型的事件只生成 ACK 形式。

TypeScript 可以直接给多个事件参数声明元组；Go 等价层使用一个请求 struct 聚合参数。这样事件名、方向、字段类型和 ACK 响应均由 Go 编译器检查，同时保留 JSON 字段名用于跨语言互操作。
