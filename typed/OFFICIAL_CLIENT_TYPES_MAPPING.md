# Socket.IO Client 4.8.3 类型契约映射

## 基准与边界

- 上游分母：`socket.io-client@4.8.3` 的 `typed-events.test-d.ts`，共 14 项。
- Go 核心客户端保留动态 API：`On(...any)`、`Emit(...any)` 与 `EmitWithAck(...any)`。
- 可选 `typed` 模块用泛型 `Event[Request, Response]`、方向声明和生成函数提供静态类型等价。它不是对 TypeScript API 名称的逐字复制，但会让错误参数、错误事件方向和错误 ACK 类型在 Go 编译期失败。
- 证据：`compile_contract_test.go` 编译正例，并要求反例真实编译失败；`runtime_test.go` 验证保留事件和运行时解码。

## 14 项逐项映射

| 官方编号 | 官方类型行为 | Go 等价证据 |
|---:|---|---|
| 1 | 无事件映射时，保留事件仍有确定参数 | `OnConnect` 为零参数；`OnConnectError` 接收 `error`；`OnDisconnect` 接收 `DisconnectReason`；编译契约和运行时测试共同验证。 |
| 2 | 普通事件监听参数为动态类型 | 核心 `types.EventListener` 是 `func(...any)`。 |
| 3 | enum 事件名的普通监听参数仍为动态类型 | Go 可用自定义 string 类型表达事件名，核心 listener 仍接收 `...any`。 |
| 4 | 无事件映射时 `emit` 接受任意参数 | 核心 `Socket.Emit` 接收 `...any`。 |
| 5 | 无事件映射时 `emitWithAck` 接受任意参数 | 核心 `Socket.EmitWithAck` 保持动态请求和响应表面。 |
| 6 | 单一事件映射推导监听参数 | `typed.On` 从 `Event[Request, Response]` 推导 Request；生成器产生命名的 `OnX`/`HandleX`。 |
| 7 | 错误监听参数必须拒绝 | 负向编译 fixture 的错误 handler 类型无法编译。 |
| 8 | 正确发送参数必须接受 | `typed.Emit` 和生成的 `EmitX` 接收声明的 Request。 |
| 9 | 错误发送参数必须拒绝 | 负向编译 fixture 传入错误 Request，Go 编译器拒绝。 |
| 10 | 分离 listen/emit map 后仍推导监听参数 | `ServerToClient` 只在客户端生成强类型 `OnX`/`HandleX`。 |
| 11 | 发送方向事件不能当作监听事件 | `ClientToServer` 不生成客户端 `OnX`；负向 fixture 必须得到 `undefined: OnOutgoing`。 |
| 12 | 分离 map 后正确发送参数必须接受 | `ClientToServer` 生成强类型 `EmitX`，正向 fixture 编译通过。 |
| 13 | 接收方向事件和错误参数不能发送 | `ServerToClient` 不生成客户端 `EmitX`，错误请求也不能编译；负向 fixture 同时验证。 |
| 14 | `emitWithAck` 推导请求和 ACK 类型 | ACK 事件生成 `EmitX(context.Context, Emitter, Request) (Response, error)`；正例接收 `bool`，把结果赋给 `string` 的反例必须编译失败。 |

## 运行验证

```bash
cd typed
go test -mod=mod -race ./... -count=1
```

当前结论：14/14 均有明确的 Go API 等价和编译/运行证据；核心动态 API 与可选强类型层的边界必须分别理解。
