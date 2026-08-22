# @socket.io/valkey-adapter（Go）

[Socket.IO](https://socket.io/) 的 Go 语言 Valkey 适配器。本模块通过 Valkey
Pub/Sub、Valkey 分片 Pub/Sub（Valkey 7+）及 Valkey Streams 提供 Socket.IO
集群能力；功能与 `adapters/redis` 模块一致，但使用
[`valkey-go`](https://github.com/valkey-io/valkey-go) 客户端。

## 适配器类型

| 类型 | 说明 |
|---|---|
| `ValkeyAdapterBuilder` | 经典 Pub/Sub——适用于单机和主从复制 Valkey |
| `ShardedValkeyAdapterBuilder` | 分片 Pub/Sub（SSUBSCRIBE/SPUBLISH）——适用于 Valkey Cluster |
| `ValkeyStreamsAdapterBuilder` | Valkey Streams——支持消息持久化与会话恢复 |

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/valkey/v4
```

## 用法

### 经典 Pub/Sub 适配器

```go
import (
    "context"

    vk "github.com/valkey-io/valkey-go"
    io "github.com/aqcool/socket.io/servers/socket/v4"
    vkadapter "github.com/aqcool/socket.io/adapters/valkey/v4/adapter"
    valkey "github.com/aqcool/socket.io/adapters/valkey/v4"
)

client, err := vk.NewClient(vk.ClientOption{
    InitAddress: []string{"localhost:6379"},
})
if err != nil {
    log.Fatal(err)
}

valkeyClient := valkey.NewValkeyClient(context.Background(), client)

server := io.NewServer(nil, nil)
server.SetAdapter(&vkadapter.ValkeyAdapterBuilder{Valkey: valkeyClient})
```

### 分片 Pub/Sub 适配器（Valkey Cluster）

```go
server.SetAdapter(&vkadapter.ShardedValkeyAdapterBuilder{Valkey: valkeyClient})
```

### Streams 适配器

```go
server.SetAdapter(&vkadapter.ValkeyStreamsAdapterBuilder{Valkey: valkeyClient})
```

### 复用现有客户端

将预先创建的 `vk.Client` 传给 `NewValkeyClient`，即可共享现有连接池，
无需再创建一个连接池：

```go
// existing client created elsewhere in your application
existingClient := myAppValkeyClient

valkeyClient := valkey.NewValkeyClient(ctx, existingClient)
server.SetAdapter(&vkadapter.ValkeyAdapterBuilder{Valkey: valkeyClient})
```

### 发射器

使用发射器可从未运行 Socket.IO 服务端的进程广播事件：

```go
import (
    "context"

    vk "github.com/valkey-io/valkey-go"
    valkey "github.com/aqcool/socket.io/adapters/valkey/v4"
    "github.com/aqcool/socket.io/adapters/valkey/v4/emitter"
)

client, _ := vk.NewClient(vk.ClientOption{InitAddress: []string{"localhost:6379"}})
valkeyClient := valkey.NewValkeyClient(context.Background(), client)

e := emitter.NewEmitter(valkeyClient, nil)
e.To("room1").Emit("hello", "world")
```

## 配置

### ValkeyAdapterOptions

| 选项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `Key` | `string` | `"socket.io"` | 频道前缀 |
| `RequestsTimeout` | `time.Duration` | `5000ms` | 节点间请求超时 |
| `PublishOnSpecificResponseChannel` | `bool` | `false` | 将响应路由到各节点专用频道 |
| `Parser` | `valkey.Parser` | MsgPack | 消息编解码器 |

### ShardedValkeyAdapterOptions

| 选项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `ChannelPrefix` | `string` | `"socket.io"` | 频道前缀 |
| `SubscriptionMode` | `SubscriptionMode` | `DynamicSubscriptionMode` | 频道策略 |

### ValkeyStreamsAdapterOptions

| 选项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `StreamName` | `string` | `"socket.io"` | Stream 名称 |
| `MaxLen` | `int64` | `10000` | Stream 近似最大长度 |
| `ReadCount` | `int64` | `100` | 每次 XREAD 调用读取的消息数 |
| `SessionKeyPrefix` | `string` | `"sio:session:"` | 会话键前缀 |

## 订阅模式

| 模式 | 说明 |
|---|---|
| `StaticSubscriptionMode` | 每个命名空间使用 2 个固定频道 |
| `DynamicSubscriptionMode` | 2 个固定频道，加上每个公开房间 1 个频道（默认） |
| `DynamicPrivateSubscriptionMode` | 每个房间使用独立频道（包括私有房间） |

## 许可证

MIT
