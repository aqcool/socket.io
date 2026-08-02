# 服务端可靠投递与离线消息

`reliability` 模块用于需要跨进程重启、跨节点或长期离线后继续投递的业务事件。它与 Socket.IO 内置连接状态恢复互补：

- 连接状态恢复处理几分钟内的临时网络中断，并恢复 Socket ID、Room、Data 和遗漏包。
- `EventStore` 使用稳定的 `clientId`/`userId` 保存业务事件，适合长期离线、服务重启和跨节点重连。

可靠投递采用至少一次语义。服务端先持久化，再发送；客户端处理后提交 offset。若处理完成但 ACK 丢失，事件可能再次出现，因此业务写操作仍应使用事件 ID 保证幂等。

## 服务端

```go
import (
    "context"
    "time"

    "github.com/aqcool/socket.io/reliability/v3"
    socket "github.com/aqcool/socket.io/servers/socket/v3"
    "github.com/aqcool/socket.io/servers/socket/v3/presence"
)

io := socket.NewServer(nil, nil)

// User 目标通过 Presence 的内部用户 Room 投递。
tracker, _ := presence.New(io, nil)
defer tracker.Close()

store := reliability.NewMemoryStore()
delivery, _ := reliability.New(io, store, &reliability.Options{
    Limits: reliability.Limits{
        Retention: 7 * 24 * time.Hour,
        MaxEvents: 1_000_000,
        MaxBytes:  2 << 30,
    },
})
defer delivery.Close()

_, err := delivery.Emit(context.Background(), reliability.Target{
    Kind:      reliability.TargetUser,
    Namespace: "/",
    ID:        "user-42",
}, "order:updated", map[string]any{"orderId": "A100"})
```

支持 `TargetNamespace`、`TargetRoom`、`TargetSocket` 和 `TargetUser`。长期离线应优先使用稳定的 User 目标；Socket ID 主要用于同一会话恢复。

客户端认证数据至少应包含稳定的 `clientId`，一个用户多设备时每台设备使用不同值：

```go
options.SetAuth(map[string]any{
    "clientId": "phone-8f14",
    "userId":   "user-42",
    // 重连时需要继续接收这些 Room 的离线事件。
    "reliableRooms": []string{"orders:user-42"},
})
```

也可以通过 `Options.ClientID`、`Options.UserID` 和 `Options.ReplayTargets` 自定义身份与重放目标解析。

## Go 客户端

```go
client, _ := socketclient.Connect(serverURL, options)
reliableClient, _ := reliability.BindClient(client)

client.On("order:updated", func(args ...any) {
    // 监听器返回后会自动提交最新 offset。
})

// 相同 ID 的客户端消息只会在服务端分发一次。
_ = reliableClient.Emit("request-20260726-1", "order:create", request)
```

传入空 ID 时会自动生成唯一 ID。主动重试业务请求时应复用原 ID。

## 持久化后端

Redis Streams：

```go
store, _ := rediseventstore.New(redisClient, "socket.io:reliability")
```

PostgreSQL：

```go
store, _ := postgreseventstore.New(ctx, postgresClient, "socket_io_reliability")
```

MongoDB：

```go
store, _ := mongoeventstore.New(ctx, mongoClient, "socket_io_reliability")
```

三种实现都保存事件、客户端 ACK 和客户端上行消息去重记录，并执行过期时间、最大事件数和最大字节数清理。Redis 使用 Stream ID，PostgreSQL 使用递增 `BIGSERIAL`，MongoDB 使用递增时间排序的 ObjectID 作为 offset。

## 协议事件

模块内部使用以下保留事件：

- `__socketio_reliable_event`：服务端投递事件信封。
- `__socketio_reliable_ack`：客户端提交已处理 offset。
- `__socketio_reliable_publish`：带幂等 ID 的客户端消息。

应用代码不应直接监听或发送这些事件。
