# Presence 与 Room 统计

`servers/socket/presence` 包提供低开销、支持集群 Adapter 的在线状态与房间统计。计数和房间列表使用 Adapter 的聚合协议，只传输数字和 Room 映射；不会为了简单计数序列化完整的 handshake、rooms 和 data。

## 启用

```go
import (
    socket "github.com/aqcool/socket.io/servers/socket/v3"
    "github.com/aqcool/socket.io/servers/socket/v3/presence"
)

io := socket.NewServer(nil, nil)
tracker, err := presence.New(io, &presence.Options{
    EmptyRoomTTL: 10 * time.Minute,
})
if err != nil {
    panic(err)
}
defer tracker.Close()
```

默认从 `socket.Data()["userId"]` 读取用户 ID，其次读取握手的 `auth.userId`。连接会自动加入一个内部用户 Room，因此同一用户的多个浏览器、移动端和设备可以同时在线。

认证中间件应在连接完成前写入用户数据：

```go
io.Use(func(s *socket.Socket, next func(*socket.ExtendedError)) {
    s.SetData(map[string]any{"userId": authenticatedUserID})
    next(nil)
})
```

自定义字段可以使用 `presence.DataUserID("accountId")`，复杂模型可以直接提供 `Options.UserID`。

## 查询

所有接口沿用项目的异步回调风格：

```go
tracker.CountSockets("/chat")(func(count uint64, err error) {})
tracker.CountRoom("/chat", "support")(func(count uint64, err error) {})
tracker.IsUserOnline("/chat", "user-42")(func(online bool, err error) {})

tracker.UserSockets("/chat", "user-42")(
    func(sockets []*socket.RemoteSocket, err error) {
        // 只有此接口会获取完整 Socket 详情。
    },
)

tracker.ListRooms("/chat")(func(rooms []presence.RoomStats, err error) {})
tracker.RoomStats("/chat", "support")(func(stats presence.RoomStats, err error) {})
```

`CountSockets`、`CountRoom` 和 `ListRooms` 在 Redis、Valkey、MongoDB、PostgreSQL、Unix 等集群 Adapter 上由各节点返回紧凑结果，再在调用节点聚合。

## 人数变化事件

```go
tracker.On("room_count_changed", func(args ...any) {
    change := args[0].(presence.RoomCountChange)
    // change.Namespace、Room、Count、Delta、At
})
```

私有 Socket ID Room 和内部用户映射 Room 不会产生业务人数事件。可以用 `Options.RoomFilter` 进一步排除业务不关心的 Room。

## Room 元数据与空 Room TTL

```go
err := tracker.SetRoomMetadata("/chat", "support", map[string]any{
    "title": "在线支持",
    "tier":  "premium",
})

err = tracker.DeleteRoomMetadata("/chat", "support")
```

元数据通过 Adapter 的 server-side emit 同步到其他节点。Room 变空后仍会在 `ListRooms` 中保留到 `EmptyRoomTTL` 到期，并带有 `EmptySince` 和 `ExpiresAt`。到期后会触发 `room_expired` 事件并删除本地记录。设置 `EmptyRoomTTL: 0` 可以禁用空 Room 保留。

元数据必须是当前 Parser 可序列化的值。若业务要求进程重启后仍保留元数据，应在 `room_count_changed` 和 `room_expired` 事件中同步到自己的持久化存储。
