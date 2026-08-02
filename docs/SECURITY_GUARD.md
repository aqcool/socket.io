# 安全与流量控制 Guard

`servers/socket/guard` 提供统一的握手、用户连接、事件、Namespace 和 Room 限制。所有限制默认关闭，只有大于零的配置才生效。

```go
import (
    "time"

    "github.com/aqcool/socket.io/servers/socket/v3/guard"
)

protection, err := guard.New(io, &guard.Options{
    HandshakeRate: guard.RateLimit{
        Limit:  30,
        Window: time.Minute,
    },
    MaxConnectionsPerUser: 5,
    SocketEventRate: guard.RateLimit{
        Limit:  100,
        Window: time.Second,
    },
    NamespaceEventRate: guard.RateLimit{
        Limit:  10_000,
        Window: time.Second,
    },
    EventRates: map[string]guard.RateLimit{
        "message:send": {Limit: 20, Window: time.Second},
    },
    MaxRoomsPerSocket:   50,
    MaxRoomMembers:      1_000,
    Action:              guard.Reject,
    ClusterQueryTimeout: time.Second,
})
if err != nil {
    panic(err)
}
```

## 限制范围

- 握手速率按客户端 IP 统计。
- 用户连接数默认读取 `data.userId`，其次读取握手 `auth.userId`。
- 用户连接上限通过内部用户 Room 和 `CountSockets` 在集群范围统计，并用本机 reservation 防止并发握手穿透限制。
- 事件速率可以按 Socket、Namespace 和事件名分别配置。
- Socket 的 Room 数不包含自己的私有 Room 和 `\x00` 开头的内部 Room。
- Room 最大成员数使用集群级低成本计数，不调用完整 `FetchSockets`。

可以通过 `Options.UserID` 自定义用户 ID 解析。

## 违规策略

- `guard.Reject`：拒绝事件或加入 Room。
- `guard.Throttle`：把超限事件延迟到当前限流窗口后继续处理，最大延迟由 `MaxThrottleDelay` 控制。
- `guard.Disconnect`：拒绝并主动断开违规 Socket。

握手、用户连接和不可恢复的 Room 硬容量限制会直接拒绝；Throttle 主要用于事件速率。

应用主动加入 Room 时可以读取错误：

```go
if err := socket.TryJoin("room-1"); err != nil {
    // 已达到 Socket Room 数或 Room 成员上限。
}
```

原有 `socket.Join` 保持兼容：验证失败时不会加入，但不返回错误。

## 审计事件

Guard 和 Server 都会发出 `audit`：

```go
protection.On("audit", func(args ...any) {
    event := args[0].(guard.AuditEvent)
    auditLogger.Write(event)
})
```

`AuditEvent` 包含规则、动作、Namespace、Socket ID、用户、IP、事件名、Room、限制值、重试时间和时间戳，可直接接入日志、SIEM、metrics 或告警系统。
