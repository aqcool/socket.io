# Admin UI 与运行时管理

`instrumentation` 模块实现了官方 `@socket.io/admin-ui@0.5.1` 使用的协议，可以查看节点、Namespace、Socket、Room 和实时事件，也可以对本机或 Adapter 集群中的 Socket 执行管理操作。锁定的官方 `socket.io-client@4.8.3` 已通过真实 EIO 4/WebSocket 端到端矩阵验证配置、统计、Socket 生命周期、事件、管理命令和认证 session 恢复。

## 接入

```go
import (
    "os"

    instrumentation "github.com/aqcool/socket.io/instrumentation/v4"
    socket "github.com/aqcool/socket.io/servers/socket/v4"
    "golang.org/x/crypto/bcrypt"
)

io := socket.NewServer(nil, nil)
passwordHash, _ := bcrypt.GenerateFromPassword([]byte(os.Getenv("ADMIN_PASSWORD")), bcrypt.DefaultCost)

admin, err := instrumentation.Instrument(io, &instrumentation.Options{
    ServerID: "node-shanghai-1",
    BasicAuth: &instrumentation.BasicAuth{
        Username:     "admin",
        PasswordHash: string(passwordHash),
    },
})
if err != nil {
    panic(err)
}
defer admin.Close()
```

然后打开[官方 Socket.IO Admin UI](https://admin.socket.io/)，填写服务地址、用户名和密码。默认管理 Namespace 是 `/admin`。

Basic Auth 凭据会通过 Socket.IO 握手发送，生产环境必须使用 HTTPS/WSS，并应从密钥管理系统或环境变量读取密码，不要写入源码。`PasswordHash` 接受 bcrypt hash，与官方实现一致；`Password` 明文字段仅为向后兼容保留。

认证成功后服务端发送官方 `session` 事件，UI 重连时可只提交 `sessionId`。默认 session 仅保存在当前进程；多节点部署应实现 `instrumentation.SessionStore` 并通过 `Options.SessionStore` 注入共享存储。

模块已提供与官方 `RedisStore` 等价的共享实现，默认键前缀为 `socket.io-admin`，默认TTL为24小时：

```go
redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
sessionStore, err := instrumentation.NewRedisStore(redisClient, nil)
if err != nil {
    panic(err)
}

admin, err := instrumentation.Instrument(io, &instrumentation.Options{
    SessionStore: sessionStore,
})
```

可通过 `RedisStoreOptions.Prefix`、`SessionDuration` 和 `Context` 覆盖前缀、TTL与操作上下文。官方21项源码测试的逐项映射见[Admin UI官方测试映射](../instrumentation/OFFICIAL_TEST_MAPPING.md)。

## 自定义认证

`Auth` 接受与普通 Namespace 相同的中间件。设置 Basic Auth 和自定义认证时，两者都会执行：

```go
admin, err := instrumentation.Instrument(io, &instrumentation.Options{
    Auth: func(s *socket.Socket, next func(*socket.ExtendedError)) {
        token, _ := s.Handshake().Auth["token"].(string)
        if !validateToken(token) {
            next(socket.NewExtendedError("无权访问管理界面", nil))
            return
        }
        next(nil)
    },
})
```

## 只读和生产模式

```go
admin, err := instrumentation.Instrument(io, &instrumentation.Options{
    ReadOnly: true,
    Mode:     instrumentation.ProductionMode,
})
```

- `ReadOnly` 不注册 emit、join、leave 和 disconnect 管理命令。
- `DevelopmentMode` 提供 Socket 快照和逐条收发事件。
- `ProductionMode` 仅发送低开销的节点统计和聚合事件。
- `StatsInterval` 默认是 2 秒，可以按监控负载调整。

## 集群行为

`all_sockets` 通过各 Namespace 的 `FetchSockets` 聚合，远程加入 Room、离开 Room 和断开连接通过 `BroadcastOperator` 执行。因此配置 Redis、MongoDB、PostgreSQL、Valkey 或其他集群 Adapter 后，管理命令和快照会沿用该 Adapter 的跨节点能力。

每个节点都会广播自己的 `server_stats`，`serverId` 应在集群内唯一。未配置时使用主机名。

## 官方协议范围

模块实现以下官方事件：

- 服务端事件：`config`、`server_stats`、`all_sockets`、`socket_connected`、`socket_updated`、`socket_disconnected`、`room_joined`、`room_left`、`event_received` 和 `event_sent`
- 管理命令：`emit`、`join`、`leave` 和 `_disconnect`
- 能力声明：`EMIT`、`JOIN`、`LEAVE`、`DISCONNECT`、`MJOIN`、`MLEAVE`、`MDISCONNECT`、`AGGREGATED_EVENTS` 和 `ALL_EVENTS`

官方 UI 会根据 `config.supportedFeatures` 隐藏当前模式不允许的操作。

`server_stats.aggregatedEvents` 与官方一致统计 `rawConnection`、`rawDisconnection`、`packetsIn`、`packetsOut`、`bytesIn` 和 `bytesOut`。真实互操作可本地复跑：

```bash
(cd instrumentation/testdata/official-admin-ui && npm ci)
cd instrumentation
SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP=1 \
  go test -mod=mod -race . -run TestOfficialAdminUI051ProtocolInterop -v
```
