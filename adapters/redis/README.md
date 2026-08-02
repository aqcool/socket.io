# socket.io-go-redis

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/redis/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/redis/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/redis/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/redis/v3)

## 简介

本模块既提供把Socket.IO广播、Room和集群操作扩展到多个进程或服务器的Redis Adapter，也提供供Engine.IO `ClusterServer` 使用的 `enginebus`。

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/redis/v3
```

## 特性

- 多服务器支持
- 进程间实时通信
- 自动重连
- 自定义 Redis 配置
- 跨服务器发送事件
- 与官方 `@socket.io/cluster-engine@0.1.0` Redis MessagePack/Channel线路互通的Engine.IO Bus

## 使用方法

基本用法示例：

```golang
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    rds "github.com/redis/go-redis/v9"
    "github.com/aqcool/socket.io/adapters/redis/v3"
    "github.com/aqcool/socket.io/adapters/redis/v3/adapter"
    "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
    // Initialize Redis client
    redisClient := redis.NewRedisClient(context.TODO(), rds.NewClient(&rds.Options{
        Addr:     "127.0.0.1:6379",
        Username: "",
        Password: "",
        DB:       0,
    }))

    // Redis error handling
    redisClient.On("error", func(a ...any) {
        fmt.Println(a)
    })

    // Socket.IO server configuration
    config := socket.DefaultServerOptions()
    config.SetAdapter(&adapter.RedisAdapterBuilder{
        Redis: redisClient,
        Opts:  &adapter.RedisAdapterOptions{},
    })

    // Create and configure server
    httpServer := s.CreateServer(nil)
    io := socket.NewServer(httpServer, config)

    // Handle socket connections
    io.On("connection", func(clients ...any) {
        client := clients[0].(*socket.Socket)
        client.On("event", func(datas ...any) {
            // Handle your events here
        })
        client.On("disconnect", func(...any) {
            // Handle disconnect
        })
    })

    // Start server
    httpServer.Listen("127.0.0.1:9000", nil)

    // Graceful shutdown handling
    exit := make(chan struct{})
    SignalC := make(chan os.Signal)

    signal.Notify(SignalC, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
    go func() {
        for s := range SignalC {
            switch s {
            case os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
                close(exit)
                return
            }
        }
    }()

    <-exit
    httpServer.Close(nil)
    os.Exit(0)
}
```

## 配置选项

Redis 适配器支持以下选项：

```golang
type RedisAdapterOptions struct {
    Prefix  string // Optional prefix for Redis keys
    // Add other available options here
}
```

## Engine.IO Cluster Engine Redis Bus

`github.com/aqcool/socket.io/adapters/redis/v3/enginebus` 实现 `engine.ClusterBus`，用于让不同进程或主机上的 `ClusterServer` 转发已建立Engine.IO会话的后续Polling、WebSocket Upgrade和Packet：

```go
pubClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
subClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

bus, err := enginebus.New(pubClient, subClient, &enginebus.Options{
    ChannelPrefix: "engine.io",
})
if err != nil {
    log.Fatal(err)
}

opts := config.DefaultServerOptions()
cluster, err := engine.NewClusterServer(bus, opts, nil)
if err != nil {
    log.Fatal(err)
}
defer cluster.Close()

log.Fatal(http.ListenAndServe(":4444", cluster))
```

默认前缀为 `engine.io`。Bus将广播发布到 `<prefix>#`，将定向消息发布到 `<prefix>#<recipient>#`，并使用 `_source: "_eio"` 与官方一致的MessagePack envelope。该协议与本模块的Socket.IO Redis Adapter不是同一种Redis消息；完整Socket.IO集群通常同时配置 `enginebus` 和一个Socket.IO Adapter。

互操作基准为官方 `@socket.io/cluster-engine@0.1.0`。当前Fixture会分别启动 `redis@4.6.15` 与 `ioredis@5.4.1` 驱动的真实官方Node `RedisEngine`，并在Node和Go分别拥有会话时验证心跳和二进制请求。这里证明的是直接 `RedisEngine` 线协议互通，不表示Go复制了Node的 `setupPrimary()`、`NodeClusterEngine`、`setupPrimaryWithRedis()`、primary/worker IPC relay或进程句柄传递。

`ClusterServer` 默认使用20字符base64url SID。自定义生成器用于跨实例或Node↔Go混部时，也必须返回20字符并满足本项目SID安全字符集，建议使用base64url。官方只检查未知SID长度20，而Go还会执行安全字符校验；特殊符号SID被Go以400拒绝是刻意的安全加严。

官方Redis 15–18项的实现、双向直接互操作及高频 `-race` 门禁均已通过，Cluster Engine按18/18闭合。复现前请只准备专用Redis实例：

```bash
cd enginebus/testdata/official-interop
npm ci
cd ../../..

SOCKET_IO_CLUSTER_ENGINE_OFFICIAL_INTEROP=1 \
SOCKET_IO_CLUSTER_ENGINE_REDIS_ADDR='127.0.0.1:<dedicated-port>' \
SOCKET_IO_CLUSTER_ENGINE_REDIS_PASSWORD='root' \
go test -mod=mod -race ./enginebus \
  -run '^TestOfficialNodeClusterEngine010RedisInterop$' -count=10 -v
```

若专用Redis没有密码，可显式设置 `SOCKET_IO_CLUSTER_ENGINE_REDIS_PASSWORD=''`。未设置 `SOCKET_IO_CLUSTER_ENGINE_OFFICIAL_INTEROP=1` 时，真实Node矩阵会跳过；普通 `go test` 的通过不能替代该互操作结论。

## 测试

运行测试套件：

```bash
make test
```

### 官方 Adapter 与 external emitter 互操作

真实互操作矩阵锁定以下官方版本：

- `@socket.io/redis-adapter@8.3.0`
- `@socket.io/redis-streams-adapter@0.3.1`
- `@socket.io/redis-emitter@5.1.0`（gitHead `bfefbdefd963b59279b7f55673a88296a660f7d8`）
- `@socket.io/redis-streams-emitter@0.1.1`（Socket.IO 4.8.3 单仓库版本，gitHead `54743633ffa7bd595cc759d4affc49121fedaeb4`）
- `socket.io@4.8.3` 与 `socket.io-client@4.8.3`

逐条官方断言、分母和证据见 [OFFICIAL_TEST_MAPPING.md](./OFFICIAL_TEST_MAPPING.md)。

覆盖口径：

- Redis Streams emitter：官方 12 项行为全部覆盖，包括广播、命名空间、Room/Except、三种 Join、三种 Leave、Disconnect 和 server-side emit。
- Redis Pub/Sub emitter：官方 16 项核心行为及 1 项自定义 JSON parser 行为全部覆盖，共 17/17；自定义 parser 使用独立频道做真实 Node↔Go 双向验证。
- Go `RedisStreamsEmitter` 使用与官方一致的 `XADD` 线路格式：普通数据为 JSON，二进制数据为 MessagePack 后 Base64。
- Sharded emitter 是本项目扩展；官方 `@socket.io/redis-emitter@5.1.0` 没有对应的 Sharded external emitter API，因此不计入官方 emitter 分母。

这些不是默认单元测试。测试会重置目标 Redis 的连接，请只指向专用测试实例：

```bash
cd testdata/interop
npm ci
cd ../..

SOCKET_IO_REDIS_INTEROP_TEST=1 \
SOCKET_IO_REDIS_TEST_ADDR='127.0.0.1:<dedicated-port>' \
SOCKET_IO_REDIS_TEST_PASSWORD='root' \
go test -race ./adapter -run TestOfficialNodeRedisAdapterInterop -v
```

若专用 Redis 没有密码，可显式设置 `SOCKET_IO_REDIS_TEST_PASSWORD=''`。未设置 `SOCKET_IO_REDIS_INTEROP_TEST=1` 时，该真实服务矩阵会跳过，普通 `go test` 的通过不能替代互操作结论。

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 支持

如果遇到问题或有任何疑问，请在 [Issue 区](https://github.com/aqcool/socket.io/issues)提交。

## 许可证

本项目采用 MIT 许可证，详情请参阅 LICENSE 文件。
