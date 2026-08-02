# Engine.IO：Go 实时通信引擎

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/engine/v3)

## 概述

Engine.IO 是 [Socket.IO Go 实现](../socket)基于传输层的跨浏览器、跨设备双向通信层。它屏蔽 WebSocket、Polling、WebTransport 等传输方式的差异，并提供统一 API。

## 特性

- 支持多种传输方式（WebSocket、Polling、WebTransport）
- 自动升级传输方式
- 带心跳机制的有状态连接
- 二进制数据支持
- 跨浏览器兼容
- 支持 Engine.IO 协议 v3 和 v4
- 可选 `ClusterServer`，支持跨监听器读写锁、Polling包转发、关闭和Upgrade接管

## 安装

```bash
go get github.com/aqcool/socket.io/servers/engine/v3
```

## 快速开始

```go
package main

import (
    "github.com/aqcool/socket.io/servers/engine/v3"
    "github.com/aqcool/socket.io/servers/engine/v3/config"
    "github.com/aqcool/socket.io/v3/pkg/types"
)

func main() {
    // Configure server options
    serverOptions := &config.ServerOptions{}
    serverOptions.SetAllowEIO3(true)
    serverOptions.SetCors(&types.Cors{
        Origin:      "*",
        Credentials: true,
    })

    // Create and start server
    server := engine.Listen(":4444", serverOptions, nil)

    // Handle connections
    server.On("connection", func(sockets ...any) {
        socket := sockets[0].(engine.Socket)
        socket.On("message", func(args ...any) {
            // Handle messages
        })
    })

    // Keep the server running
    select {}
}
```

## 用法

### 服务端初始化方式

1. **直接监听**
2. **集成 HTTP 服务端**
3. **自定义请求处理**
4. **集成 WebSocket**
5. **WebTransport 支持**

## 配置

### 服务端选项

```go
opts := &config.ServerOptions{}
opts.SetPingTimeout(20_000 * time.Millisecond)
opts.SetPingInterval(25_000 * time.Millisecond)
opts.SetUpgradeTimeout(10_000 * time.Millisecond)
opts.SetMaxHttpBufferSize(1e6)
opts.SetCookie(&http.Cookie{}) // 默认 io=<sid>; Path=/; HttpOnly; SameSite=Lax
opts.SetCookiePath("")        // 显式省略 Path，等价于官方 cookie.path=false
opts.SetCookieHttpOnly(false) // 显式关闭 HttpOnly
opts.SetGenerateId(func(ctx *types.HttpContext) (string, error) {
    return createSessionID(ctx) // 返回错误会拒绝握手并触发 ID_GENERATION_ERROR
})
// ...
```

普通服务端可以使用任意满足基础校验的自定义SID。若这个选项交给 `ClusterServer`，跨Go实例或与Node混部的SID必须是20字符，并满足本项目 `utils.IsValidSid` 的安全字符集，建议使用base64url。官方Cluster Engine对未知SID只检查长度20；包含特殊符号的20字符SID可能被官方Node路由，但会被Go以400拒绝，这是刻意的安全加严和已知差异。

## ClusterServer

`ClusterServer` 允许同一Engine.IO会话的后续请求到达其他监听器。它会在共享Bus上申请读锁或写锁，并转发Packet、Drain、Close、Upgrade及升级前缓冲，不依赖粘性会话：

```go
bus := engine.NewMemoryClusterBus()
defer bus.Close()

cluster, err := engine.NewClusterServer(bus, opts, &engine.ClusterOptions{
    ResponseTimeout:  time.Second,
    NoopUpgradeInterval: 200 * time.Millisecond,
})
if err != nil {
    log.Fatal(err)
}
defer cluster.Close()

cluster.On("cluster_error", func(values ...any) {
    log.Printf("cluster error: %v", values[0])
})
log.Fatal(http.ListenAndServe(":4444", cluster))
```

每个监听器都需要自己的 `ClusterServer` 订阅。`MemoryClusterBus` 可以由同一进程内的多个监听器共享，但不能跨进程；它按订阅者异步交付，对有序 `Publish` 调用保持FIFO，且不会从 `Publish` 调用栈同步重入listener。自定义 `ClusterBus` 必须同样响应Context取消、避免同步重入，并对有序发布维持每订阅者FIFO。跨进程或跨主机请使用 [`adapters/redis/enginebus`](../../adapters/redis/README.md)，它实现官方 `@socket.io/cluster-engine@0.1.0` 的Redis MessagePack与Channel线路。

`ClusterServer` 默认生成20字符base64url SID。自定义 `SetGenerateId` 若不满足“20字符且通过本项目SID安全字符校验”，只保证本地owner处理，不能作为跨Go实例或Node↔Go路由承诺。可用 `NodeID()` 识别当前节点，并通过 `PendingClusterRequests()`、`RemoteTransportCount()` 检查待响应请求和远端传输是否收敛。

官方测试证据分为三组：

- 1–12项由 `TestOfficialClusterEngine010InMemory` 验证远端读写、锁、关闭、心跳、非法SID和Upgrade；
- 13–14项由 `TestOfficialClusterEngine010NodeCluster` 在三个Go实例间无粘性round-robin验证心跳和二进制，属于部署行为等价，不是Node OS worker/IPC API复刻；
- 15–18项通过Redis Go线路及真实官方Node `RedisEngine` 的 `redis`/`ioredis`、Node/Go双owner矩阵建立直接互操作证据，并已通过高频 `-race` 门禁；Cluster Engine按18/18闭合。

Go直接兼容 `RedisEngine` 线协议，但不提供Node专属的 `setupPrimary()`、`NodeClusterEngine`、`setupPrimaryWithRedis()`、primary/worker IPC、进程句柄或类继承API。官方0.1.0与Go当前实现也都没有可用的跨节点WebTransport接管入口；内部枚举包含 `webtransport` 不代表该路径已接通。官方快速接管固定使用EIO 4，因此普通连接的EIO 3/Socket.IO v2兼容结论不外推到跨节点快速接管。完整口径见[官方测试对齐映射](../../docs/OFFICIAL_TEST_MAPPING.md)和[集群部署](../../docs/CLUSTER_DEPLOYMENT.md)。

## 传输方式实现

- **Polling**：XHR/JSONP 传输
- **WebSocket**：标准 WebSocket 传输
- **WebTransport**：实验性 WebTransport 支持

## 事件

### 服务端事件

- `connection`：新的客户端连接
- `connection_error`：连接错误
- `initial_headers`：每个连接仅在初始 Polling/直接 WebSocket 握手时触发，可修改响应头
- `headers`：每次 Polling 响应和 WebSocket 握手/升级时触发，可修改响应头
- `flush`：刷新缓冲区
- `drain`：缓冲区已清空

### Socket 事件

- `message`：收到消息
- `close`：连接已关闭
- `error`：发生错误
- `flush`：刷新写缓冲区
- `drain`：写缓冲区已清空
- `packet`：收到原始数据包
- `packetCreate`：发送数据包前
- `heartbeat`：收到 Ping/Pong

## 开发

### 前置条件

- Go 1.26.0+
- Make

### 测试

```bash
make test
```

### 调试

设置 `DEBUG` 环境变量：

```bash
DEBUG=engine*
```

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 许可证

采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE)。

## 支持

- [API 文档](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3)
- [问题跟踪](https://github.com/aqcool/socket.io/issues)
