# Socket.IO 粘性会话路由器

本模块提供一个按Engine.IO `sid` 路由的HTTP/WebSocket反向代理，用于在同一入口后运行多个Go Socket.IO进程。它是官方 `@socket.io/sticky` 的Go部署等价方案，也可以与Unix、Redis等Cluster Adapter组合，通过owner-affinity提供连续会话。官方Cluster Engine式的包转发路径已经由 `engine.NewClusterServer` 提供；本模块是可选替代，不再是项目唯一的Engine.IO集群方案。

## 安装

```bash
go get github.com/aqcool/socket.io/sticky/v3
```

## 使用

```go
package main

import (
    "log"
    "net/http"
    "net/url"

    "github.com/aqcool/socket.io/sticky/v3"
)

func main() {
    router, err := sticky.New(sticky.Options{
        LoadBalancingMethod: sticky.LeastConnection,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer router.Close()

    first, _ := url.Parse("http://127.0.0.1:3001")
    second, _ := url.Parse("http://127.0.0.1:3002")
    if err := router.AddBackend("worker-1", first); err != nil {
        log.Fatal(err)
    }
    if err := router.AddBackend("worker-2", second); err != nil {
        log.Fatal(err)
    }

    log.Fatal(http.ListenAndServe(":3000", router))
}
```

新建连接会按 `random`、`round-robin` 或 `least-connection` 之一分配。Polling 握手响应中的 `sid` 会自动绑定到创建它的后端；后续 GET/POST 都回到同一后端。WebSocket Upgrade 由标准库反向代理透传。后端退出时调用 `RemoveBackend` 会同时删除它拥有的映射；健康检查和进程拉起由部署层负责。

## 与官方 Node 组件的关系

- 官方 `@socket.io/sticky` 在 Node Primary 中转交 TCP Socket；本模块在 Go 入口中反向代理 HTTP 和 WebSocket，外部路由语义相同。
- 官方 `@socket.io/cluster-engine` 允许不同Worker接收同一Polling会话，再把包转给会话拥有者；Go的对应实现是 `engine.NewClusterServer` 配合进程内 `MemoryClusterBus` 或Redis `enginebus`。本模块则直接把同一 `sid` 送回owner，避免额外的包级转发。
- Socket.IO 层的跨节点广播、Room、ACK、`fetchSockets` 和 `serverSideEmit` 仍需 Cluster Adapter。单机多进程可使用 `adapters/unix`，跨主机可使用 Redis、MongoDB 或 PostgreSQL Adapter。
- 本模块不伪装成 Node 的 `setupPrimary()`、`setupWorker()` 或 `NodeClusterEngine` API；Go 使用显式 Router 和 Backend 生命周期。

本模块的10项官方Sticky行为映射单独统计，不计入 `@socket.io/cluster-engine` 的18项分母。需要任意节点接收同一会话请求、跨节点read lock或直接与官方Node `RedisEngine` 混部时，应使用 `ClusterServer`；详细选择见[集群部署](../docs/CLUSTER_DEPLOYMENT.md)。

`SessionTTL` 只清理长时间没有 Polling 请求的遗留映射；正在使用 Polling 的客户端会持续刷新映射。纯 WebSocket 连接固定在一个反向代理连接上，无需后续 `sid` 查表。
