# Go 集群部署

本项目对齐官方 Socket.IO 4.8.3 的三种集群组件，但把 Engine.IO 会话转发与 Socket.IO 广播明确分层：

| 官方组件 | 官方版本 | Go实现及作用层 |
|---|---:|---|
| `@socket.io/cluster-adapter` | 0.3.0 | `ClusterAdapterWithHeartbeat` 配合Unix、Redis等Adapter，负责Socket.IO广播、Room、ACK和跨节点管理 |
| `@socket.io/cluster-engine` | 0.1.0 | `engine.ClusterServer` 配合 `ClusterBus`，负责Engine.IO锁、Polling包转发、关闭和Upgrade接管 |
| `@socket.io/sticky` | 2.0.1 | 可选的 `sticky.Router`，以random、round-robin或least-connection分配新会话，并把后续 `sid` 请求送回owner |

Cluster Engine 18/18已经完成分类与映射：官方1–12项进程内精确行为、13–14项无粘性三实例部署等价行为，以及15–18项真实Redis线路与官方Node `RedisEngine` 双向直接互操作均已通过高频 `-race` 门禁。

## 方案一：ClusterServer包转发

该方案允许同一Polling会话的后续请求到达任意共享Bus的监听器，不要求负载均衡器保持 `sid` 亲和。收到请求的 `ClusterServer` 会向owner申请读锁或写锁，并转发Packet、Drain、Close和Upgrade消息。

### 同一进程内的多个监听器

`MemoryClusterBus` 适合在同一Go进程中运行多个HTTP监听器，也用于确定性测试：

```go
bus := engine.NewMemoryClusterBus()

engineOptions := config.DefaultServerOptions()
engineOptions.SetPath("/socket.io")
cluster, err := engine.NewClusterServer(bus, engineOptions, nil)
if err != nil {
    log.Fatal(err)
}

io := socket.NewServer(nil, socket.DefaultServerOptions())
io.Bind(cluster)
log.Fatal(http.ListenAndServe(":3001", cluster))
```

同一进程的其他监听器应各自创建 `ClusterServer`，共享这个Bus，并各自绑定一个Socket.IO Server。`MemoryClusterBus` 只在当前进程内传递消息，不能跨进程或跨主机；它为每个订阅者维护异步队列，对有序 `Publish` 调用保持FIFO，且不会在当前调用栈内重入listener。自定义 `ClusterBus` 也必须遵守Context取消、非同步重入，以及对有序发布保持每订阅者FIFO的契约。

### Redis跨进程或跨主机

跨进程时使用 `adapters/redis/enginebus`。每个进程创建自己的订阅连接，并把Bus交给 `NewClusterServer`：

```go
pubClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
subClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

bus, err := enginebus.New(pubClient, subClient, &enginebus.Options{
    ChannelPrefix: "engine.io",
})
if err != nil {
    log.Fatal(err)
}

engineOptions := config.DefaultServerOptions()
engineOptions.SetPath("/socket.io")
cluster, err := engine.NewClusterServer(bus, engineOptions, nil)
if err != nil {
    log.Fatal(err)
}

io := socket.NewServer(nil, socket.DefaultServerOptions())
io.Bind(cluster)
log.Fatal(http.ListenAndServe(":3001", cluster))
```

`enginebus` 使用与 `@socket.io/cluster-engine@0.1.0` 相同的MessagePack envelope：广播Channel为 `<prefix>#`，定向Channel为 `<prefix>#<recipient>#`，并用 `_source: "_eio"` 隔离其他Redis消息。它与Socket.IO Redis Adapter不是同一层；分布式Socket.IO应用通常同时需要 `enginebus` 处理Engine.IO会话，以及Redis/Streams等Adapter处理广播和Room。

`ClusterServer` 默认生成20字符base64url SID。若通过 `SetGenerateId` 自定义SID，跨Go实例或与Node混部时仍必须返回20字符，并满足本项目 `utils.IsValidSid` 的安全字符集，建议继续使用base64url。官方Cluster Engine对未知SID只检查长度20；包含特殊符号的20字符SID可能被官方Node路由，但会被Go以400拒绝，这是刻意的安全加严和已知差异。其他长度的自定义SID只保证本地owner处理，不提供跨运行时路由承诺。

## 方案二：sticky.Router owner-affinity

不需要任意节点接管会话时，可以让入口始终把同一 `sid` 送回owner。该方案仍是有效的低开销部署替代，但不执行Cluster Engine的包级转发，其Sticky 10项证据不计入Cluster Engine 18项。

单机多进程时，每个业务进程可以使用相同的Unix Socket基础路径，并监听独立的内部HTTP地址：

```go
ctx := context.Background()
unixClient := unix.NewUnixClient(ctx, "/run/socket.io/cluster.sock")

adapterOptions := unixadapter.DefaultUnixAdapterOptions()
serverOptions := socket.DefaultServerOptions()
serverOptions.SetAdapter(&unixadapter.UnixAdapterBuilder{
    Unix: unixClient,
    Opts: adapterOptions,
})

io := socket.NewServer(nil, serverOptions)
http.ListenAndServe("127.0.0.1:3001", io.ServeHandler(nil))
```

入口进程注册这些内部地址：

```go
router, _ := sticky.New(sticky.Options{
    LoadBalancingMethod: sticky.LeastConnection,
})
first, _ := url.Parse("http://127.0.0.1:3001")
second, _ := url.Parse("http://127.0.0.1:3002")
router.AddBackend("worker-1", first)
router.AddBackend("worker-2", second)
http.ListenAndServe(":3000", router)
```

进程被摘流或退出时调用 `RemoveBackend`；新进程健康检查通过后调用 `AddBackend`。业务进程间的广播、Room、ACK和管理操作由Unix Adapter负责，Engine.IO Polling与Upgrade会话归属由Router负责。

## 跨主机选择

- 需要任意节点接收同一会话请求时，使用Redis `enginebus`，入口不需要按 `sid` 保持亲和；Socket.IO层仍另选Redis、Redis Streams、MongoDB或PostgreSQL Adapter。
- 选择owner-affinity时，可继续使用 `sticky.Router`，或在Nginx、HAProxy、Ingress等入口配置Cookie/`sid`会话亲和；Socket.IO层同样需要一个跨主机Adapter。

## 与Node公共API的边界

Go实现直接兼容官方Node `RedisEngine` 的0.1.0线协议，但不复制Node运行时拓扑：

- 没有 `setupPrimary()`、`NodeClusterEngine` 或 `setupPrimaryWithRedis()` API；
- 不实现Node primary/worker IPC relay、TCP句柄传递、类继承或 `_primaryId` worker映射；
- 官方13–14项以三个Go实例的无粘性round-robin验证客户端可观察行为，不声称执行了Node OS worker/IPC路径；
- 官方15–18项验证的是直接 `RedisEngine` 线路互操作，不等于支持 `setupPrimaryWithRedis()` 的Node进程组织方式。
- 官方 `engine.io@6.6.9` 和 `@socket.io/cluster-engine@0.1.0` 没有把WebTransport session接到跨节点owner查找，Go当前也不提供跨节点WebTransport接管；transport枚举出现 `webtransport` 不代表存在可用入口，这是共同边界。
- 官方Cluster Engine 0.1.0的快速接管路径固定以Engine.IO协议版本4创建新Socket，Go保持相同行为；不要把项目普通连接的EIO 3/Socket.IO v2兼容声明外推到跨节点快速接管。

## 验证

Cluster Engine 1–14项可在不启动Redis的情况下复现：

```bash
(cd servers/engine && \
  go test -race . \
    -run 'TestOfficialClusterEngine010(InMemory|NodeCluster)$' \
    -count=10)
```

Redis Go线路和官方Node双向互操作使用专用Redis实例：

```bash
(cd adapters/redis/enginebus/testdata/official-interop && npm ci)

(cd adapters/redis && \
  SOCKET_IO_CLUSTER_ENGINE_REDIS_ADDR='127.0.0.1:<dedicated-port>' \
  SOCKET_IO_CLUSTER_ENGINE_REDIS_PASSWORD='root' \
  go test -mod=mod -race ./enginebus \
    -run '^TestOfficialClusterEngine010Redis$' -count=10 -v)

(cd adapters/redis && \
  SOCKET_IO_CLUSTER_ENGINE_OFFICIAL_INTEROP=1 \
  SOCKET_IO_CLUSTER_ENGINE_REDIS_ADDR='127.0.0.1:<dedicated-port>' \
  SOCKET_IO_CLUSTER_ENGINE_REDIS_PASSWORD='root' \
  go test -mod=mod -race ./enginebus \
    -run '^TestOfficialNodeClusterEngine010RedisInterop$' -count=10 -v)
```

Redis 15–18项和官方Node双向互操作的高频门禁已经通过。未设置显式环境变量时真实Redis或Node矩阵会跳过，因此普通 `go test` 的通过仍不能代替该互操作结论。

Cluster Adapter 18项和Sticky owner-affinity部署使用另一组Unix多进程测试：

```bash
(cd adapters/unix/testdata/cluster-official && npm ci)
cd adapters/unix
SOCKET_IO_CLUSTER_OFFICIAL_INTEROP=1 \
  go test ./adapter -run TestOfficialClusterDeploymentEquivalence -v
```

每周3小时持续验收覆盖72个官方客户端持续ACK与广播、主动频繁重连、20分钟后端网络分区、进程滚动替换，并在协议关闭窗口结束后检查客户端、goroutine和堆内存是否回落。这是独立的稳定性门禁，不计入Cluster Engine 18项或1004项源行分母。首次完整验收已通过，结果见 [2026-08-01集群3小时稳定性验收记录](soak/2026-08-01.md)。

本地短时检查可使用：

```bash
SOCKET_IO_CLUSTER_SOAK=1 \
SOCKET_IO_SOAK_DURATION=30s \
SOCKET_IO_SOAK_PARTITION_DURATION=6s \
SOCKET_IO_SOAK_CLIENTS=24 \
  go test -mod=mod ./adapter -run TestOfficialClusterSoakAndResourceLeaks -v -timeout=2m
```
