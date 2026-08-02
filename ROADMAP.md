# Socket.IO for Go 功能改进清单

本文档记录项目后续可增加或完善的功能，重点关注协议能力、生产可用性和 Go 生态体验，不包含代码格式、CI 优化等工程治理事项。

## 当前已有能力

- [x] Engine.IO v3/v4 协议支持
- [x] 严格校验 `EIO`，并验证官方 Socket.IO JavaScript v2/v3/v4 客户端兼容矩阵
- [x] HTTP long-polling、WebSocket、WebTransport
- [x] Namespace、动态 Namespace 和 Room
- [x] 单播、广播、排除房间和本地广播
- [x] 普通 ACK、广播 ACK 和超时
- [x] 客户端自动重连、重试和 `ackTimeout`
- [x] 客户端发送/接收缓冲区
- [x] 连接状态恢复基础能力
- [x] `fetchSockets`、`socketsJoin`、`socketsLeave` 和 `disconnectSockets`
- [x] `serverSideEmit` 和集群心跳
- [x] Redis、Redis Streams 和 Redis Sharded Pub/Sub Adapter
- [x] Valkey、Valkey Streams 和 Valkey Sharded Pub/Sub Adapter
- [x] MongoDB、PostgreSQL 和 Unix Domain Socket Adapter
- [x] Redis、Valkey、MongoDB、PostgreSQL 和 Unix 外部 Emitter
- [x] JSON、自定义 Parser 和 MessagePack
- [x] CORS、Cookie、压缩和自定义请求校验

## 对标官方 Socket.IO 4.8.3 的对齐状态

以下清单只计算与官方 Socket.IO/Engine.IO 及其非云生态组件的对齐程度，不包含 Google Cloud、AWS 和 Azure Adapter。上面的项目功能清单全部完成，不代表已经逐项通过官方仓库的全部行为测试。

严格运行时分母按锁定版本官方源码中的测试声明行统计，共1004项，当前1004/1004项已经完成分类/映射闭合。这个数字用于追踪官方源行，不代表存在1004个互不重复且原样执行的Go测试：部分证据会被多个官方断言复用，且分母中包含上游 `skip`、宿主N/A和平台等价项。编译期类型契约共36项（服务端22项、客户端14项）单独统计，不与1004项运行时分母相加。

- [x] 使用锁定版本的官方 JavaScript v2/v3/v4 客户端验证 EIO 3/4、Polling、WebSocket、升级、事件、二进制、ACK、Room、中间件和认证
- [x] 对 v2/v3/v4 验证动态 Namespace、服务端 catch-all 监听器、ACK 超时和迟到 ACK 竞态
- [x] 使用官方 Redis Pub/Sub、Sharded Pub/Sub 和 Redis Streams Adapter 验证 Node/Go 混合集群、故障恢复与滚动升级
- [x] 固定官方源码提交并建立 Socket.IO/Engine.IO 测试文件级覆盖映射
- [x] 建立官方 Socket.IO 服务端 203 项运行时测试到 Go 测试的逐项映射；Node/uWebSockets.js 专属 `attachApp()` 不复制表面 API，其 `uws.ts` 12 项可观察行为已由官方 v2/v3/v4 客户端在 Go Polling、WebSocket 与升级连接上完成部署等价验证
- [x] 建立官方 `socket.io.test-d.ts` 22 项编译期类型测试到 Go 的逐项等价映射；泛型请求/ACK、方向感知生成器、保留断开原因、Server/Namespace/Socket/BroadcastOperator、节点事件与 Adapter 均由真实编译器正反例验证
- [x] 建立 Engine.IO 6.6.9 服务端197个源测试行的逐项分类与映射：`engine.io.js` 13项、`middlewares.js` 12项、`parser.js` 1项、`server.js` 153项及 `webtransport.mjs` 18项的可适用行为均有证据，上游 `skip` 与Node/Go宿主边界单列；官方 Engine.IO v3/v4/v6 客户端矩阵覆盖核心协议与升级期间双向高频流量，真实 HTTP/3 + QUIC 回归覆盖 WebTransport 直连、升级、心跳、关闭与1 MB双向大包
- [x] 完成 `engine.io-parser@5.2.3` 的38/38、`socket.io-parser@4.2.7` 的34/34及 `socket.io-adapter@2.5.8` 的48/48官方运行时逐项映射；覆盖数字均由真实具名子测试和逐行映射生成，不使用汇总常量冒充执行结果
- [x] 完成 `engine.io-client@6.6.6` 的源码全平台测试映射：官方108项有效测试声明已全部分类，所有可移植行为均有Go等价证据，另1项上游跳过；Node与浏览器条件分支分别标明真实执行、Go平台等价或不适用，详见 [Engine.IO客户端官方测试映射](clients/engine/OFFICIAL_TEST_MAPPING.md)
- [x] 完成 `socket.io-client@4.8.3` 的115项运行时测试、14项类型测试及独立 `socket.io-component-emitter` 16项依赖契约映射；类型层由核心动态API与可选 `typed` 模块共同提供Go等价
- [x] 使用官方 `socket.io-client@4.8.3` 对 `@socket.io/admin-ui@0.5.1` 协议完成真实 EIO 4/WebSocket 联调，覆盖配置/统计、Socket/Room/事件、四类管理命令、bcrypt Basic Auth、session恢复及 packets/bytes 聚合指标
- [x] 使用官方 `@socket.io/mongo-adapter@0.4.0` 与 Go MongoDB Adapter 完成双向文本/二进制广播、Room、集群查询、节点 ACK、远程 Join、Go↔Node 会话恢复、节点退出和滚动重启互操作测试
- [x] 使用官方 `@socket.io/postgres-adapter@0.5.0` 与 Go PostgreSQL Adapter 完成双向 JSON/MessagePack attachment、Room、集群查询、节点 ACK、远程 Join、节点退出和滚动重启互操作测试；官方版本明确不支持连接恢复，Go 扩展恢复由 Go↔Go 测试覆盖
- [x] 将四个非云官方 Adapter 的上游源码测试逐项建表并补齐等价证据：原始125项已分类闭合为120项Go可适用行为有自动化证据、3项上游 `skip`、2项宿主N/A；其中 Redis 为30+1 N/A+1 skip、Redis Streams为29+1 skip、MongoDB为36项行为映射、PostgreSQL为25+1 skip+1 N/A，不能表述成“125/125测试全部执行通过”
- [x] 完成四个非云 external emitter 的官方测试矩阵：`@socket.io/redis-emitter@5.1.0` 17/17、`@socket.io/redis-streams-emitter@0.1.1` 12/12、`@socket.io/postgres-emitter@0.1.1` 12/12、`@socket.io/mongo-emitter@0.2.0` 12/12，合计53/53真实双向互操作，失败0
- [x] 完成 `@socket.io/cluster-adapter@0.3.0` 的18项三进程部署等价行为和 `@socket.io/sticky@2.0.1` 的10项行为，并验证 Worker 强制退出收敛、滚动加入；uWebSockets.js 的12项可观察部署行为也已覆盖
- [x] 按 `@socket.io/admin-ui@0.5.1` 官方源码完成21个唯一测试声明的逐项映射：20项Go可移植行为均有运行时断言，Node专属 `io.listen()` 1项明确为宿主API不适用；同时补齐默认前缀 `socket.io-admin`、默认TTL 86400秒且可配置的Redis session store，详见 [Admin UI官方测试映射](instrumentation/OFFICIAL_TEST_MAPPING.md)
- [x] 完成 `@socket.io/cluster-engine@0.1.0` 的18/18分类与映射：`ClusterServer`、跨节点读写锁、Polling包转发、Upgrade接管及官方Redis MessagePack/Channel线路已经落地；1–12项进程内精确行为、13–14项无粘性三实例部署等价行为、15–18项真实Redis及官方Node `RedisEngine`（`redis`/`ioredis`、Node/Go双向owner）直接互操作均已通过，并完成高频 `-race` 门禁。Go不复制Node的 `setupPrimary()`、`NodeClusterEngine` 或 `setupPrimaryWithRedis()` 进程拓扑API
- [x] 完成至少数小时的持续连接、频繁重连、长时间网络分区和资源泄漏压力验收；72个官方客户端的完整3小时门禁已通过，覆盖20分钟网络分区、滚动替换、388307/388308次ACK成功、100%广播交付与资源回落检查，详见 [2026-08-01验收记录](docs/soak/2026-08-01.md)

## P0：优先处理

### 1. 补全分布式连接状态恢复

- [x] 为 Adapter 增加能力声明，例如 `SupportsConnectionStateRecovery() bool`
- [x] 启用连接恢复但 Adapter 不支持时，在启动阶段返回明确错误
- [x] 为 MongoDB Adapter 实现 `PersistSession`
- [x] 为 MongoDB Adapter 实现 `RestoreSession`
- [x] 使用 MongoDB Change Stream `_id` 或等价标识作为恢复 offset
- [x] 使用 TTL 索引自动清理 MongoDB 会话和历史数据
- [x] 为 PostgreSQL Adapter 设计持久化事件表和递增 offset
- [x] 为 PostgreSQL Adapter 实现会话保存、恢复和过期清理
- [x] 明确 Unix、普通 Redis Pub/Sub 等 Adapter 不支持恢复的行为
- [x] 增加跨节点断线、重连和遗漏消息恢复测试

验收标准：

- 客户端断线后连接到另一个节点，仍能恢复 Socket ID、Room、Data 和遗漏事件。
- 不支持恢复的 Adapter 不会静默降级为空实现。

### 2. Admin UI 和运行时管理

- [x] 新增独立的 `admin` 或 `instrumentation` 模块
- [x] 展示当前 Socket.IO 节点及其状态
- [x] 展示连接总数和各 Namespace 连接数
- [x] 展示 Socket ID、transport、handshake、rooms 和 data
- [x] 展示 Room 列表和成员
- [x] 实时查看服务端收发事件
- [x] 支持远程让 Socket 加入或离开 Room
- [x] 支持远程断开 Socket
- [x] 支持只读模式
- [x] 支持 Basic Auth 或自定义认证中间件
- [x] 支持多节点环境下的管理数据聚合
- [x] 评估并实现对官方 Socket.IO Admin UI 协议的兼容

验收标准：

- 管理员可以从一个界面查看集群中的节点、连接和房间。
- 管理操作可以通过现有 Adapter 正确传播到远程节点。

### 3. 动态认证和 Token 刷新

- [x] 客户端增加 `AuthProvider` 回调
- [x] `AuthProvider` 接收 `context.Context`
- [x] 首次连接前动态获取认证数据
- [x] 每次重连前重新获取认证数据
- [x] `connect_error` 后允许刷新 Token 并重试
- [x] 支持更新单个 Namespace 的认证数据
- [x] 提供 JWT/JWK 验证中间件扩展
- [x] 提供 API Key 验证中间件扩展
- [x] 支持用户被禁用后主动断开其全部连接

建议接口：

```go
type AuthProvider func(context.Context) (map[string]any, error)
```

### 4. 背压和缓冲区限制

- [x] 为客户端 `sendBuffer` 增加最大包数
- [x] 为客户端 `receiveBuffer` 增加最大包数
- [x] 为客户端 retry queue 增加最大包数
- [x] 支持按字节数限制缓冲区
- [x] 增加 `Reject` 溢出策略
- [x] 增加 `DropNewest` 溢出策略
- [x] 增加 `DropOldest` 溢出策略
- [x] 增加 `Disconnect` 溢出策略
- [x] 暴露 `drain`、`overflow` 和 `slow_consumer` 事件
- [x] 支持 Socket 和 Namespace 级别的独立限制
- [x] 提供当前积压包数和字节数查询接口

验收标准：

- 断网或慢客户端无法导致发送队列无限增长。
- 用户可以选择可靠性优先或实时性优先的溢出策略。

## P1：高价值功能

### 5. 服务端可靠投递和离线消息

- [x] 定义可插拔的 `EventStore` 接口
- [x] 为服务端事件生成唯一 ID 和 offset
- [x] 支持按用户、Socket、Room 或 Namespace 持久化事件
- [x] 客户端提交最后确认的 offset
- [x] 重连后自动 replay 未确认事件
- [x] 支持客户端消息去重
- [x] 支持至少一次投递语义
- [x] 支持保留时间、最大事件数和最大存储空间限制
- [x] 实现 Redis Streams EventStore
- [x] 实现 PostgreSQL EventStore
- [x] 实现 MongoDB EventStore
- [x] 区分临时断线恢复与长期离线消息

建议接口：

```go
type EventStore interface {
	Append(ctx context.Context, target Target, event Event) (Offset, error)
	Replay(ctx context.Context, target Target, after Offset) ([]Event, error)
	Ack(ctx context.Context, clientID string, offset Offset) error
}
```

### 6. Presence 和 Room 统计

- [x] 增加集群级 `CountSockets`
- [x] 增加 `CountRoom`
- [x] 增加 `ListRooms`
- [x] 增加 `RoomStats`
- [x] 增加 `IsUserOnline`
- [x] 增加 `UserSockets`
- [x] 支持一个用户的多设备连接
- [x] 支持用户 ID 到 Socket ID 的标准映射
- [x] 暴露 Room 人数变化事件
- [x] 支持 Room 元数据
- [x] 支持空 Room TTL
- [x] 避免为简单计数调用完整的 `FetchSockets`

验收标准：

- 可以低成本查询跨节点在线人数、用户状态和房间成员数量。

### 7. Prometheus 和 OpenTelemetry

- [x] 新增 Prometheus metrics 扩展模块
- [x] 统计当前连接数、连接速率和断开速率
- [x] 按 transport 和 Namespace 分类统计
- [x] 统计收发事件数和字节数
- [x] 统计 ACK 延迟和超时率
- [x] 统计客户端重连次数
- [x] 统计连接恢复成功率
- [x] 统计 Adapter 发布和响应延迟
- [x] 统计发送队列积压和溢出次数
- [x] 新增 OpenTelemetry tracing 扩展模块
- [x] 为连接、事件处理、ACK 和 Adapter 广播创建 span
- [x] 支持 `traceparent` 或自定义 trace metadata 传播
- [x] 避免默认把高基数 Socket ID 放入 metrics label

### 8. Go 原生强类型事件 API

- [x] 在现有 `any` API 之上增加可选泛型 API
- [x] 支持强类型事件请求体
- [x] 支持强类型 ACK 响应
- [x] 支持 `context.Context` 超时和取消
- [x] 支持强类型 Namespace 事件定义
- [x] 支持强类型 server-side emit
- [x] 提供 Go struct 到 TypeScript 类型生成
- [x] 根据事件定义生成 Go 客户端和服务端代码
- [x] 自动生成事件与 ACK 文档
- [x] 保持与现有动态 API 的兼容

期望用法：

```go
result, err := EmitAck[CreateOrder, OrderResult](
	ctx,
	socket,
	"create-order",
	request,
)
```

## P2：生态扩展

### 9. 新增消息系统和云服务 Adapter

建议优先级：

- [x] NATS Core Adapter
- [x] NATS JetStream Adapter
- [x] Kafka Adapter
- [x] RabbitMQ/AMQP Adapter

每个 Adapter 至少需要评估：

- [x] 普通广播
- [x] Room 定向广播
- [x] 广播 ACK
- [x] `fetchSockets`
- [x] `socketsJoin` 和 `socketsLeave`
- [x] `disconnectSockets`
- [x] `serverSideEmit`
- [x] 节点发现和心跳
- [x] 消息顺序
- [x] 重复消息处理
- [x] 连接状态恢复
- [x] 外部 Emitter

### 10. 可插拔网络组件

- [x] 客户端允许注入 `http.RoundTripper`
- [x] 客户端允许注入自定义 HTTP Client
- [x] 客户端允许注入自定义 WebSocket Dialer
- [x] 客户端允许注入自定义 WebTransport Dialer
- [x] 支持每个连接独立配置代理
- [x] 服务端实现可插拔 WebSocket engine
- [x] 支持统一的拨号、TLS 和代理错误回调
- [x] 允许网络组件接入 tracing 和 metrics

### 11. 安全和流量控制扩展

- [x] 按 IP 限制握手速率
- [x] 按用户限制连接数
- [x] 按 Socket 限制事件速率
- [x] 按 Namespace 配置独立限流规则
- [x] 按事件名配置独立限流规则
- [x] 限制单个 Socket 可加入的 Room 数量
- [x] 限制单个 Room 最大成员数
- [x] 支持违规事件的拒绝、降速或断开策略
- [x] 提供统一的审计事件

## 推荐实施顺序

1. Adapter 能力声明与 MongoDB 连接状态恢复
2. 动态 `AuthProvider`
3. 缓冲区上限和背压策略
4. Admin UI 基础能力
5. Prometheus 指标
6. Presence 和 Room 统计
7. PostgreSQL 连接状态恢复
8. NATS Adapter
9. 服务端可靠投递和离线消息
10. 强类型事件及代码生成

## 版本规划建议

### v3.x：保持兼容的增量功能

- [x] Adapter 能力声明
- [x] 动态认证 Provider
- [x] 缓冲区限制
- [x] Prometheus 扩展
- [x] Presence 查询 API
- [x] MongoDB 连接恢复

### v4：允许调整公共 API

- [x] Context-first API
- [x] 强类型事件 API
- [x] 统一的同步 ACK 返回方式
- [x] 可插拔网络组件
- [x] 标准化 Adapter capability 接口
- [x] 可靠投递 EventStore
