# Socket.IO for Go 功能改进清单

本文档记录项目后续可增加或完善的功能，重点关注协议能力、生产可用性和 Go 生态体验，不包含代码格式、CI 优化等工程治理事项。

## 当前已有能力

- [x] Engine.IO v3/v4 协议支持
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

## P0：优先处理

### 1. 补全分布式连接状态恢复

- [ ] 为 Adapter 增加能力声明，例如 `SupportsConnectionStateRecovery() bool`
- [ ] 启用连接恢复但 Adapter 不支持时，在启动阶段返回明确错误
- [ ] 为 MongoDB Adapter 实现 `PersistSession`
- [ ] 为 MongoDB Adapter 实现 `RestoreSession`
- [ ] 使用 MongoDB Change Stream `_id` 或等价标识作为恢复 offset
- [ ] 使用 TTL 索引自动清理 MongoDB 会话和历史数据
- [ ] 为 PostgreSQL Adapter 设计持久化事件表和递增 offset
- [ ] 为 PostgreSQL Adapter 实现会话保存、恢复和过期清理
- [ ] 明确 Unix、普通 Redis Pub/Sub 等 Adapter 不支持恢复的行为
- [ ] 增加跨节点断线、重连和遗漏消息恢复测试

验收标准：

- 客户端断线后连接到另一个节点，仍能恢复 Socket ID、Room、Data 和遗漏事件。
- 不支持恢复的 Adapter 不会静默降级为空实现。

### 2. Admin UI 和运行时管理

- [ ] 新增独立的 `admin` 或 `instrumentation` 模块
- [ ] 展示当前 Socket.IO 节点及其状态
- [ ] 展示连接总数和各 Namespace 连接数
- [ ] 展示 Socket ID、transport、handshake、rooms 和 data
- [ ] 展示 Room 列表和成员
- [ ] 实时查看服务端收发事件
- [ ] 支持远程让 Socket 加入或离开 Room
- [ ] 支持远程断开 Socket
- [ ] 支持只读模式
- [ ] 支持 Basic Auth 或自定义认证中间件
- [ ] 支持多节点环境下的管理数据聚合
- [ ] 评估并实现对官方 Socket.IO Admin UI 协议的兼容

验收标准：

- 管理员可以从一个界面查看集群中的节点、连接和房间。
- 管理操作可以通过现有 Adapter 正确传播到远程节点。

### 3. 动态认证和 Token 刷新

- [ ] 客户端增加 `AuthProvider` 回调
- [ ] `AuthProvider` 接收 `context.Context`
- [ ] 首次连接前动态获取认证数据
- [ ] 每次重连前重新获取认证数据
- [ ] `connect_error` 后允许刷新 Token 并重试
- [ ] 支持更新单个 Namespace 的认证数据
- [ ] 提供 JWT/JWK 验证中间件扩展
- [ ] 提供 API Key 验证中间件扩展
- [ ] 支持用户被禁用后主动断开其全部连接

建议接口：

```go
type AuthProvider func(context.Context) (map[string]any, error)
```

### 4. 背压和缓冲区限制

- [ ] 为客户端 `sendBuffer` 增加最大包数
- [ ] 为客户端 `receiveBuffer` 增加最大包数
- [ ] 为客户端 retry queue 增加最大包数
- [ ] 支持按字节数限制缓冲区
- [ ] 增加 `Reject` 溢出策略
- [ ] 增加 `DropNewest` 溢出策略
- [ ] 增加 `DropOldest` 溢出策略
- [ ] 增加 `Disconnect` 溢出策略
- [ ] 暴露 `drain`、`overflow` 和 `slow_consumer` 事件
- [ ] 支持 Socket 和 Namespace 级别的独立限制
- [ ] 提供当前积压包数和字节数查询接口

验收标准：

- 断网或慢客户端无法导致发送队列无限增长。
- 用户可以选择可靠性优先或实时性优先的溢出策略。

## P1：高价值功能

### 5. 服务端可靠投递和离线消息

- [ ] 定义可插拔的 `EventStore` 接口
- [ ] 为服务端事件生成唯一 ID 和 offset
- [ ] 支持按用户、Socket、Room 或 Namespace 持久化事件
- [ ] 客户端提交最后确认的 offset
- [ ] 重连后自动 replay 未确认事件
- [ ] 支持客户端消息去重
- [ ] 支持至少一次投递语义
- [ ] 支持保留时间、最大事件数和最大存储空间限制
- [ ] 实现 Redis Streams EventStore
- [ ] 实现 PostgreSQL EventStore
- [ ] 实现 MongoDB EventStore
- [ ] 区分临时断线恢复与长期离线消息

建议接口：

```go
type EventStore interface {
	Append(ctx context.Context, target Target, event Event) (Offset, error)
	Replay(ctx context.Context, target Target, after Offset) ([]Event, error)
	Ack(ctx context.Context, clientID string, offset Offset) error
}
```

### 6. Presence 和 Room 统计

- [ ] 增加集群级 `CountSockets`
- [ ] 增加 `CountRoom`
- [ ] 增加 `ListRooms`
- [ ] 增加 `RoomStats`
- [ ] 增加 `IsUserOnline`
- [ ] 增加 `UserSockets`
- [ ] 支持一个用户的多设备连接
- [ ] 支持用户 ID 到 Socket ID 的标准映射
- [ ] 暴露 Room 人数变化事件
- [ ] 支持 Room 元数据
- [ ] 支持空 Room TTL
- [ ] 避免为简单计数调用完整的 `FetchSockets`

验收标准：

- 可以低成本查询跨节点在线人数、用户状态和房间成员数量。

### 7. Prometheus 和 OpenTelemetry

- [ ] 新增 Prometheus metrics 扩展模块
- [ ] 统计当前连接数、连接速率和断开速率
- [ ] 按 transport 和 Namespace 分类统计
- [ ] 统计收发事件数和字节数
- [ ] 统计 ACK 延迟和超时率
- [ ] 统计客户端重连次数
- [ ] 统计连接恢复成功率
- [ ] 统计 Adapter 发布和响应延迟
- [ ] 统计发送队列积压和溢出次数
- [ ] 新增 OpenTelemetry tracing 扩展模块
- [ ] 为连接、事件处理、ACK 和 Adapter 广播创建 span
- [ ] 支持 `traceparent` 或自定义 trace metadata 传播
- [ ] 避免默认把高基数 Socket ID 放入 metrics label

### 8. Go 原生强类型事件 API

- [ ] 在现有 `any` API 之上增加可选泛型 API
- [ ] 支持强类型事件请求体
- [ ] 支持强类型 ACK 响应
- [ ] 支持 `context.Context` 超时和取消
- [ ] 支持强类型 Namespace 事件定义
- [ ] 支持强类型 server-side emit
- [ ] 提供 Go struct 到 TypeScript 类型生成
- [ ] 根据事件定义生成 Go 客户端和服务端代码
- [ ] 自动生成事件与 ACK 文档
- [ ] 保持与现有动态 API 的兼容

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

- [ ] NATS Core Adapter
- [ ] NATS JetStream Adapter
- [ ] Kafka Adapter
- [ ] Google Cloud Pub/Sub Adapter
- [ ] AWS SQS/SNS Adapter
- [ ] Azure Service Bus Adapter
- [ ] RabbitMQ/AMQP Adapter

每个 Adapter 至少需要评估：

- [ ] 普通广播
- [ ] Room 定向广播
- [ ] 广播 ACK
- [ ] `fetchSockets`
- [ ] `socketsJoin` 和 `socketsLeave`
- [ ] `disconnectSockets`
- [ ] `serverSideEmit`
- [ ] 节点发现和心跳
- [ ] 消息顺序
- [ ] 重复消息处理
- [ ] 连接状态恢复
- [ ] 外部 Emitter

### 10. 可插拔网络组件

- [ ] 客户端允许注入 `http.RoundTripper`
- [ ] 客户端允许注入自定义 HTTP Client
- [ ] 客户端允许注入自定义 WebSocket Dialer
- [ ] 客户端允许注入自定义 WebTransport Dialer
- [ ] 支持每个连接独立配置代理
- [ ] 服务端实现可插拔 WebSocket engine
- [ ] 支持统一的拨号、TLS 和代理错误回调
- [ ] 允许网络组件接入 tracing 和 metrics

### 11. 安全和流量控制扩展

- [ ] 按 IP 限制握手速率
- [ ] 按用户限制连接数
- [ ] 按 Socket 限制事件速率
- [ ] 按 Namespace 配置独立限流规则
- [ ] 按事件名配置独立限流规则
- [ ] 限制单个 Socket 可加入的 Room 数量
- [ ] 限制单个 Room 最大成员数
- [ ] 支持违规事件的拒绝、降速或断开策略
- [ ] 提供统一的审计事件

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

- [ ] Adapter 能力声明
- [ ] 动态认证 Provider
- [ ] 缓冲区限制
- [ ] Prometheus 扩展
- [ ] Presence 查询 API
- [ ] MongoDB 连接恢复

### v4：允许调整公共 API

- [ ] Context-first API
- [ ] 强类型事件 API
- [ ] 统一的同步 ACK 返回方式
- [ ] 可插拔网络组件
- [ ] 标准化 Adapter capability 接口
- [ ] 可靠投递 EventStore

