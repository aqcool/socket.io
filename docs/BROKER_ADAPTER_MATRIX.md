# 消息系统 Adapter 能力矩阵

本文档记录 Broker 系列 Adapter 的能力边界和验收依据。表中的“支持”表示功能已通过通用集群契约或对应 transport 测试；“不支持”表示 Adapter 会明确声明能力缺失，不会静默降级。

## Socket.IO 集群操作

NATS Core、NATS JetStream、Kafka 和 RabbitMQ/AMQP 都复用 `adapters/broker` 的集群协议，因此具有相同的 Socket.IO 操作能力。

| 能力 | NATS Core | NATS JetStream | Kafka | RabbitMQ/AMQP | 验收依据 |
|---|---|---|---|---|---|
| 普通广播 | 支持 | 支持 | 支持 | 支持 | Broker 集群编码、发布和广播路径 |
| Room 定向广播 | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` 的 Room 定向广播 ACK |
| 广播 ACK | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` |
| `fetchSockets` | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` |
| `socketsJoin` / `socketsLeave` | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` |
| `disconnectSockets` | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` |
| `serverSideEmit` | 支持 | 支持 | 支持 | 支持 | `TestMemoryBrokerPropagatesServerSideEmit` |
| 节点发现和心跳 | 支持 | 支持 | 支持 | 支持 | `TestBrokerClusterContract` |
| 外部 Emitter | 支持 | 支持 | 支持 | 支持 | `TestEmitterAndDuplicateSuppression` |

通用契约测试使用确定性的 Memory Broker 验证完整 Socket.IO 行为；各 transport 测试负责验证消息系统 SDK 与 `broker.Broker` 之间的发布、订阅、消息 ID、错误和关闭语义。

## 投递、顺序和重复消息

| Adapter | 投递语义 | 顺序 | 重复消息处理 |
|---|---|---|---|
| NATS Core | 至多一次 | NATS subject 到达顺序；通用层串行分派 | Publisher 写入全局消息 ID，通用层按 ID 抑制重复 |
| NATS JetStream | 至少一次，显式 ACK/NACK | 单 Stream / Consumer 顺序；通用层串行分派 | `Nats-Msg-Id` 服务端去重，加通用层消费端去重 |
| Kafka | 至多一次控制消息消费 | 分区内有序；固定 `PartitionKey` 配合 Hash Partitioner 可固定到一个分区 | Topic、Partition、Offset 组成稳定消息 ID，通用层按 ID 去重 |
| RabbitMQ/AMQP | 至少一次，手动 ACK/NACK 和 Publisher Confirm | 单 Channel、单 Queue 内有序；重新入队时可能重排 | Publisher 写入全局 Message ID，通用层按 ID 抑制重新投递 |

Kafka 多分区之间不声明全局顺序。需要严格全局顺序时，应使用单分区 Topic，或保证固定 key 在分区数量不变时始终映射到同一分区。

## 连接状态恢复

| Adapter | 状态 | 原因 |
|---|---|---|
| NATS Core | 不支持 | Core 不持久化消息 |
| NATS JetStream | 不支持 | 当前持久化的是集群控制命令，没有实现 Socket.IO Session 保存和按 offset 精确事件重放 |
| Kafka | 不支持 | 当前消费者从订阅时的最新 offset 开始，不保存 Socket.IO Session |
| RabbitMQ/AMQP | 不支持 | 独占自动删除队列只保存在线控制命令，不保存 Socket.IO Session |

这四个 Builder 的 `SupportsConnectionStateRecovery()` 均通过通用 Broker Builder 明确返回 `false`。启用连接状态恢复时，Socket.IO 服务端会在启动阶段返回明确错误。

Redis Streams、Valkey Streams、MongoDB 和 PostgreSQL 的恢复能力参见 [升级指南](UPGRADE.md)。

## 官方 Redis Adapter 互通

普通 Redis Pub/Sub、Sharded Pub/Sub 和 Redis Streams Adapter 已通过官方 `@socket.io/redis-adapter@8.3.0`、`@socket.io/redis-streams-adapter@0.3.1` 与 Go 实现的真实双向互通测试，覆盖文本及二进制广播、Room、广播 ACK、`fetchSockets`、`serverSideEmitWithAck`、`socketsJoin`、`socketsLeave` 和 `disconnectSockets`。Redis Streams 还验证了 Node 保存、Go 恢复以及 Go 保存、Node 恢复两种会话迁移方向，包括 Socket ID、Room 和遗漏事件。三种模式均验证了 Redis 单连接强制重置、经 Toxiproxy 完全不可达 2 秒以及 Redis 容器实际重启后的自动重连、重新订阅和集群操作恢复，也验证了官方 Node Adapter 进程和独立 Go Adapter 进程分别被 `SIGKILL` 后的成员收敛和失联 Socket 清理。故障收敛后还会在两个方向各发送 300 条连续广播，检查顺序、丢失和重复。

滚动升级矩阵额外锁定普通 Pub/Sub `8.2.1→8.3.0` 和 Redis Streams `0.3.0→0.3.1`，验证新旧节点重叠、旧节点强制退出及新版节点接管。Sharded Go Adapter 同时兼容 `8.2.1` 的无 `nsp` 旧报文和 `8.3.0` 的新报文；但官方 Node `8.3.0` 本身会拒绝来自 `8.2.1` 的无 `nsp` 报文，因此这两个官方 Sharded Node 版本之间需要协调替换，不能宣称直接全双工滚动兼容。测试入口和运行方式参见[兼容性说明](COMPATIBILITY.md)。

## 官方 MongoDB/PostgreSQL Adapter 互通

官方 `@socket.io/mongo-adapter@0.4.0` 与 Go MongoDB Adapter 已在真实副本集上完成双向广播、二进制、Room、集群查询、节点 ACK、远程 Join、两方向会话恢复、节点退出和滚动重启。官方 Mongo 协议的消息类型 13 是 `SESSION`，Go Mongo 实现对此作传输专属处理，避免与新版通用协议的 `ADAPTER_CLOSE` 冲突。

官方 `@socket.io/postgres-adapter@0.5.0` 与 Go PostgreSQL Adapter 已在真实 PostgreSQL 上完成相同集群操作，并分别覆盖直接 JSON `NOTIFY`、二进制 attachment 和 20 KB 大 attachment。官方 PostgreSQL Adapter 当前不支持连接恢复；Go 的 PostgreSQL 恢复属于扩展能力，只声明 Go↔Go 验证，不声明 Node↔Go。
