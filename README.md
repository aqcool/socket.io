# Go 语言 Socket.IO

[![Go](https://github.com/aqcool/socket.io/actions/workflows/go.yml/badge.svg)](https://github.com/aqcool/socket.io/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/v3)

一个现代、符合 Go 语言习惯的 [Socket.IO](https://socket.io/) 实现，用于通过 WebSocket 及其他传输方式进行实时双向通信。

---

## ⬆️ 升级指南

如果你正在从本库的 **v1** 或 **v2** 升级，请参阅[升级指南](docs/UPGRADE.md)，其中包含包替换方式和详细迁移步骤。

## 🚀 快速开始

建议先阅读[升级指南](docs/UPGRADE.md)、各模块的 README 以及[示例](examples)。

安装指定模块：

```bash
go get github.com/aqcool/socket.io/servers/socket/v3
```

安装 PostgreSQL 适配器：

```bash
go get github.com/aqcool/socket.io/adapters/postgres/v3
```

安装 Valkey 适配器：

```bash
go get github.com/aqcool/socket.io/adapters/valkey/v3
```

安装 MongoDB 适配器：

```bash
go get github.com/aqcool/socket.io/adapters/mongo/v3
```

---

## ❓ 问题与支持

[Issues](https://github.com/aqcool/socket.io/issues) 仅用于提交已确认的缺陷或功能请求。

如需一般使用帮助或咨询实现问题：

- 阅读[项目文档](docs)
- 在 [Discussions → Q&A](https://github.com/aqcool/socket.io/discussions/new?category=q-a) 中提问

---

## 🔒 安全

如果发现漏洞或安全问题，**请勿提交公开 Issue**，请按照[安全策略](./SECURITY.md)中的步骤进行报告。

---

## 🛠 参与贡献

欢迎各种形式的贡献！如需报告缺陷、建议功能或提交拉取请求：

- 请先阅读[贡献指南](./CONTRIBUTING.md)
- 确保改动经过充分测试，并已使用 `make fmt` 格式化
- 开始重大改动前，请先创建 Issue 或 Discussion 进行讨论

感谢所有让项目不断进步的[贡献者](https://github.com/aqcool/socket.io/graphs/contributors) ❤️

---

## 🗺 功能路线图

协议、可靠性、可观测性、适配器及 Go API 等方面的改进计划记录在[功能路线图](ROADMAP.md)中。官方 JavaScript v2/v3/v4 客户端的支持范围和验证方式见[兼容性说明](docs/COMPATIBILITY.md)，官方 4.8.3 测试套件的覆盖证据与兼容边界见[官方测试对齐映射](docs/OFFICIAL_TEST_MAPPING.md)，多进程、粘性会话和 Cluster Engine 的 Go 等价部署见[集群部署](docs/CLUSTER_DEPLOYMENT.md)。短期 Token 和重连刷新用法见[动态认证](docs/AUTHENTICATION.md)，JWT/JWK 与 API Key 接入见[服务端认证](docs/SERVER_AUTH.md)，安全限流见[Guard](docs/SECURITY_GUARD.md)，在线状态与房间统计见[Presence](docs/PRESENCE.md)，长期离线事件见[可靠投递](docs/RELIABLE_DELIVERY.md)，泛型封装与代码生成见[强类型事件](docs/TYPED_EVENTS.md)，HTTP/Dialer/代理注入见[可插拔网络组件](docs/NETWORK_COMPONENTS.md)，Prometheus 与链路追踪见[可观测性](docs/OBSERVABILITY.md)，慢连接保护见[客户端背压](docs/BACKPRESSURE.md)，运行时管理见[Admin UI 接入](docs/ADMIN_UI.md)。

当前严格运行时追踪口径为1004/1004项官方源测试行完成分类/映射闭合，其中Cluster Engine 18/18已通过进程内、三实例、真实Redis及官方Node `RedisEngine` 双向互操作与并发门禁。该口径包含证据复用、上游跳过、宿主不适用及平台等价项，不等于1004个唯一上游JavaScript测试在Go中原样执行。服务端22项与客户端14项编译期类型契约共36项另行统计。Google Cloud、AWS 和 Azure Adapter 不在本项目对齐范围内。

---

## 📦 模块

本项目采用单体仓库结构，包含以下 Go 模块：

### Go 模块

#### 根模块
| 状态 | Go 模块 | 说明 |
|-------|-----------|-------------|
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/v3) | `github.com/aqcool/socket.io/v3` | 提供共享接口、类型和基础定义的根模块 |

---

#### 服务端
| 状态 | Go 模块 | 说明 |
|-------|-----------|-------------|
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/engine/v3) | `github.com/aqcool/socket.io/servers/engine/v3` | Engine.IO 服务端及支持跨监听器包转发、读写锁和 Upgrade 接管的 `ClusterServer` |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/socket/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/socket/v3) | `github.com/aqcool/socket.io/servers/socket/v3` | 构建于 Engine.IO 服务端之上的 Socket.IO 服务端实现 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/instrumentation/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/instrumentation/v3) | `github.com/aqcool/socket.io/instrumentation/v3` | 兼容官方 Admin UI 协议的运行时观测与管理模块 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/observability/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/observability/v3) | `github.com/aqcool/socket.io/observability/v3` | Prometheus 指标与 OpenTelemetry tracing 扩展 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/reliability/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/reliability/v3) | `github.com/aqcool/socket.io/reliability/v3` | 服务端可靠投递、长期离线重放与客户端消息去重 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/sticky/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/sticky/v3) | `github.com/aqcool/socket.io/sticky/v3` | Engine.IO `sid` 感知的多进程 HTTP/WebSocket 路由器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/typed/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/typed/v3) | `github.com/aqcool/socket.io/typed/v3` | Go 强类型事件、ACK、Context 与 TypeScript/Go/文档生成 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/amqp/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/amqp/v3) | `github.com/aqcool/socket.io/adapters/amqp/v3` | RabbitMQ / AMQP 0-9-1 集群 Adapter |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/broker/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/broker/v3) | `github.com/aqcool/socket.io/adapters/broker/v3` | 消息系统 Adapter 的统一集群协议与传输抽象 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/kafka/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/kafka/v3) | `github.com/aqcool/socket.io/adapters/kafka/v3` | Apache Kafka 集群 Adapter |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/nats/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/nats/v3) | `github.com/aqcool/socket.io/adapters/nats/v3` | NATS Core 与 NATS JetStream 集群 Adapter |

---

消息系统 Adapter 的广播、ACK、跨节点操作、顺序、去重及恢复能力边界见[能力矩阵](docs/BROKER_ADAPTER_MATRIX.md)。

---

#### 客户端
| 状态 | Go 模块 | 说明 |
|-------|-----------|-------------|
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/clients/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/clients/engine/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/clients/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/clients/engine/v3) | `github.com/aqcool/socket.io/clients/engine/v3` | Engine.IO 客户端实现 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/clients/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/clients/socket/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/clients/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/clients/socket/v3) | `github.com/aqcool/socket.io/clients/socket/v3` | 构建于 Engine.IO 客户端之上的 Socket.IO 客户端实现 |

---

#### 解析器
| 状态 | Go 模块 | 说明 |
|-------|-----------|-------------|
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/parsers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/parsers/engine/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/parsers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/parsers/engine/v3) | `github.com/aqcool/socket.io/parsers/engine/v3` | Engine.IO 协议的数据包解析器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/parsers/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/parsers/socket/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/parsers/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/parsers/socket/v3) | `github.com/aqcool/socket.io/parsers/socket/v3` | Socket.IO 协议的数据包解析器 |

---

#### 适配器
| 状态 | Go 模块 | 说明 |
|-------|-----------|-------------|
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/adapter/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/adapter/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/adapter/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/adapter/v3) | `github.com/aqcool/socket.io/adapters/adapter/v3` | 用于实现广播机制的基础适配器接口 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/redis/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/redis/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/redis/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/redis/v3) | `github.com/aqcool/socket.io/adapters/redis/v3` | Socket.IO Redis Adapter，以及与官方 Cluster Engine Redis 线路互通的 `enginebus` |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/valkey/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/valkey/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/valkey/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/valkey/v3) | `github.com/aqcool/socket.io/adapters/valkey/v3` | 基于 Valkey Pub/Sub、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/postgres/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/postgres/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/postgres/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/postgres/v3) | `github.com/aqcool/socket.io/adapters/postgres/v3` | 基于 PostgreSQL LISTEN/NOTIFY、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/mongo/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/mongo/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/mongo/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/mongo/v3) | `github.com/aqcool/socket.io/adapters/mongo/v3` | 基于 MongoDB、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/unix/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/unix/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/unix/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/unix/v3) | `github.com/aqcool/socket.io/adapters/unix/v3` | 基于 Unix 域套接字、用于同一主机进程间广播消息的适配器 |

连接状态恢复支持范围：

| Adapter | 支持状态恢复 | offset 与持久化方式 |
|---------|-------------|---------------------|
| 内存 Session-aware Adapter | 是 | 进程内事件列表 |
| Redis Streams / Valkey Streams | 是 | Stream ID 与带过期时间的会话 |
| MongoDB | 是 | Change Stream 文档 `_id`、会话文档与 TTL 索引 |
| PostgreSQL | 是 | `bigserial` 事件 offset、持久会话表与过期清理 |
| 普通/分片 Redis Pub/Sub、普通/分片 Valkey Pub/Sub、Unix、基础 Cluster Adapter | 否 | 启用恢复时在服务启动阶段明确报错 |

自定义 Adapter 必须通过 `SupportsConnectionStateRecovery() bool` 声明能力。需要自行处理启动错误时请使用 `socket.NewServerWithError`；原有 `socket.NewServer` 遇到不支持恢复的 Adapter 会在启动阶段 panic，不会静默降级。

---

## 🧾 许可证

本项目采用 [MIT 许可证](https://opensource.org/licenses/MIT)。

## 赞助

本项目的 CDN 加速和安全防护由 [Tencent EdgeOne](https://edgeone.ai/?from=github) 赞助。

<p align="left">
  <a href="https://edgeone.ai/?from=github" target="_blank">
    <img src="https://edgeone.ai/media/34fe3a45-492d-4ea4-ae5d-ea1087ca7b4b.png" alt="Tencent EdgeOne" width="180">
  </a>
</p>

[**亚洲领先的 CDN、边缘计算与安全解决方案——腾讯 EdgeOne**](https://edgeone.ai/?from=github)
