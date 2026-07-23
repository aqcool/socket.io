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

协议、可靠性、可观测性、适配器及 Go API 等方面的改进计划记录在[功能路线图](ROADMAP.md)中。

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
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/engine/v3) | `github.com/aqcool/socket.io/servers/engine/v3` | 负责底层传输处理的 Engine.IO 服务端实现 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/socket/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/socket/v3) | `github.com/aqcool/socket.io/servers/socket/v3` | 构建于 Engine.IO 服务端之上的 Socket.IO 服务端实现 |

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
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/redis/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/redis/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/redis/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/redis/v3) | `github.com/aqcool/socket.io/adapters/redis/v3` | 基于 Redis、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/valkey/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/valkey/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/valkey/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/valkey/v3) | `github.com/aqcool/socket.io/adapters/valkey/v3` | 基于 Valkey Pub/Sub、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/postgres/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/postgres/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/postgres/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/postgres/v3) | `github.com/aqcool/socket.io/adapters/postgres/v3` | 基于 PostgreSQL LISTEN/NOTIFY、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/mongo/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/mongo/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/mongo/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/mongo/v3) | `github.com/aqcool/socket.io/adapters/mongo/v3` | 基于 MongoDB、用于在分布式服务器间广播消息的适配器 |
| [![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/unix/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/unix/v3) <br/> [![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/unix/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/unix/v3) | `github.com/aqcool/socket.io/adapters/unix/v3` | 基于 Unix 域套接字、用于同一主机进程间广播消息的适配器 |

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
