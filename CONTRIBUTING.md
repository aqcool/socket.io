# Socket.IO 贡献指南（Go 版本）

感谢你关注并参与 [`aqcool/socket.io`](https://github.com/aqcool/socket.io) 维护的 **Socket.IO Go 实现**！

为保证协作顺畅，请在开始前阅读以下指南。

<!-- TOC -->
  * [开始之前](#开始之前)
  * [报告缺陷](#报告缺陷)
  * [提出功能请求](#提出功能请求)
  * [创建拉取请求](#创建拉取请求)
    * [缺陷修复](#缺陷修复)
    * [新功能](#新功能)
  * [项目结构](#项目结构)
  * [开发环境](#开发环境)
  * [常用命令](#常用命令)
    * [代码格式化](#代码格式化)
    * [运行测试](#运行测试)
<!-- TOC -->

## 开始之前

- [Issue 列表](https://github.com/aqcool/socket.io/issues)仅用于**缺陷报告和功能请求**。
- 一般使用问题请：
  - 查阅[项目文档](docs)
  - 或创建一个 [Discussion](https://github.com/aqcool/socket.io/discussions/new?category=q-a)

## 报告缺陷

- 首先在带有 [bug 标签的问题](https://github.com/aqcool/socket.io/issues?q=label%3Abug+)中检查是否已有相同报告。
- 如果问题曾被报告但已关闭，而问题依然存在，请创建一个**新 Issue**，不要在旧 Issue 下留言。
- 对于安全相关缺陷，**请勿**创建公开 Issue，请参阅[安全策略](./SECURITY.md)。

### 创建缺陷报告时
- 提供 Go 模块版本
- 说明运行平台（操作系统、架构）
- 基于[示例](examples)或模块文档提供最小可复现示例

## 提出功能请求

- 在[改进列表](https://github.com/aqcool/socket.io/labels/enhancement)中检查是否已有相似请求。
- 查看[功能路线图](ROADMAP.md)。
- 如果没有相关请求，请[提交新的功能请求](https://github.com/aqcool/socket.io/issues/new/choose)。

请包含：
- 需要解决的问题
- 建议的解决方案
- 已考虑的替代方案或临时解决办法

## 创建拉取请求

欢迎提交缺陷修复、新功能和其他改进。

### 缺陷修复
- 引用相关 Issue（如有）
- 添加测试用例以防止问题再次出现
- 确保全部现有测试通过

### 新功能
- 请先创建[功能请求](#提出功能请求)进行讨论
- 同时提供测试和文档
- 提交前确保全部测试通过

## 项目结构

这是一个 **Go 单体仓库**，每个子模块都是独立的 Go 模块：

| Go 模块                                                    | 说明                                                                                                                                    |
|------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `github.com/aqcool/socket.io/v3`                       | 定义共享类型、接口和入口的根模块。                                                                                                    |
| `github.com/aqcool/socket.io/servers/engine/v3`        | Engine.IO 服务端：通过各类传输方式管理底层通信。                                                                                       |
| `github.com/aqcool/socket.io/clients/engine/v3`        | Engine.IO 客户端：建立到服务端的底层传输连接。                                                                                         |
| `github.com/aqcool/socket.io/parsers/engine/v3`        | Engine.IO 数据包解析器：编解码传输层消息。                                                                                             |
| `github.com/aqcool/socket.io/servers/socket/v3`        | Socket.IO 服务端：构建于 Engine.IO 服务端之上，提供实时双向通信。                                                                      |
| `github.com/aqcool/socket.io/clients/socket/v3`        | Socket.IO 客户端：构建于 Engine.IO 客户端之上，支持房间、命名空间等功能。                                                              |
| `github.com/aqcool/socket.io/parsers/socket/v3`        | Socket.IO 数据包解析器：处理基于事件的消息编解码。                                                                                     |
| `github.com/aqcool/socket.io/adapters/adapter/v3`      | 适配器接口：用于多节点通信的可插拔广播层。                                                                                             |
| `github.com/aqcool/socket.io/adapters/redis/v3`        | Redis 适配器：通过 Redis Pub/Sub 实现消息广播。                                                                                        |
| `github.com/aqcool/socket.io/adapters/valkey/v3`       | Valkey 适配器：通过 Valkey Pub/Sub 实现消息广播。                                                                                      |
| `github.com/aqcool/socket.io/adapters/postgres/v3`     | PostgreSQL 适配器：通过 LISTEN/NOTIFY 实现消息广播。                                                                                    |
| `github.com/aqcool/socket.io/adapters/mongo/v3`        | MongoDB 适配器：通过 MongoDB 变更流实现消息广播。                                                                                      |
| `github.com/aqcool/socket.io/adapters/unix/v3`         | Unix 域套接字适配器：在同一主机的进程间进行低延迟消息广播。                                                                            |

## 开发环境

- 安装 [Go](https://go.dev) **1.26.0 或更高版本**
- 克隆仓库；如有需要，可使用 Go 工作区（`go work`）关联各子模块

## 常用命令

### 代码格式化

格式化所有工作区：

```bash
make fmt
```

格式化指定工作区：

**Windows：**
```bash
make fmt servers/socket
```

**Unix：**
```bash
make fmt MODULE=servers/socket
```

### 运行测试

运行全部测试：

```bash
make test
```

测试指定工作区：

**Windows：**
```bash
make test servers/socket
```

**Unix：**
```bash
make test MODULE=servers/socket
```

---

如有任何问题，欢迎创建 Issue 或 Discussion。祝编码愉快！✨
