# Socket.IO Go 示例

本目录包含 Socket.IO Go 实现的使用示例和协议一致性测试，包括多个示例应用以及覆盖 Engine.IO、Socket.IO 行为的自动化测试套件。

## 示例

| 示例 | 说明 |
|---------|-------------|
| [benchmark](./benchmark/) | 使用高频连接和断开检测内存及 goroutine 泄漏 |
| [chat](./chat/) | 支持用户名、输入状态及加入/离开通知的经典聊天室 |
| [basic-crud-application](./basic-crud-application/) | 对共享待办列表执行实时 CRUD 操作并广播更新 |
| [middleware-auth](./middleware-auth/) | 基于令牌的身份认证中间件及管理员命名空间授权 |
| [test-suite](./test-suite/) | Engine.IO 和 Socket.IO 协议一致性测试 |
| [unix-adapter-debug](./unix-adapter-debug/) | Unix 域套接字适配器的多节点调试示例 |

## 快速开始

每个示例都是独立的 Go 模块。运行示例：

```bash
cd examples/<example-name>
go run main.go
```

运行测试：

```bash
cd examples/<example-name>
go test -v -race ./...
```

## 示例功能

### 聊天室
- 多名用户使用唯一用户名加入
- 实时消息广播
- 输入状态通知
- 用户加入/离开事件及在线人数统计

### 基础 CRUD 应用
- 创建、读取、更新和删除待办事项
- 向所有已连接客户端实时广播改动
- 使用确认回调（ack）确认操作结果
- 线程安全的内存存储

### 中间件认证
- 使用命名空间级中间件进行连接认证
- 建立连接前验证令牌
- 需要额外授权的管理员命名空间
- 通过确认回调获取用户资料

### 测试套件
- Engine.IO 握手（HTTP 长轮询和 WebSocket）
- Engine.IO 心跳（ping/pong 和超时）
- Engine.IO 会话关闭及传输升级
- Socket.IO 命名空间连接和断开
- Socket.IO 消息传递（纯文本、二进制和确认回调）
- 载荷限制、边界情况及会话管理
