# Engine.IO Go 客户端

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/clients/engine/v4.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/clients/engine/v4)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/clients/engine/v4)](https://goreportcard.com/report/github.com/aqcool/socket.io/clients/engine/v4)

一个稳定可靠的 [Engine.IO](../../servers/engine) Go 客户端实现。Engine.IO 是支撑 [Socket.IO](../../servers/socket) 的实时双向通信层。

## 特性

- **多种传输方式**
  - WebSocket 传输
  - HTTP 长轮询传输
  - WebTransport（实验性）
  - 自动升级传输方式
  - 回退机制

- **连接管理**
  - 心跳机制
  - 连接状态处理
  - Cookie 支持
  - 自定义请求头
  - 按顺序尝试所有传输方式（`tryAllTransports`）

- **数据处理**
  - 二进制数据支持
  - Base64 编码回退
  - 数据包缓冲
  - 消息压缩

- **高级特性**
  - 事件驱动架构
  - 可配置超时
  - 自定义 HTTP 客户端、RoundTripper、代理和 TLS
  - 调试日志
  - 跨平台兼容
  - Engine.IO 协议 v4

Engine.IO 的 `Socket` 本身不会自动重连，这与官方 `engine.io-client` 一致。自动重连属于上层 Socket.IO Manager 的职责。

## 安装

```bash
go get github.com/aqcool/socket.io/clients/engine/v4
```

## 快速开始

### 基本用法

```go
package main

import (
    "log"

    eio "github.com/aqcool/socket.io/clients/engine/v4"
    "github.com/aqcool/socket.io/v4/pkg/types"
)

func main() {
    socket := eio.NewSocket("ws://localhost", nil)

    socket.On("open", func(args ...any) {
        log.Println("Connection established")
        socket.Send(types.NewStringBufferString("Hello!"), nil, nil)
    })

    socket.On("message", func(args ...any) {
        log.Printf("Received: %v", args[0])
    })

    socket.On("close", func(args ...any) {
        log.Println("Connection closed")
    })

    select {}
}
```

### 高级配置

```go
package main

import (
    "time"

    engine "github.com/aqcool/socket.io/clients/engine/v4"
    "github.com/aqcool/socket.io/clients/engine/v4/transports"
)

func main() {
    opts := engine.DefaultSocketOptions()

    // Transport configuration
    // 传输顺序有业务含义，应使用有序 API。
    opts.SetTransportList([]engine.TransportCtor{
        transports.WebSocket,
        transports.Polling,
        transports.WebTransport,
    })

    // Connection settings
    opts.SetPath("/engine.io")
    opts.SetRequestTimeout(10 * time.Second)
    opts.SetWithCredentials(true)

    // Upgrade configuration
    opts.SetUpgrade(true)
    opts.SetRememberUpgrade(true)
    opts.SetTryAllTransports(true)

    socket := engine.NewSocket("ws://localhost", opts)
    // ... event handlers
}
```

## API 参考

### Socket 状态

- `SocketStateOpening`：正在建立连接
- `SocketStateOpen`：连接已打开并可用
- `SocketStateClosing`：连接正在关闭
- `SocketStateClosed`：连接已关闭

### 事件

- `open`：连接已建立
- `message`：收到消息
- `close`：连接已关闭
- `error`：发生错误
- `upgrade`：传输方式升级完成
- `upgradeError`：传输方式升级失败
- `packet`：收到原始数据包
- `drain`：写缓冲区已清空

### 传输类型

```go
import "github.com/aqcool/socket.io/clients/engine/v4/transports"

// Available transports
transports.Polling      // HTTP long-polling
transports.WebSocket    // WebSocket
transports.WebTransport // WebTransport (experimental)
```

## 开发

### 运行测试

```bash
make test
```

官方 `engine.io-client@6.6.6` 测试分母、Go 等价项和平台不适用项见 [官方测试映射](OFFICIAL_TEST_MAPPING.md)。

### 调试

启用调试日志：

```go
import "github.com/aqcool/socket.io/v4/pkg/log"

log.DEBUG = true
```

```bash
make test
```

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 许可证

采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE) 文件。

## 相关项目

- [Engine.IO 协议](https://github.com/socketio/engine.io-protocol)
- [Engine.IO 服务端](../../servers/engine)
