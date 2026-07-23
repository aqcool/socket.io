# Socket.IO Go 客户端

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/clients/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/clients/socket/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/clients/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/clients/socket/v3)

一个稳定可靠的 [Socket.IO](https://socket.io/) Go 客户端实现，提供基于事件的实时双向通信。

## 特性

- **传输方式**
  - WebSocket
  - HTTP 长轮询
  - WebTransport（实验性）
  - 自动升级传输方式
  - 回退机制

- **连接管理**
  - 使用指数退避的自动重连
  - 连接状态处理
  - 多命名空间支持
  - 房间支持

- **事件处理**
  - 事件发送与监听
  - 确认回调支持
  - 易失事件
  - 二进制数据支持
  - 自定义事件监听器

- **高级特性**
  - 多路复用
  - 自定义中间件
  - 连接超时处理
  - 调试日志

## 安装

```bash
go get github.com/aqcool/socket.io/clients/socket/v3
```

## 快速开始

### 基本连接

```go
package main

import (
    "log"

    "github.com/aqcool/socket.io/clients/socket/v3"
)

func main() {
    client, err := socket.Connect("http://localhost:3000", nil)
    if err != nil {
        log.Fatal(err)
    }

    client.On("connect", func(...any) {
        log.Println("Connected!")
        client.Emit("message", "Hello Server!")
    })

    client.On("event", func(data ...any) {
        log.Printf("Received: %v", data)
    })

    select {}
}
```

### 高级用法

```go
package main

import (
    "time"

    "github.com/aqcool/socket.io/clients/socket/v3"
    "github.com/aqcool/socket.io/clients/engine/v3/transports"
    "github.com/aqcool/socket.io/v3/pkg/types"
)

func main() {
    opts := socket.DefaultOptions()
    opts.SetTransports(types.NewSet(
        transports.Polling,
        transports.WebSocket,
    ))
    opts.SetTimeout(5 * time.Second)
    opts.SetReconnection(true)
    opts.SetReconnectionAttempts(5)

    manager := socket.NewManager("http://localhost:3000", opts)

    // Custom namespace
    socket := manager.Socket("/custom", nil)

    // Event handling
    socket.On("connect", func(...any) {
        socket.Emit("auth", map[string]string{
            "token": "your-auth-token",
        })
    })

    // Acknowledgement
    socket.Emit("event", "data", func(args ...any) {
        log.Printf("Server acknowledged: %v", args)
    })

    // Listen to all events
    socket.OnAny(func(args ...any) {
        log.Printf("Caught event: %v", args)
    })
}
```

## API 参考

### Manager 选项

```go
opts := socket.DefaultManagerOptions()
opts.SetReconnection(true)                   // Enable/disable reconnection
opts.SetReconnectionAttempts(math.Inf(1))    // Number of reconnection attempts
opts.SetReconnectionDelay(1000)              // Initial delay in milliseconds
opts.SetReconnectionDelayMax(5000)           // Maximum delay between reconnections
opts.SetRandomizationFactor(0.5)             // Randomization factor for delays
opts.SetTimeout(20000)                       // Connection timeout
```

### Socket 方法

- **Emit**: `socket.Emit(eventName string, args ...any)`
- **On**: `socket.On(eventName string, fn func(...any))`
- **Once**: `socket.Once(eventName string, fn func(...any))`
- **Off**: `socket.Off(eventName string, fn func(...any))`
- **OnAny**: `socket.OnAny(fn func(...any))`
- **Connect**: `socket.Connect()`
- **Disconnect**: `socket.Disconnect()`

### 事件

- `connect`：连接成功时触发
- `disconnect`：断开连接时触发
- `connect_error`：连接出错时触发
- `reconnect`：重连成功时触发
- `reconnect_attempt`：尝试重连时触发
- `reconnect_error`：重连出错时触发
- `reconnect_failed`：重连失败时触发

## 调试

启用调试日志：


```go
import "github.com/aqcool/socket.io/v3/pkg/log"

log.DEBUG = true
```

```bash
make test
```

## 测试

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

- [Socket.IO](https://socket.io/)
- [Engine.IO Go 客户端](../engine)
- [Socket.IO Go 服务端](../../servers/socket)
