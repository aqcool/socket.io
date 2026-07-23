# Go 语言 Socket.IO 服务端

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/socket/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/socket/v3)

## 概述

Socket.IO 是面向 Go 的实时双向事件通信库。本模块提供 Socket.IO 服务端实现。

## 特性

- **协议支持**
  - Socket.IO v4+ 协议
  - 二进制数据传输
  - 多路复用（命名空间）
  - 房间支持

- **传输层**
  - WebSocket
  - HTTP 长轮询
  - WebTransport（实验性）

- **高级特性**
  - 自动重连
  - 数据包缓冲
  - 确认回调
  - 广播
  - 多服务端实例支持

## 安装

```bash
go get github.com/aqcool/socket.io/servers/socket/v3
```

## 快速开始

### 基本用法

```go
package main

import (
    "github.com/aqcool/socket.io/servers/socket/v3"
    "github.com/aqcool/socket.io/v3/pkg/types"
)

func main() {
    server := socket.NewServer(nil, nil)

    server.On("connection", func(clients ...any) {
        client := clients[0].(*socket.Socket)

        // Handle events
        client.On("message", func(data ...any) {
            // Echo the received message
            client.Emit("message", data...)
        })
    })

    server.Listen(":3000", nil)
}
```

## 服务端集成

### 标准 HTTP 服务端

```go
http.Handle("/socket.io/", server.ServeHandler(nil))
http.ListenAndServe(":3000", nil)
```

### Fasthttp

```go
fasthttp.ListenAndServe(":3000", fasthttpadaptor.NewFastHTTPHandler(
    server.ServeHandler(nil),
))
```

### Fiber

```go
app := fiber.New()
app.Use("/socket.io/", adaptor.HTTPHandler(server.ServeHandler(nil)))
```

## 高级用法

### 命名空间

```go
// Create a custom namespace
nsp := server.Of("/custom", nil)

nsp.On("connection", func(clients ...any) {
    client := clients[0].(*socket.Socket)
    // Handle namespace specific events
})
```

### 房间

```go
server.On("connection", func(clients ...any) {
    client := clients[0].(*socket.Socket)

    // Join a room
    client.Join("room1")

    // Broadcast to room
    server.To("room1").Emit("event", "message")
})
```

### 中间件

```go
server.Use(func(client *socket.Socket, next func()) {
    // Middleware logic
    next()
})
```

## 配置

```go
opts := socket.DefaultServerOptions()
opts.SetPingTimeout(20 * time.Second)
opts.SetPingInterval(25 * time.Second)
opts.SetMaxHttpBufferSize(1e6)
opts.SetCors(&types.Cors{
    Origin: "*",
    Credentials: true,
})
```

## 调试

启用调试日志：

```bash
DEBUG=socket.io*
```

## 测试

运行测试套件：

```bash
make test
```

## API 文档

详细 API 文档请参阅：

- [GoDoc 文档](https://pkg.go.dev/github.com/aqcool/socket.io/servers/socket/v3)
- [Socket.IO 协议](https://github.com/socketio/socket.io-protocol)

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 许可证

本项目采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE) 文件。
