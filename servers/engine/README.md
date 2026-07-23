# Engine.IO：Go 实时通信引擎

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/servers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/servers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/servers/engine/v3)

## 概述

Engine.IO 是 [Socket.IO Go 实现](../socket)基于传输层的跨浏览器、跨设备双向通信层。它屏蔽 WebSocket、Polling、WebTransport 等传输方式的差异，并提供统一 API。

## 特性

- 支持多种传输方式（WebSocket、Polling、WebTransport）
- 自动升级传输方式
- 带心跳机制的有状态连接
- 二进制数据支持
- 多路复用支持
- 自动重连支持
- 跨浏览器兼容
- 支持 Engine.IO 协议 v3 和 v4

## 安装

```bash
go get github.com/aqcool/socket.io/servers/engine/v3
```

## 快速开始

```go
package main

import (
    "github.com/aqcool/socket.io/servers/engine/v3"
    "github.com/aqcool/socket.io/servers/engine/v3/config"
    "github.com/aqcool/socket.io/v3/pkg/types"
)

func main() {
    // Configure server options
    serverOptions := &config.ServerOptions{}
    serverOptions.SetAllowEIO3(true)
    serverOptions.SetCors(&types.Cors{
        Origin:      "*",
        Credentials: true,
    })

    // Create and start server
    server := engine.Listen(":4444", serverOptions, nil)

    // Handle connections
    server.On("connection", func(sockets ...any) {
        socket := sockets[0].(engine.Socket)
        socket.On("message", func(args ...any) {
            // Handle messages
        })
    })

    // Keep the server running
    select {}
}
```

## 用法

### 服务端初始化方式

1. **直接监听**
2. **集成 HTTP 服务端**
3. **自定义请求处理**
4. **集成 WebSocket**
5. **WebTransport 支持**

## 配置

### 服务端选项

```go
opts := &config.ServerOptions{}
opts.SetPingTimeout(20_000 * time.Millisecond)
opts.SetPingInterval(25_000 * time.Millisecond)
opts.SetUpgradeTimeout(10_000 * time.Millisecond)
opts.SetMaxHttpBufferSize(1e6)
// ...
```

## 传输方式实现

- **Polling**：XHR/JSONP 传输
- **WebSocket**：标准 WebSocket 传输
- **WebTransport**：实验性 WebTransport 支持

## 事件

### 服务端事件

- `connection`：新的客户端连接
- `connection_error`：连接错误
- `flush`：刷新缓冲区
- `drain`：缓冲区已清空

### Socket 事件

- `message`：收到消息
- `close`：连接已关闭
- `error`：发生错误
- `flush`：刷新写缓冲区
- `drain`：写缓冲区已清空
- `packet`：收到原始数据包
- `packetCreate`：发送数据包前
- `heartbeat`：收到 Ping/Pong

## 开发

### 前置条件

- Go 1.26.0+
- Make

### 测试

```bash
make test
```

### 调试

设置 `DEBUG` 环境变量：

```bash
DEBUG=engine*
```

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 许可证

采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE)。

## 支持

- [API 文档](https://pkg.go.dev/github.com/aqcool/socket.io/servers/engine/v3)
- [问题跟踪](https://github.com/aqcool/socket.io/issues)
