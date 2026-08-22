# socket.io-go-adapter

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/adapter/v4.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/adapter/v4)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/adapter/v4)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/adapter/v4)

## 简介

Socket.IO Go 服务端的基础适配器实现，提供构建自定义适配器和扩展 Socket.IO 应用所需的核心能力。

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/adapter/v4
```

## 特性

- 基础适配器接口及实现
- 集群适配器支持
- 会话感知适配器能力
- 用于集群通信的心跳机制
- 远程 Socket 处理
- 可扩展的适配器架构

## 使用方法

基本用法示例：

```golang
package main

import (
    "github.com/aqcool/socket.io/adapters/adapter/v4"
    "github.com/aqcool/socket.io/servers/socket/v4"
)

func main() {
    // 创建 Socket.IO 服务端配置
    config := socket.DefaultServerOptions()

    // 使用默认适配器
    config.SetAdapter(&adapter.AdapterBuilder{})

    // 创建使用该适配器的服务端
    io := socket.NewServer(nil, config)

    // 处理连接
    io.On("connection", func(clients ...any) {
        client := clients[0].(*socket.Socket)
        // 在此编写连接处理逻辑
    })
}
```

## 适配器类型

本包提供以下适配器实现：

1. 基础适配器

```golang
type Adapter interface {
    Broadcast([]Room, *BroadcastOptions, ...any)
    BroadcastWithAck([]Room, *BroadcastOptions, ...any) <-chan []any
    // ... other methods
}
```

2. 集群适配器

```golang
type ClusterAdapter interface {
    Adapter
    ServerCount() int
    // Additional cluster-specific methods
}
```

3. 会话感知适配器

```golang
type SessionAwareAdapter interface {
    Adapter
    SaveSession(id string, session any)
    GetSession(id string) any
    // Session management methods
}
```

## 配置选项

### ClusterAdapterOptions

```golang
type ClusterAdapterOptions struct {
    HeartbeatInterval time.Duration
    HeartbeatTimeout  time.Duration
}
```

## 测试

运行测试套件：

```bash
make test
```

## 参与贡献

1. Fork 本仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交改动（`git commit -m 'Add some amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 创建拉取请求

## 支持

如果遇到问题或有任何疑问，请在 [Issue 区](https://github.com/aqcool/socket.io/issues)提交。

## 许可证

本项目采用 MIT 许可证，详情请参阅 LICENSE 文件。
