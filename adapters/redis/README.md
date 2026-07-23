# socket.io-go-redis

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/redis/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/redis/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/redis/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/redis/v3)

## 简介

Socket.IO Go 服务端的 Redis 适配器，可将 Socket.IO 应用扩展到多个进程或服务器。

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/redis/v3
```

## 特性

- 多服务器支持
- 进程间实时通信
- 自动重连
- 自定义 Redis 配置
- 跨服务器发送事件

## 使用方法

基本用法示例：

```golang
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    rds "github.com/redis/go-redis/v9"
    "github.com/aqcool/socket.io/adapters/redis/v3"
    "github.com/aqcool/socket.io/adapters/redis/v3/adapter"
    "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
    // Initialize Redis client
    redisClient := redis.NewRedisClient(context.TODO(), rds.NewClient(&rds.Options{
        Addr:     "127.0.0.1:6379",
        Username: "",
        Password: "",
        DB:       0,
    }))

    // Redis error handling
    redisClient.On("error", func(a ...any) {
        fmt.Println(a)
    })

    // Socket.IO server configuration
    config := socket.DefaultServerOptions()
    config.SetAdapter(&adapter.RedisAdapterBuilder{
        Redis: redisClient,
        Opts:  &adapter.RedisAdapterOptions{},
    })

    // Create and configure server
    httpServer := s.CreateServer(nil)
    io := socket.NewServer(httpServer, config)

    // Handle socket connections
    io.On("connection", func(clients ...any) {
        client := clients[0].(*socket.Socket)
        client.On("event", func(datas ...any) {
            // Handle your events here
        })
        client.On("disconnect", func(...any) {
            // Handle disconnect
        })
    })

    // Start server
    httpServer.Listen("127.0.0.1:9000", nil)

    // Graceful shutdown handling
    exit := make(chan struct{})
    SignalC := make(chan os.Signal)

    signal.Notify(SignalC, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
    go func() {
        for s := range SignalC {
            switch s {
            case os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
                close(exit)
                return
            }
        }
    }()

    <-exit
    httpServer.Close(nil)
    os.Exit(0)
}
```

## 配置选项

Redis 适配器支持以下选项：

```golang
type RedisAdapterOptions struct {
    Prefix  string // Optional prefix for Redis keys
    // Add other available options here
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
