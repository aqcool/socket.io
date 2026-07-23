# socket.io-go-postgres

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/postgres/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/postgres/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/postgres/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/postgres/v3)

## 简介

Socket.IO Go 服务端的 PostgreSQL 适配器，通过 PostgreSQL 的 `LISTEN`/`NOTIFY` 机制将应用扩展到多个进程或服务器。

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/postgres/v3
```

## 特性

- 通过 PostgreSQL `LISTEN`/`NOTIFY` 支持多服务器
- 通过附件表自动处理大型载荷
- 基于心跳的节点故障检测
- 进程间实时通信
- 自定义 PostgreSQL 配置

## 使用方法

### 适配器

```golang
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/aqcool/socket.io/adapters/postgres/v3"
    pgadapter "github.com/aqcool/socket.io/adapters/postgres/v3/adapter"
    "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
    pool, err := pgxpool.New(context.Background(), "postgres://user:password@localhost:5432/mydb")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    pgClient := postgres.NewPostgresClient(context.TODO(), pool)

    io := socket.NewServer(nil, nil)
    io.SetAdapter(&pgadapter.PostgresAdapterBuilder{
        Postgres: pgClient,
    })

    io.On("connection", func(args ...any) {
        s := args[0].(*socket.Socket)
        fmt.Printf("connect %s\n", s.Id())
    })

    exit := make(chan struct{})
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sig
        close(exit)
    }()
    <-exit
}
```

### 发射器

```golang
package main

import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/aqcool/socket.io/adapters/postgres/v3"
    pgemitter "github.com/aqcool/socket.io/adapters/postgres/v3/emitter"
)

func main() {
    pool, err := pgxpool.New(context.Background(), "postgres://user:password@localhost:5432/mydb")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    pgClient := postgres.NewPostgresClient(context.TODO(), pool)

    emitter := pgemitter.NewEmitter(pgClient, nil)
    emitter.Emit("hello", "world")
    emitter.To("room1").Emit("hello", "world")
}
```

## 配置选项

### 适配器选项

```golang
type PostgresAdapterOptions struct {
    Key               string        // PostgreSQL channel prefix (default: "socket.io")
    TableName         string        // Attachment storage table name (default: "socket_io_attachments")
    PayloadThreshold  int           // Byte threshold for attachment storage (default: 8000)
    CleanupInterval   int64         // Cleanup interval in milliseconds (default: 30000)
    HeartbeatInterval time.Duration // Interval between heartbeats (default: 5000ms)
    HeartbeatTimeout  int64         // Heartbeat response timeout (default: 10000)
    ErrorHandler      func(error)   // Custom error handler callback
}
```

### 发射器选项

```golang
type EmitterOptions struct {
    Key              string // PostgreSQL channel prefix (default: "socket.io")
    TableName        string // Attachment storage table name (default: "socket_io_attachments")
    PayloadThreshold int    // Byte threshold for attachment storage (default: 8000)
}
```

## 架构

PostgreSQL 适配器使用两种机制进行节点间通信：

1. **LISTEN/NOTIFY**——对低于载荷阈值的消息使用轻量级发布/订阅
2. **附件表**——存储超过 `NOTIFY` 限制的大型载荷或二进制数据

直接通过 `NOTIFY` 传输的消息使用 JSON 序列化，附件存储则使用 MessagePack。这保证了与 Node.js `socket.io-postgres-adapter` 的兼容性，允许在同一集群中混合部署 Go 和 Node.js。

### 数据库结构

适配器会在启动时自动创建附件表：

```sql
CREATE TABLE IF NOT EXISTS socket_io_attachments (
    id bigserial UNIQUE,
    created_at timestamptz DEFAULT NOW(),
    payload bytea
);
```

## 混合部署

本 Go 适配器在线路协议层兼容 Node.js 的 [`socket.io-postgres-adapter`](https://github.com/socketio/socket.io-postgres-adapter) 和 [`socket.io-postgres-emitter`](https://github.com/socketio/socket.io-postgres-emitter)。满足以下条件时，可在同一集群中混合部署 Go 和 Node.js 服务端：

- 使用相同的频道前缀（默认：`socket.io`）
- 使用相同的附件表名（默认：`socket_io_attachments`）
- 使用相同的命名空间名称

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
