# socket.io-go-mongo

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/adapters/mongo/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/adapters/mongo/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/adapters/mongo/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/adapters/mongo/v3)

## 简介

Socket.IO Go 服务端的 MongoDB 适配器，通过 MongoDB 变更流将 Socket.IO 应用扩展到多个进程或服务器。

本适配器兼容 Node.js 的 [@socket.io/mongo-adapter](https://github.com/socketio/socket.io-mongo-adapter)，支持 Go 与 Node.js 混合部署。

**注意：** MongoDB 必须配置为副本集或分片集群，才能使用变更流。

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/mongo/v3
```

## 特性

- 通过 MongoDB 变更流支持多服务器
- 兼容 Node.js `@socket.io/mongo-adapter`，支持混合部署
- 基于心跳的节点故障检测
- 进程间实时通信
- 同时支持固定集合和 TTL 索引

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

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
    mgadapter "github.com/aqcool/socket.io/adapters/mongo/v3/adapter"
    mgclient "github.com/aqcool/socket.io/adapters/mongo/v3"
    "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
    client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(context.Background())

    collection := client.Database("mydb").Collection("socket.io-adapter-events")

    mongoClient := mgclient.NewMongoClient(context.TODO(), collection)

    io := socket.NewServer(nil, nil)
    io.SetAdapter(&mgadapter.MongoAdapterBuilder{
        Mongo: mongoClient,
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

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
    mgclient "github.com/aqcool/socket.io/adapters/mongo/v3"
    mgemitter "github.com/aqcool/socket.io/adapters/mongo/v3/emitter"
)

func main() {
    client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(context.Background())

    collection := client.Database("mydb").Collection("socket.io-adapter-events")

    mongoClient := mgclient.NewMongoClient(context.TODO(), collection)

    emitter := mgemitter.NewEmitter(mongoClient, nil)
    emitter.Emit("hello", "world")
    emitter.To("room1").Emit("hello", "world")
}
```

## 工作原理

适配器使用 MongoDB 变更流检测共享集合中新插入的文档。当 Socket.IO 服务端需要广播消息或执行跨节点操作时，会向 MongoDB 集合插入文档。其他监听同一集合的服务器会收到通知并处理相应事件。

### 固定集合与 TTL 索引

可使用**固定集合**或 **TTL 索引**进行自动清理：

#### 固定集合（多数场景推荐）
```javascript
db.createCollection("socket.io-adapter-events", { capped: true, size: 1e6 })
```

#### TTL 索引
```javascript
db.collection("socket.io-adapter-events").createIndex(
    { createdAt: 1 },
    { expireAfterSeconds: 3600 }
)
```

使用 TTL 索引时，请将 `AddCreatedAtField` 选项设为 `true`：
```golang
opts := &mgadapter.MongoAdapterOptions{}
opts.SetAddCreatedAtField(true)
```

## 许可证

[MIT](LICENSE)
