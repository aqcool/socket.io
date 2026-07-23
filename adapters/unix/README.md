# Socket.IO Unix 域套接字适配器

本包以 Unix 域套接字作为消息代理，支持在多个 Socket.IO 服务端之间广播数据包。

本适配器适用于同一主机上的多进程部署，可在不依赖 Redis 或 PostgreSQL 等外部服务的情况下实现低延迟进程间通信。

## 目录

- [安装](#安装)
- [用法](#用法)
  - [适配器](#适配器)
  - [发射器](#发射器)
- [工作原理](#工作原理)
- [许可证](#许可证)

## 安装

```bash
go get github.com/aqcool/socket.io/adapters/unix/v3
```

## 用法

### 适配器

```go
package main

import (
	"context"
	"net/http"

	sio "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/adapters/unix/v3"
	unixadapter "github.com/aqcool/socket.io/adapters/unix/v3/adapter"
)

func main() {
	ctx := context.Background()
	socketPath := "/tmp/socket.io.sock"

	client := unix.NewUnixClient(ctx, socketPath)

	opts := unixadapter.DefaultUnixAdapterOptions()
	opts.SetKey("socket.io")

	io := sio.NewServer(nil, nil)
	io.SetAdapter(&unixadapter.UnixAdapterBuilder{
		Unix: client,
		Opts: opts,
	})

	io.On("connection", func(args ...any) {
		socket := args[0].(*sio.Socket)
		socket.On("message", func(args ...any) {
			io.Emit("message", args...)
		})
	})

	http.Handle("/socket.io/", io.ServeHandler(nil))
	http.ListenAndServe(":3000", nil)
}
```

### 发射器

发射器允许从任意进程向已连接客户端发送事件，无需运行完整的 Socket.IO 服务端：

```go
package main

import (
	"context"
	"fmt"

	"github.com/aqcool/socket.io/adapters/unix/v3"
	unixemitter "github.com/aqcool/socket.io/adapters/unix/v3/emitter"
)

func main() {
	ctx := context.Background()
	socketPath := "/tmp/socket.io.sock"

	client := unix.NewUnixClient(ctx, socketPath)

	opts := &unixemitter.EmitterOptions{}
	opts.SetKey("socket.io")
	opts.SetSocketPath(socketPath)

	emitter := unixemitter.NewEmitter(client, opts)

	// Emit to all clients
	if err := emitter.Emit("hello", "world"); err != nil {
		fmt.Printf("emit error: %v\n", err)
	}

	// Emit to specific room
	if err := emitter.To("room1").Emit("hello", "room"); err != nil {
		fmt.Printf("emit error: %v\n", err)
	}
}
```

## 工作原理

每个 Socket.IO 服务端节点都会创建唯一的 Unix 域套接字监听文件：

```
/tmp/socket.io.sock.{server-uid}
```

需要广播消息时，适配器会扫描套接字目录，查找符合基础路径模式的所有对等节点监听文件，并通过 Unix 数据报套接字将消息发送给每个节点。

**消息编码：**
- 非二进制消息使用 JSON
- 二进制消息使用 MessagePack

**节点发现：**
- 基于文件系统：每个节点创建一个 `{base}.{uid}` 套接字文件
- 通过扫描套接字目录发现其他节点

## 许可证

[MIT](LICENSE)
