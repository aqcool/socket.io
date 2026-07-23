# socket.io-go-parser

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/parsers/socket/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/parsers/socket/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/parsers/socket/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/parsers/socket/v3)

## 概述

这是 Socket.IO 协议的 Go 解析器，负责数据包的编码与解码，由 [Socket.IO 客户端](../../clients/socket)和 [Socket.IO 服务端](../../servers/socket)共同使用。

### 兼容性表

| 解析器版本 | Socket.IO 服务端版本 | 协议修订版 |
|----------------|---------------------------|-------------------|
| 3.x            | 3.x                       | 5                 |

## 特性

- 完整支持 Socket.IO 协议 v5
- 数据包编解码
- 二进制数据支持
- 基于事件的解码
- 可扩展且线程安全的实现

## 安装

运行以下命令安装本包：

```bash
go get github.com/aqcool/socket.io/parsers/socket/v3
```

## 使用示例

### 编解码数据包

```go
package main

import (
    "github.com/aqcool/socket.io/v3/pkg/utils"
    "github.com/aqcool/socket.io/parsers/socket/v3/parser"
)

func main() {
    encoder := parser.NewEncoder()
    id := uint64(13)
    packet := &parser.Packet{
        Type: parser.EVENT,
        Data: []string{"test-packet"},
        Id:   &id,
    }
    encodedPackets := encoder.Encode(packet)
    utils.Log().Default("Encoded: %v", encodedPackets)

    for _, encodedPacket := range encodedPackets {
        decoder := parser.NewDecoder()
        decoder.On("decoded", func(decodedPackets ...any) {
            utils.Log().Default("Decoded: %v", decodedPackets[0])
            // decodedPackets[0].Type == parser.EVENT
            // decodedPackets[0].Data == []string{"test-packet"}
            // decodedPackets[0].Id == 13
        })

        decoder.Add(encodedPacket)
    }
}
```

### 编解码包含二进制数据的数据包

```go
package main

import (
    "github.com/aqcool/socket.io/v3/pkg/utils"
    "github.com/aqcool/socket.io/parsers/socket/v3/parser"
)

func main() {
    encoder := parser.NewEncoder()
    attachments := uint64(0)
    packet := &parser.Packet{
        Type:        parser.BINARY_EVENT,
        Data:        []any{"test-packet", []byte{1, 2, 3, 4, 5}},
        Id:          utils.Ptr(uint64(13)),
        Attachments: &attachments,
    }
    encodedPackets := encoder.Encode(packet)
    utils.Log().Default("Encoded: %v", encodedPackets)

    for _, encodedPacket := range encodedPackets {
        decoder := parser.NewDecoder()
        decoder.On("decoded", func(decodedPackets ...any) {
            utils.Log().Default("Decoded: %v", decodedPackets[0])
            // decodedPackets[0].Type == parser.BINARY_EVENT
            // decodedPackets[0].Data == []any{"test-packet", []byte{1, 2, 3, 4, 5}}
            // decodedPackets[0].Id == 13
        })

        decoder.Add(encodedPacket)
    }
}
```

## API 参考

### 数据包结构

```go
type Packet struct {
    Type        PacketType   // Type of the packet (e.g., EVENT, BINARY_EVENT)
    Data        any          // Packet data
    Id          *uint64      // Packet ID (optional)
    Attachments *uint64      // Number of binary attachments (optional)
}
```

### Encoder 接口

```go
type Encoder interface {
    Encode(packet *Packet) []types.BufferInterface
}
```

### Decoder 接口

```go
type Decoder interface {
    types.EventEmitter
    Add(data any) error
    Destroy()
}
```

## 测试

运行测试套件：

```bash
make test
```

## 开发

请按以下步骤参与项目贡献：

1. Fork 本仓库。
2. 创建功能分支：`git checkout -b feature/amazing-feature`。
3. 提交改动：`git commit -m 'Add some amazing feature'`。
4. 推送分支：`git push origin feature/amazing-feature`。
5. 创建拉取请求。

## 支持

如果遇到问题或有任何疑问，请在 [Issue 区](https://github.com/aqcool/socket.io/issues)提交。

## 许可证

本项目采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE) 文件。
