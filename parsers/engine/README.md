# engine.io-go-parser

[![Go Reference](https://pkg.go.dev/badge/github.com/aqcool/socket.io/parsers/engine/v3.svg)](https://pkg.go.dev/github.com/aqcool/socket.io/parsers/engine/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqcool/socket.io/parsers/engine/v3)](https://goreportcard.com/report/github.com/aqcool/socket.io/parsers/engine/v3)

## 简介

Engine.IO 协议解析器的 Go 实现。[Engine.IO 客户端](../../clients/engine)和 [Engine.IO 服务端](../../servers/engine)均使用本包进行协议编解码。

## 安装

```bash
go get github.com/aqcool/socket.io/parsers/engine/v3
```

## 特性

- 数据包编解码
- 载荷编解码
- 二进制数据支持
- 支持协议 v3 和 v4
- UTF-8 编码支持

## 使用方法

### 基本用法

```go
package main

import (
    "bytes"
    "fmt"

    "github.com/aqcool/socket.io/parsers/engine/v3/packet"
    "github.com/aqcool/socket.io/v3/pkg/types"
)

func main() {
    // Initialize parser
    parser := packet.Parserv4()

    // Encode a packet
    encodedData, err := parser.EncodePacket(&packet.Packet{
        Type: packet.MESSAGE,
        Data: bytes.NewBuffer([]byte("Hello World")),
    }, true)
    if err != nil {
        panic(err)
    }

    // Decode a packet
    decodedPacket, err := parser.DecodePacket(encodedData)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Decoded message: %s\n", decodedPacket.Data)
}
```

### 处理载荷

```go
func handlePayload() {
    parser := packet.Parserv4()

    packets := []*packet.Packet{
        {
            Type: packet.MESSAGE,
            Data: bytes.NewBuffer([]byte("First message")),
        },
        {
            Type: packet.MESSAGE,
            Data: bytes.NewBuffer([]byte("Second message")),
        },
    }

    // Encode payload
    encoded, err := parser.EncodePayload(packets)
    if err != nil {
        panic(err)
    }

    // Decode payload
    decoded, err := parser.DecodePayload(encoded)
    if err != nil {
        panic(err)
    }
}
```

## API 参考

### Parser 接口

#### EncodePacket

```go
EncodePacket(packet *packet.Packet, supportsBinary bool) (types.BufferInterface, error)
```

- `packet`：待编码的数据包
- `supportsBinary`：是否启用二进制支持
- 返回：编码后的数据包及可能发生的错误

#### DecodePacket

```go
DecodePacket(data types.BufferInterface) (*packet.Packet, error)
```

- `data`：待解码的数据
- 返回：解码后的数据包及可能发生的错误

#### EncodePayload

```go
EncodePayload(packets []*packet.Packet) (types.BufferInterface, error)
```

- `packets`：待编码的数据包数组
- 返回：编码后的载荷及可能发生的错误

#### DecodePayload

```go
DecodePayload(data types.BufferInterface) ([]*packet.Packet, error)
```

- `data`：待解码的载荷
- 返回：解码后的数据包数组及可能发生的错误

## 开发

### 前置条件

- Go 1.26.0+
- Make

### 测试

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

本项目采用 MIT 许可证，详情请参阅 [LICENSE](LICENSE) 文件。
