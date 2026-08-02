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
- 对齐 `engine.io-parser@5.2.3` 的 WebTransport 长度帧流
- `maxPayload`、零长度和 JavaScript 安全整数上限校验

官方 38 项运行时测试的准确分母和逐项对应关系见
[OFFICIAL_TEST_MAPPING.md](OFFICIAL_TEST_MAPPING.md)。

## 使用方法

### 基本用法

```go
package main

import (
    "fmt"
    "io"
    "strings"

    "github.com/aqcool/socket.io/parsers/engine/v3/packet"
    engineparser "github.com/aqcool/socket.io/parsers/engine/v3/parser"
)

func main() {
    // Initialize parser
    codec := engineparser.Parserv4()

    // Encode a packet
    encodedData, err := codec.EncodePacket(&packet.Packet{
        Type: packet.MESSAGE,
        Data: strings.NewReader("Hello World"),
    }, true)
    if err != nil {
        panic(err)
    }

    // Decode a packet
    decodedPacket, err := codec.DecodePacket(encodedData)
    if err != nil {
        panic(err)
    }

    decodedData, err := io.ReadAll(decodedPacket.Data)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Decoded message: %s\n", decodedData)
}
```

### 处理载荷

```go
func handlePayload() {
    codec := engineparser.Parserv4()

    packets := []*packet.Packet{
        {
            Type: packet.MESSAGE,
            Data: strings.NewReader("First message"),
        },
        {
            Type: packet.MESSAGE,
            Data: strings.NewReader("Second message"),
        },
    }

    // Encode payload
    encoded, err := codec.EncodePayload(packets)
    if err != nil {
        panic(err)
    }

    // Decode payload
    decoded, err := codec.DecodePayload(encoded)
    if err != nil {
        panic(err)
    }
}
```

### WebTransport packet stream

官方 WebTransport 流不是用 `0x1e` 拼接的 polling payload，而是给每个包增加
1、3 或 9 字节的大端长度头。Go 对应接口如下：

```go
var wire bytes.Buffer

encoder := engineparser.NewPacketStreamEncoder(&wire)
if err := encoder.Encode(&packet.Packet{
    Type: packet.MESSAGE,
    Data: strings.NewReader("hello"),
}); err != nil {
    panic(err)
}

decoder := engineparser.NewPacketStreamDecoder(&wire, 1024*1024)
decoded, err := decoder.Decode()
if err != nil {
    panic(err)
}
```

文本数据请使用 `*strings.Reader` 或 `*types.StringBuffer`；其他 `io.Reader`
按二进制数据处理。`PacketStreamDecoder` 在读取声明的 payload 前校验
`maxPayload`，避免不受控分配。

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

### Packet stream 接口

```go
EncodePacketToBinary(packet *packet.Packet) ([]byte, error)
EncodePacketFrame(packet *packet.Packet) (header, payload []byte, err error)
NewPacketStreamEncoder(writer io.Writer) *PacketStreamEncoder
NewPacketStreamDecoder(reader io.Reader, maxPayload uint64) *PacketStreamDecoder
```

- `PacketStreamEncoder.Encode`：向连续 WebTransport 流写入一个长度帧。
- `PacketStreamDecoder.Decode`：从流中读取一个包；流正常结束时返回 `io.EOF`。
- 协议错误同时返回标准 Engine.IO error packet 和可用 `errors.Is` 判断的 Go 错误。

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
