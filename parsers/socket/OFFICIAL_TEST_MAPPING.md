# socket.io-parser 4.2.7 官方运行时测试映射

## 基准与分母

- 官方版本：`socket.io-parser@4.2.7`
- 官方标签提交：`4054894738817f5a2125e6e6b18e79d92c75ab33`
- 官方测试源码：[`parser.js`](https://github.com/socketio/socket.io/blob/4054894738817f5a2125e6e6b18e79d92c75ab33/packages/socket.io-parser/test/parser.js)、[`buffer.js`](https://github.com/socketio/socket.io/blob/4054894738817f5a2125e6e6b18e79d92c75ab33/packages/socket.io-parser/test/buffer.js)、[`arraybuffer.js`](https://github.com/socketio/socket.io/blob/4054894738817f5a2125e6e6b18e79d92c75ab33/packages/socket.io-parser/test/arraybuffer.js)、[`blob.js`](https://github.com/socketio/socket.io/blob/4054894738817f5a2125e6e6b18e79d92c75ab33/packages/socket.io-parser/test/blob.js)
- 统计口径：上述四个文件中声明的 `it(...)` 运行时测试；辅助函数和环境探测不单独计数。

| 文件 | 官方测试数 |
|---|---:|
| `parser.js` | 17 |
| `buffer.js` | 8 |
| `arraybuffer.js` | 6 |
| `blob.js` | 3 |
| **去重后的完整分母** | **34** |

官方 CI 使用 Node.js 24，因此 Node 运行会加载全部 34 项；浏览器运行会跳过 Node `Buffer` 文件，执行 26 项。Go 映射以四个文件的并集为固定分母，结果为 **34/34**。所有对应回归都位于 `parser/official_runtime_427_test.go` 的 `TestOfficialRuntimeSuite427` 中。

## 逐项映射

### `parser.js`：17/17

| # | 官方测试名 | Go 映射与证据 |
|---:|---|---|
| 1 | `exposes types` | 直接映射；校验七种 `PacketType` 的数值和有效性。 |
| 2 | `encodes connection` | 直接映射；CONNECT、`/woot` 和认证对象完整往返。 |
| 3 | `encodes disconnection` | 直接映射；DISCONNECT 与自定义 namespace 完整往返。 |
| 4 | `encodes an event` | 直接映射；字符串事件、数字参数和对象完整往返。 |
| 5 | `encodes an event (with an integer as event name)` | 直接映射；Go JSON 解码后的数字规范化为 `float64`，事件名仍按数字合法处理。 |
| 6 | `encodes an event (with ack)` | 直接映射；EVENT、namespace 和 ACK ID 完整往返。 |
| 7 | `encodes an ack` | 直接映射；ACK、ID 和数组负载完整往返。 |
| 8 | `encodes an connect error` | 直接映射；字符串 CONNECT_ERROR 完整往返。 |
| 9 | `encodes an connect error (with object)` | 直接映射；对象 CONNECT_ERROR 完整往返。 |
| 10 | `throws an error when encoding circular objects` | 直接映射；循环 map 明确触发 `ErrCircularReference`，不会无限递归。 |
| 11 | `decodes a bad binary packet` | 直接映射；缺少附件头时返回精确 `Illegal attachments`。 |
| 12 | `throws an error when receiving too many attachments` | 直接映射；`MaxAttachments=2` 拒绝声明 3 个附件的包。 |
| 13 | `decodes with a custom reviver` | 直接映射；`NewDecoder(reviver)` 旧式构造参数。 |
| 14 | `decodes with a custom reviver (options object)` | 直接映射；`DecoderOptions.SetReviver()`。 |
| 15 | `throw an error upon parsing error` | 直接映射；覆盖官方全部 11 个非法 payload、5 个非法附件计数、未知包类型和未知 Go 输入类型，见下节。 |
| 16 | `should resume decoding after calling destroy()` | 直接映射；销毁未完成的 binary reconstruction 后可继续解析普通事件。 |
| 17 | `should ensure that a packet is valid` | 直接映射；`IsPacketValid()` 覆盖官方 11 个数据组合。Go 的 namespace 和 ACK ID 分别由 `string`、`*uint64` 静态约束。 |

第 15 项没有缩减官方内部断言。非法 payload 为：

```text
442["some","data"
0/admin,"invalid"
0[]
1/admin,{}
2/admin,"invalid
2/admin,{}
2[{"toString":"foo"}]
2[true,"foo"]
2[null,"bar"]
2["connect"]
2["disconnect","123"]
```

非法附件计数为 `5`、`51`、`50-`、`5a-`、`51.23-`；另校验 `999` 返回 `unknown packet type 9`，整数输入 `999` 返回 `Unknown type: 999`。

### `buffer.js`：8/8

Node `Buffer` 在 Go 中由 `[]byte` 或 `types.BufferInterface` 表示。容器类型不同，但线上二进制帧、placeholder 和还原结果相同。

| # | 官方测试名 | Go 映射与证据 |
|---:|---|---|
| 18 | `encodes a Buffer` | `[]byte` 等价；EVENT、ID、namespace 与二进制内容完整往返。 |
| 19 | `encodes a nested Buffer` | `[]byte` 等价；map/slice 深层 Buffer 完整往返。 |
| 20 | `encodes a binary ack with Buffer` | `[]byte` 等价；重建后包类型恢复为普通 `ACK`。 |
| 21 | `encodes a Buffer nested in an object with a toJSON() method` | `JSONTransformer.ToJSON()` + `types.BufferInterface` 等价；还原为 `{file: Buffer}` 形状。 |
| 22 | `throws an error when adding an attachment with an invalid 'num' attribute (string)` | 直接映射；`num:"splice"` 返回精确 `illegal attachments`。 |
| 23 | `throws an error when adding an attachment with an invalid 'num' attribute (out-of-bound)` | 直接映射；越界索引返回精确 `illegal attachments`。 |
| 24 | `throws an error when adding an attachment without header` | 直接映射；返回 `got binary data when not reconstructing a packet`。 |
| 25 | `throws an error when decoding a binary event without attachments` | 直接映射；重建期间收到文本返回 `got plaintext data when reconstructing a packet`。 |

### `arraybuffer.js`：6/6

JavaScript `ArrayBuffer` 和 `Uint8Array` 在 Go 中统一映射为 `[]byte`；没有原型的普通对象映射为 `map[string]any`。

| # | 官方测试名 | Go 映射与证据 |
|---:|---|---|
| 26 | `encodes an ArrayBuffer` | `[]byte` 等价完整往返。 |
| 27 | `encodes an ArrayBuffer into an object with a null prototype` | `map[string]any` + `[]byte` 等价完整往返；Go 没有 JavaScript prototype。 |
| 28 | `encodes a TypedArray` | `Uint8Array` 映射为 `[]byte`，字节序列完整往返。 |
| 29 | `encodes ArrayBuffers deep in JSON` | 深层 map 中多个 `[]byte` 完整往返。 |
| 30 | `encodes deep binary JSON with null values` | 深层二进制与 `nil` 共存时完整往返。 |
| 31 | `should not modify the input packet` | 编码前后包类型、namespace、附件字段和原始字节数据不变。 |

### `blob.js`：3/3

Go 标准库没有浏览器 `Blob`/`BlobBuilder` 类。其“可读取的二进制对象”行为由 `io.Reader` 映射；浏览器构造器和厂商前缀探测属于测试环境逻辑，记为 N/A，不另占官方 `it(...)` 分母。

| # | 官方测试名 | Go 映射与证据 |
|---:|---|---|
| 32 | `encodes a Blob` | `bytes.Reader`（`io.Reader`）等价完整往返。 |
| 33 | `encodes an Blob deep in JSON` | 深层 map 中 `io.Reader` 等价完整往返。 |
| 34 | `encodes a binary ack with a blob` | ACK 深层 `io.Reader` 等价；重建后类型恢复为普通 `ACK`。 |

## 关键等价边界

- 官方解码器收到 `BINARY_EVENT`/`BINARY_ACK` 头后，在完整重建时对外发出普通 `EVENT`/`ACK`；Go 实现遵循同一行为。
- JavaScript 动态检查 namespace 类型和 ACK ID 是否为整数；Go 的 `Packet` 字段在编译期限定为 `string` 和 `*uint64`，因此不存在对应的非法运行时值。
- Node `Buffer`、浏览器 `ArrayBuffer`/`Uint8Array`/`Blob` 是平台容器，不是协议新增类型；Go 分别使用 `[]byte`、`types.BufferInterface` 和 `io.Reader`，placeholder 编号及线上帧结构不变。
- `BlobBuilder`、浏览器厂商前缀和 `Object.create(null)` 的 JavaScript API 本身为 N/A；其可观察编码行为均已通过 Go 等价容器覆盖。

## 验证命令

```bash
cd parsers/socket
go test -race ./... -count=3
golangci-lint run ./...
```
