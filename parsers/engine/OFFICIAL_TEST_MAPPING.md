# engine.io-parser 官方测试映射

## 对齐基线与准确分母

- npm 包：`engine.io-parser@5.2.3`
- npm `gitHead`：`0692bed4629047a26ae8fc96e3f8636e0a4d4b57`
- npm 完整性：`sha512-HqD3yTBfnBxIrbnM1DoD6Pcq8NECnh8d4As1Qgh0z5Gg3jRRIqijury0CL3ghu/edArpUYiYqQiDUQBIs4np3Q==`
- 官方源码：该提交下的 `packages/engine.io-parser`
- 计数方法：统计 `test/index.ts`、`test/node.ts`、`test/browser.ts` 中的运行时 `it(...)` 声明，不把格式检查、TypeScript 编译、benchmark 或工具函数计入运行时分母。

官方源码共有 **38 项互异运行时测试声明**：

| 官方测试文件 | 分类 | 数量 |
| --- | --- | ---: |
| `test/index.ts` | 共享：单包 2、payload 2、流编码 6、流解码 9 | 19 |
| `test/node.ts` | Node：单包 7、payload 1、流编码 1、流解码 1 | 10 |
| `test/browser.ts` | Browser：单包 5、payload 2、流编码 1、流解码 1 | 9 |
| **互异声明并集** | | **38** |

官方 Node 命令运行共享 19 + Node 10，共 29 项；浏览器构建会用 `test/browser.ts` 替换 `test/node.ts`，运行共享 19 + Browser 9，共 28 项。并集分母不是 29，也不是 28，而是 38。流测试在官方源码中受 `TransformStream`/`TextEncoder` 能力检测保护；Go 的 `io.Reader`/`io.Writer` 实现不需要该运行时条件。

## 38 项逐项映射

表内 Go 名称均位于 `parser/official_parser_test.go`。`✅` 表示协议可观察值已验证；Node/Browser 对象身份等语言运行时专属断言按下文的数据表示规则做 Go 等价验证。

### 共享测试（19/19）

| # | 官方 `it(...)` | Go 子测试 | 状态 |
| ---: | --- | --- | :---: |
| 1 | single packet / should encode/decode a string | `TestOfficialEngineIOParser523Shared/single_packet/should_encode/decode_a_string` | ✅ |
| 2 | single packet / should fail to decode a malformed packet | `.../should_fail_to_decode_a_malformed_packet` | ✅ |
| 3 | payload / should encode/decode all packet types | `.../payload/should_encode/decode_all_packet_types` | ✅ |
| 4 | payload / should fail to decode a malformed payload | `.../payload/should_fail_to_decode_a_malformed_payload` | ✅ |
| 5 | encoder stream / should encode a plaintext packet | `.../createPacketEncoderStream/should_encode_a_plaintext_packet` | ✅ |
| 6 | encoder stream / should encode a binary packet (Uint8Array) | `.../binary_packet_(Uint8Array)` | ✅ |
| 7 | encoder stream / should encode a binary packet (ArrayBuffer) | `.../binary_packet_(ArrayBuffer)` | ✅ |
| 8 | encoder stream / should encode a binary packet (Uint16Array) | `.../binary_packet_(Uint16Array)` | ✅ |
| 9 | encoder stream / should encode a binary packet (Uint8Array - medium) | `.../binary_packet_(Uint8Array_-_medium)` | ✅ |
| 10 | encoder stream / should encode a binary packet (Uint8Array - big) | `.../binary_packet_(Uint8Array_-_big)` | ✅ |
| 11 | decoder stream / should decode a plaintext packet | `.../createPacketDecoderStream/should_decode_a_plaintext_packet` | ✅ |
| 12 | decoder stream / should decode a plaintext packet (bytes by bytes) | `.../plaintext_packet_(bytes_by_bytes)` | ✅ |
| 13 | decoder stream / should decode a plaintext packet (all bytes at once) | `.../plaintext_packet_(all_bytes_at_once)` | ✅ |
| 14 | decoder stream / should decode a binary packet (ArrayBuffer) | `.../binary_packet_(ArrayBuffer)` | ✅ |
| 15 | decoder stream / should decode a binary packet (ArrayBuffer) (medium) | `.../binary_packet_(ArrayBuffer)_(medium)` | ✅ |
| 16 | decoder stream / should decode a binary packet (ArrayBuffer) (big) | `.../binary_packet_(ArrayBuffer)_(big)` | ✅ |
| 17 | decoder stream / payload length exceeds `maxPayload` | `.../length_of_the_payload_is_too_big` | ✅ |
| 18 | decoder stream / payload length is zero | `.../length_of_the_payload_is_invalid` | ✅ |
| 19 | decoder stream / payload length exceeds `Number.MAX_SAFE_INTEGER` | `.../length_is_bigger_than_Number.MAX_SAFE_INTEGER` | ✅ |

官方 big 用例分配 `123456789` 字节。Go 测试把它分解为两项等价证据：精确验证 `123456789` 的 64 位头字节及解码值，并以 `65536` 字节完整走通同一 64 位编码、读取和内容校验路径，避免 `-race` 每次保留超过 123 MB 的测试对象。

### Node 专属测试（10/10）

| # | 官方 `it(...)` | Go 子测试 | 状态 |
| ---: | --- | --- | :---: |
| 20 | should encode/decode a Buffer | `TestOfficialEngineIOParser523Node/...Buffer` | ✅ |
| 21 | should encode/decode a Buffer as base64 | `...Buffer_as_base64` | ✅ |
| 22 | should encode/decode an ArrayBuffer | `...ArrayBuffer` | ✅ |
| 23 | should encode/decode an ArrayBuffer as base64 | `...ArrayBuffer_as_base64` | ✅ |
| 24 | should encode a typed array | `...typed_array` | ✅ |
| 25 | should encode a typed array (with offset and length) | `...typed_array_(with_offset_and_length)` | ✅ |
| 26 | should decode an ArrayBuffer as ArrayBuffer | `...decode_an_ArrayBuffer_as_ArrayBuffer` | ✅ |
| 27 | should encode/decode a string + Buffer payload | `...string_+_Buffer_payload` | ✅ |
| 28 | encoder stream / should encode a binary packet (Buffer) | `...createPacketEncoderStream/...Buffer` | ✅ |
| 29 | decoder stream / should decode a binary packet (Buffer) | `...createPacketDecoderStream/...Buffer` | ✅ |

### Browser 专属测试（9/9）

| # | 官方 `it(...)` | Go 子测试 | 状态 |
| ---: | --- | --- | :---: |
| 30 | should encode/decode an ArrayBuffer | `TestOfficialEngineIOParser523Browser/...ArrayBuffer` | ✅ |
| 31 | should encode/decode an ArrayBuffer as base64 | `...ArrayBuffer_as_base64` | ✅ |
| 32 | should encode a typed array | `...typed_array` | ✅ |
| 33 | should encode/decode a Blob | `...Blob` | ✅ |
| 34 | should encode/decode a Blob as base64 | `...Blob_as_base64` | ✅ |
| 35 | should encode/decode a string + ArrayBuffer payload | `...string_+_ArrayBuffer_payload` | ✅ |
| 36 | should encode/decode a string + a 0-length ArrayBuffer payload | `...0-length_ArrayBuffer_payload` | ✅ |
| 37 | encoder stream / should encode a binary packet (Blob) | `...createPacketEncoderStream/...Blob` | ✅ |
| 38 | decoder stream / should decode a binary packet (Blob) | `...createPacketDecoderStream/...Blob` | ✅ |

## 实现对应关系

| 官方导出 | Go 对应 | 说明 |
| --- | --- | --- |
| `encodePacket` / `decodePacket` | `Parserv4().EncodePacket` / `DecodePacket` | 文本、原始二进制及 base64 `b...` |
| `encodePayload` / `decodePayload` | `Parserv4().EncodePayload` / `DecodePayload` | `0x1e` 分隔，二进制强制 base64；畸形项返回标准 error packet 并停止 |
| `encodePacketToBinary` | `EncodePacketToBinary` | 文本含类型前缀；二进制保持原字节 |
| `createPacketEncoderStream` | `EncodePacketFrame`、`PacketStreamEncoder` | 1/3/9 字节大端长度头，首字节最高位为 binary 标志 |
| `createPacketDecoderStream` | `PacketStreamDecoder` | 支持任意分块、连续多包、`maxPayload`、零长度和 JS 安全整数上限校验 |

## Node/Browser 与 Go 数据表示差异

这些差异不改变线上字节：

- Node 的 `Buffer`、Browser 的 `ArrayBuffer`/typed-array/`Blob` 在 Go 中统一由 `io.Reader` 输入，并以 `*types.BytesBuffer` 输出。官方 `binaryType` 只选择 JavaScript 容器类型；Go 不需要把相同字节复制到多种语言对象。
- 官方对某些 Buffer/typed-array 做对象身份断言；Go 验证容器类别和精确字节，不承诺读流被消费后的对象身份。
- typed-array 的 offset/length 由 Go byte slice 的切片表达；`Uint16Array` 用例同时验证官方测试所期望的小端字节。
- Browser `Blob` 由可组合的 `io.Reader` 表达，原始和 base64 路径均验证精确内容。

## 额外兼容范围

模块还保留 Engine.IO v3 parser 及其测试，但 v3 不计入 `engine.io-parser@5.2.3` 的 38 项分母。Go 对空 packet slice 提供同步的空 payload 往返；官方 JavaScript 的 callback API 在空数组时不会调用 callback，且官方 5.2.3 没有把这一点列为运行时测试。

## 验证命令

```bash
cd parsers/engine
go test -race ./...
golangci-lint run ./...
```
