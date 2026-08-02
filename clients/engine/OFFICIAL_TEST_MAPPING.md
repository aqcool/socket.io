# engine.io-client 6.6.6 官方测试映射

## 基准和分母

- 基准包：`engine.io-client@6.6.6`
- 官方标签：`engine.io-client@6.6.6`
- Git commit：`22cc483786a5084b8c6d8595e46f7d99d1587fea`
- 官方测试源码共有 **108 个有效 `it()` 声明**，另有 1 个显式 `it.skip()`。
- 默认 Node 测试入口实际注册 **94 个有效用例 + 1 个显式跳过用例**。其余声明属于 Blob、旧 IE/XDomainRequest、Worker/beforeunload 等浏览器条件分支。
- 具备 ArrayBuffer、Blob、WebSocket 和 Worker 的现代浏览器入口通常注册 **75 个有效用例**；旧浏览器会按能力切换分支，所以不存在一个适用于所有浏览器的固定执行数。
- 官方包没有独立的 `*.test-d.ts`/`tsd` 类型断言用例，故专用类型测试分母为 **0**；`npm test` 中的 `tsc` 是源码和声明生成的编译门禁。Go 侧以 `go test`、`go vet` 和 `golangci-lint` 作为类型/静态门禁。

状态说明：

- `等价已测`：已有明确的 Go 等价行为测试。
- `部分等价`：核心协议行为已测，但 JS 数据外形或特定运行时细节不同。
- `不适用`：仅属于浏览器、Node 事件循环或 JS 对象类型。
- `待补`：实现或一对一证据仍不完整，不能计为官方覆盖。

## 逐项映射

### `arraybuffer/polling.js`（4）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| receive binary data when bouncing it back (polling) | 等价已测 | `TestOfficialClientPollingTextAndBinaryRoundTrip` |
| receive binary data and a multibyte UTF-8 string (polling) | 等价已测 | 同一真实 Polling 会话验证 UTF-8 与原始字节 |
| receive binary data when forcing base64 (polling) | 等价已测 | `TestOfficialClientForceBase64RoundTrip/polling` 验证真实强制 Base64 回环；URI 测试同时验证 `b64=1` |
| merge binary packets according to maxPayload | 等价已测 | `TestOfficialClientMixedBinaryMaxPayloadSequence` 按官方 72/20/text20/20/72 组合验证分包；通用分包另由 `TestOfficialClientMaxPayloadBatching` 覆盖 |

### `arraybuffer/ws.js`（3）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| receive binary data when bouncing it back (ws) | 等价已测 | `TestOfficialClientWebSocketTextAndBinaryRoundTrip` |
| receive binary data and a multibyte UTF-8 string (ws) | 等价已测 | 同一真实 WebSocket 会话验证文本帧和二进制帧 |
| receive binary data while forcing base64 (ws) | 等价已测 | `TestOfficialClientForceBase64RoundTrip/websocket` 验证真实强制 Base64 WebSocket 回环 |

### `binary-fallback.js`（1）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| receive binary data when ArrayBuffer is unavailable | 不适用 | Go 没有可被移除的浏览器 `ArrayBuffer` 全局对象；字节以 `io.Reader`/`BytesBuffer` 表示 |

### `blob/polling.js`（3）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| receive binary as Blob (polling) | 不适用 | Blob 是浏览器返回外形；Go 使用 `BytesBuffer`，底层字节回环已测 |
| send Blob (polling) | 不适用 | Go 接受 `io.Reader`，没有 Blob 类型；等价原始字节发送已测 |
| merge Blob packets by maxPayload | 不适用 | Blob 外形不适用；通用 payload 分包为部分等价 |

### `blob/ws.js`（3）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| receive binary as Blob (ws) | 不适用 | Go 使用 `BytesBuffer`；真实二进制帧已测 |
| send Blob (ws) | 不适用 | Go 使用 `io.Reader`；真实二进制帧已测 |
| send Blob encoded as base64 (ws) | 不适用 | Blob 类型不适用；底层强制 Base64 字节回环已由 `TestOfficialClientForceBase64RoundTrip/websocket` 覆盖 |

### `connection.js`（13）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| connect to localhost (polling) | 等价已测 | `TestOfficialClientPollingTextAndBinaryRoundTrip` |
| connect to localhost (ws) | 等价已测 | `TestOfficialClientWebSocketTextAndBinaryRoundTrip` |
| receive multibyte UTF-8 with polling | 等价已测 | Polling 真实回环 |
| receive emoji | 等价已测 | Polling 真实回环 |
| do not send packets after close starts | 等价已测 | `TestOfficialClientNoPacketsAfterCloseBegins` |
| merge packets according to maxPayload | 等价已测 | `TestOfficialClientMaxPayloadBatching`、`TestOfficialClientMixedBinaryMaxPayloadSequence` 与 drain 顺序测试 |
| send an oversized first packet anyway | 等价已测 | `TestOfficialClientMaxPayloadBatching` |
| work in a Web Worker | 不适用 | 浏览器 Worker 环境专属 |
| defer close while upgrading | 等价已测 | `TestOfficialClientCloseWaitsForUpgradeOutcome` |
| close on upgradeError when close is deferred | 等价已测 | 同上 `upgradeError` 子用例 |
| do not send while deferred close is active | 等价已测 | closing 状态发送抑制测试 |
| send all buffered packets before deferred close | 等价已测 | `TestOfficialClientWriteBufferDrainsAfterTransportWrite`、`TestOfficialClientBurstWriteOrdering` 和 `TestOfficialClientMixedBurstWireFrames`；后两者同时防止 drain 前重复 flush 导致空帧 |
| close on beforeunload | 不适用 | 浏览器生命周期事件；Go 进程信号是不同的宿主机制 |

### `engine.io-client.js`（15）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| expose protocol number | 等价已测 | `Protocol`/`TestDefaultConstants` 及编译时 API |
| parse HTTP URI without port | 等价已测 | `TestOfficialClientURIAndHostParsing` |
| parse HTTPS URI without port | 等价已测 | 同上 |
| parse WSS URI without port | 等价已测 | 同上 |
| parse WSS URI with port | 等价已测 | 同上 |
| parse host option without port | 等价已测 | 同上 |
| parse host option with port | 等价已测 | 同上及 SocketOptions 测试 |
| honor addTrailingSlash=false | 等价已测 | `TestOfficialClientPathAndQueryParsing` |
| parse IPv6 URI without port | 等价已测 | URI 表驱动测试 |
| parse IPv6 URI with port | 等价已测 | URI 表驱动测试 |
| parse bracketed IPv6 host without port | 等价已测 | URI 表驱动测试 |
| parse secure bracketed IPv6 host | 等价已测 | URI 表驱动测试 |
| parse IPv6 host with explicit port | 等价已测 | SocketOptions 与 URI 组合测试 |
| parse unbracketed IPv6 host | 等价已测 | URI 表驱动测试 |
| generate random string | 等价已测 | `TestOfficialClientRandomString` 验证长度固定为 8 且连续值不重复；时间戳 URI 测试验证实际接线 |

### `node.js`（10）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| autoUnref: process stops | 不适用 | Go 在 `main` 返回时直接结束进程，没有 Node event-loop ref 语义 |
| autoUnref: polling-only process stops | 不适用 | 同上 |
| autoUnref: websocket-only process stops | 不适用 | 同上 |
| autoUnref=false keeps process alive | 不适用 | goroutine/timer 不采用 Node handle ref 模型 |
| merge binary packets by maxPayload | 等价已测 | `TestOfficialClientMixedBinaryMaxPayloadSequence` 一对一覆盖官方五包 72/20/text20/20/72 组合 |
| send cookies with withCredentials=true | 等价已测 | `TestOfficialClientPollingCookiesFollowWithCredentials` |
| do not send cookies with withCredentials=false | 等价已测 | 同上；同时修复了 Resty 默认 CookieJar 导致的泄漏 |
| parse simple Set-Cookie | 不适用 | 交给 Go 标准库 `net/http/cookiejar`，端到端 Cookie 测试已覆盖 |
| parse complex Set-Cookie | 不适用 | 同上，Go 标准库负责 Expires/Domain/Path |
| parse unusual cookie value | 不适用 | 同上；不复制官方 JS 私有 Cookie parser |

### `parseuri.js`（1）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| parse URI into JS-specific fields | 部分等价 | 构造器 URI、scheme-less host、IPv6、query 已测；Go 使用 `net/url`，不暴露 `pathNames/queryKey/userInfo` 的 JS 结果对象 |

### `socket.js`（14）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| filter upgrades to configured transports | 等价已测 | `TestOfficialClientFilterUpgrades` |
| do not mutate caller transports | 等价已测 | `TestOfficialClientOrderedTransportsAndCallerList` |
| error when no transports are available | 等价已测 | `TestOfficialClientNoTransportsAvailable`，错误文本与官方一致 |
| try second polling transport when enabled | 等价已测 | `TestOfficialClientTryAllTransports/same-name_polling`，验证两个同名 Polling 构造器仍按数组顺序逐一尝试 |
| try second websocket transport when enabled | 等价已测 | `TestOfficialClientTryAllTransports/same-name_websocket`，验证两个同名 WebSocket 构造器不会被名称映射折叠 |
| do not try second transport when disabled | 等价已测 | `TestOfficialClientTryAllTransports/disabled` |
| custom polling transport implementation | 等价已测 | `TestOfficialClientCustomTransportImplementations/polling`，真实服务端连接与关闭 |
| custom websocket transport implementation | 等价已测 | `TestOfficialClientCustomTransportImplementations/websocket`，真实服务端连接与关闭 |
| use window timeout by default | 不适用 | 浏览器 fake-window timer 专属；Go 使用 `time.Timer` |
| expose polling 413 close details | 等价已测 | `TestOfficialClientPollingHTTPErrorDetails/max_payload` |
| expose websocket 1009 close details | 等价已测 | `TestOfficialClientWebSocketCloseDetails` |
| expose unknown polling SID details | 等价已测 | `TestOfficialClientPollingHTTPErrorDetails/unknown_session` |
| expose unknown websocket SID details | 等价已测 | `TestOfficialClientWebSocketHandshakeErrorIsTransportError` |
| detect throttled/expired ping timer | 等价已测 | `TestOfficialClientPingTimeoutUsesIntervalPlusTimeout` 等关闭测试 |

### `transport.js`（25）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| remember successful websocket | 等价已测 | `TestOfficialClientRememberUpgradeAcrossInstances`；已改为跨实例共享状态 |
| do not remember when option is false | 等价已测 | `TestOfficialClientRememberUpgradeDisabledUsesFirstTransport` |
| export Transport constructor | 等价已测 | Go 导出 `Transport` 接口与 `MakeTransport` |
| export Polling and WebSocket constructors | 等价已测 | Go 导出 builder，构造/自定义 transport 测试覆盖 |
| generate HTTP URI | 等价已测 | `TestOfficialClientTransportURIs` |
| HTTP URI without default port | 等价已测 | 同上 |
| HTTP URI with explicit port | 等价已测 | 同上 |
| HTTPS URI without default port | 等价已测 | 同上 |
| HTTPS default port supplied as string | 等价已测 | Go 端口本来就是 string；同上 |
| HTTPS URI with explicit port | 等价已测 | 同上 |
| timestamp polling URI | 等价已测 | `TestOfficialClientTimestampAndBase64TransportURIs` |
| IPv6 polling URI | 等价已测 | URI 表驱动测试 |
| IPv6 polling URI with port | 等价已测 | URI 表驱动测试 |
| generate WS URI | 等价已测 | URI 表驱动测试 |
| generate WSS URI | 等价已测 | URI 表驱动测试 |
| timestamp WS URI | 等价已测 | timestamp/base64 测试 |
| IPv6 WS URI | 等价已测 | URI 表驱动测试 |
| IPv6 WS URI with port | 等价已测 | URI 表驱动测试 |
| accept Node Agent for WebSocket | 等价已测 | Go 等价入口是自定义 `WebSocketDialer`/代理/TLS，而不是 Node Agent |
| accept Node Agent for XHR | 等价已测 | Go 等价入口是 `HTTPClient`/`RoundTripper` |
| set extraHeaders for WebSocket | 等价已测 | `TestOfficialClientExtraHeadersReachServer`、custom dialer 测试 |
| set extraHeaders for XHR | 等价已测 | 同一测试的 Polling 子用例 |
| perMessageDeflate threshold=0 | 等价已测 | `TestOfficialClientPerMessageDeflateThreshold` |
| disable compression below threshold | 等价已测 | 同上 |
| browser transportOptions.extraHeaders | 等价已测 | `TestOfficialClientTransportSpecificExtraHeadersReachServer` 通过 Polling 真实请求验证 transportOptions 合并；Go 不区分浏览器实现 |

### `webtransport.mjs`（11）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| connect directly with WebTransport | 等价已测 | `TestOfficialClientWebTransportDirectTrafficAndHeartbeat`（真实 UDP/HTTP3） |
| upgrade to WebTransport | 等价已测 | `TestOfficialClientPollingToWebTransportUpgrade` |
| favor WebTransport over WebSocket | 等价已测 | `TestOfficialClientFavorsWebTransportOverWebSocket` 以真实 Polling/WebSocket/WebTransport 三路竞争验证优先选择 |
| send ping/pong | 等价已测 | 真实 Engine.IO heartbeat 回归 |
| handle server close | 等价已测 | `TestOfficialClientWebTransportCloseBothDirections/server` |
| handle client close | 等价已测 | `TestOfficialClientWebTransportCloseBothDirections/client` |
| plaintext client to server | 等价已测 | 直连与升级后双向消息 |
| plaintext server to client | 等价已测 | 直连与升级后双向消息 |
| binary client to server | 等价已测 | 真实 WebTransport 二进制帧回环 |
| binary server to client as ArrayBuffer | 部分等价 | Go 返回 `BytesBuffer`，字节内容已测；ArrayBuffer 外形不适用 |
| binary server to client as Buffer | 等价已测 | Go `BytesBuffer` 等价字节容器已测 |

### `xmlhttprequest.js`（5）

| 官方用例 | 状态 | Go 证据/边界 |
|---|---|---|
| IE8/9 XMLHttpRequest property shape | 不适用 | IE/JavaScript 对象外形专属 |
| IE8/9 XDomainRequest property shape | 不适用 | 同上 |
| IE8/9 cross-scheme without XDR | 不适用 | 同上 |
| IE8/9 cross-domain without XDR | 不适用 | 同上 |
| IE10/11 XMLHttpRequest property shape | 不适用 | 同上 |

## 不能伪装成 Go 等价通过的宿主差异

当前没有已知但尚未覆盖的、可移植到 Go 运行时的官方 6.6.6 行为项。仍标记为“不适用”或“部分等价”的项目，是浏览器 Blob/ArrayBuffer/Worker/beforeunload、旧 IE XHR、Node `autoUnref` 和 JS 专用对象外形；这些宿主差异不能用 Go 单测伪装成一对一通过。

因此，本表表达的是“官方每个声明均已分类，所有可移植行为都有 Go 等价证据”，而不是声称把官方 108 个 JavaScript 用例原样移植成了 108 个 Go 顶层测试。
