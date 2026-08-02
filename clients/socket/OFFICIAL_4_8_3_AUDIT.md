# Socket.IO Client 4.8.3 功能与测试映射

## 基准与计数方法

- 上游：`socketio/socket.io` 的 `4.8.3` 标签，提交 `9978574e4f1d4e21593497f94c40053cd0fff359`。
- 范围：`packages/socket.io-client/test`。
- 运行时分母来自 `index.ts` 实际导入的 6 个测试文件：`url.ts` 9 项、`connection.ts` 48 项、`socket.ts` 48 项、`node.ts` 5 项、`connection-state-recovery.ts` 1 项、`retry.ts` 4 项，共 115 项。
- 类型分母来自 `typed-events.test-d.ts` 的 14 个 `it()`。
- 可重复枚举命令（在官方源码根目录执行）：

  ```bash
  for file in packages/socket.io-client/test/{url,connection,socket,node,connection-state-recovery,retry,typed-events.test-d}.ts; do
    printf '%s ' "$file"
    rg -c '^\s*it\(' "$file"
  done
  ```

这里不维护一个手写的“已覆盖 N/N”字段。覆盖结论只来自下面逐项映射以及实际测试结果，避免测试增加后数字仍然不变。

状态说明：

- `已验证`：有本模块测试；标为 `Node 4.8.3` 的用例还会连接真实 `socket.io@4.8.3` 服务端。
- `跨模块已验证`：公开能力由仓库内其他模块提供，并有该模块测试或编译契约；证据会明确指出模块边界。
- `下层模块`：属于 Engine.IO 传输或 Socket.IO parser 的分母，应由相应模块的官方矩阵验证，不能冒充本模块覆盖。
- `平台等价`：浏览器、Node 进程或 Promise/TypeScript 独有语义，Go 没有相同运行时概念。
- `待验证`：本地存在相应实现，但还没有与官方用例同级的断言。
- `功能缺口`：当前公开 API 或行为确实没有官方对应能力。

官方 Node 互操作测试默认跳过，运行方式：

```bash
SOCKET_IO_CLIENT_OFFICIAL_INTEROP=1 go test -race ./... -run '^TestOfficialClient483NodeInterop$' -count=1
```

## `url.ts`（9 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 7 | works with undefined | 平台等价 | Go 没有 `window.location`，`Connect` 要求显式 URI。 |
| 18 | works with relative paths | 平台等价 | Go 没有浏览器当前地址可用于补全相对路径。 |
| 29 | works with no protocol | 跨模块已验证 | `pkg/utils.Url` 在无浏览器 `location` 时按官方 Node 语义默认补 `https://`；`TestOfficialClient483LookupManagerSelection/protocol-less_URLs.../bare_host` 验证该规则经过 `Connect/lookup` 生效。 |
| 37 | works with no schema | 跨模块已验证 | `pkg/utils.Url` 将 `//host` 按无 `location` 的官方语义补成 HTTPS；`.../protocol_relative` 验证客户端边界。浏览器按页面协议补全的分支在 Go 中不适用。 |
| 45 | forces ports for unique url ids | 已验证 | `TestOfficialClient483LookupManagerSelection/default_ports_share_the_same_URL_identity`。 |
| 56 | identifies the namespace | 已验证 | `TestOfficialClient483LookupManagerSelection/URI_query_is_available_before_the_manager_opens`。 |
| 65 | works with ipv6 | 已验证 | `TestOfficialClient483LookupManagerSelection/IPv6_URLs_retain_namespace_and_default_port_identity`。 |
| 73 | works with ipv6 location | 平台等价 | 依赖浏览器 `location`。 |
| 86 | works with a custom path | 已验证 | `TestOfficialClient483LookupManagerSelection/different_engine_paths_get_different_managers`。 |

## `connection.ts`（48 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 10 | should connect to localhost | 已验证 | Node 4.8.3：`default namespace...`。 |
| 21 | should not connect when autoConnect option set to false | 已验证 | `TestNewManagerDefaults/Engine_is_nil_before_connect` 及互操作辅助函数显式 `Connect()`。 |
| 27 | should start two connections with same path | 已验证 | `TestOfficialClient483LookupManagerSelection/same_namespace_gets_a_new_manager`。 |
| 36 | should start two connections with same path and different querystrings | 已验证 | 同上，第二个 URI 带 query。 |
| 45 | should start two connections with different paths | 已验证 | `.../different_engine_paths_get_different_managers`。 |
| 54 | should start a single connection with different namespaces | 已验证 | `.../different_namespaces_reuse_the_default_multiplexed_manager`；Node 4.8.3：`namespace opened after...`。 |
| 64 | should work with acks | 已验证 | Node 4.8.3：服务端事件到 Go ACK 再回传服务端。 |
| 78 | should receive date with ack | 已验证 | Node 4.8.3 返回真实 JS `Date`，Go 收到 ISO 字符串。 |
| 89 | should work with false | 已验证 | Node 4.8.3 scalar/UTF-8 ACK。 |
| 101 | should receive utf8 multibyte characters | 已验证 | Node 4.8.3 scalar/UTF-8 ACK。 |
| 125 | should connect to a namespace after connection established | 已验证 | Node 4.8.3：`namespace opened after the shared manager is connected`。 |
| 141 | should open a new namespace after connection gets closed | 已验证 | `TestOfficialClient483OpensNamespaceAfterPreviousSocketClosed`：根 namespace 关闭后同一 Manager 成功打开 `/foo`。 |
| 160 | should reconnect by default | 已验证 | Node 4.8.3：`connection state recovery` 会自动重连。 |
| 174 | should reconnect manually | 已验证 | Node 4.8.3：`manual disconnect and reconnect`。 |
| 191 | should reconnect automatically after reconnecting manually | 已验证 | `TestOfficialClient483AutomaticReconnectStillWorksAfterManualReconnect`。 |
| 211 | should attempt reconnects after a failed reconnect | 已验证 | `TestOfficialClient483ReconnectFailureEventsAndSecondLoop` 连续执行两轮耗尽循环，每轮 attempts 均为 `[1,2]`。 |
| 238 | reconnect delay should increase every time | 已验证 | `TestOfficialClient483ReconnectDelayIncreasesEveryAttempt` 断言 100/200/400ms 指数退避下界。 |
| 276 | should not reconnect when force closed | 已验证 | `TestOfficialClient483ForceCloseControlsReconnectLoop/force_close_before_the_first_attempt`。 |
| 296 | should stop reconnecting when force closed | 已验证 | `.../force_close_stops_an_active_loop`：第一轮 attempt 中关闭后不再继续。 |
| 316 | should reconnect after stopping reconnection | 已验证 | `.../manual_connect_restarts_a_stopped_loop`。 |
| 334 | should stop reconnecting on a socket and keep to reconnect on another | 已验证 | `TestOfficialClient483ConnectionLine334StopsOneNamespaceWhileAnotherReconnects`：停用 root 后 `/asd` 独立恢复。 |
| 362 | should try to reconnect twice and fail when requested two attempts with immediate timeout and reconnect enabled | 已验证 | `TestOfficialClient483ReconnectFailureEventsAndSecondLoop` 严格断言 attempts `[1,2]` 与 `reconnect_failed`。 |
| 389 | should fire reconnect_* events on manager | 已验证 | 同一测试断言 `error`、`reconnect_attempt`、`reconnect_error`、`reconnect_failed` 的次数。 |
| 415 | should fire reconnecting (on manager) with attempts number when reconnecting twice | 已验证 | 同一测试断言 attempt 参数类型为 `uint64` 且每轮严格为 `1,2`。 |
| 441 | should not try to reconnect and should form a connection when connecting to correct port with default timeout | 已验证 | `TestOfficialClient483ConnectionLine441DefaultTimeoutConnectsWithoutReconnect`。 |
| 464 | should connect while disconnecting another socket | 已验证 | `TestOfficialClient483ConnectionLine464ConnectsWhileAnotherNamespaceDisconnects`，并断言复用原 Engine。 |
| 476 | should emit a connect_error event when reaching a Socket.IO server in v2.x | 已验证 | `TestOfficialClient483RejectsLegacyConnectPacketWithoutSID`。 |
| 494 | should not close the connection when disconnecting a single socket | 已验证 | `TestOfficialClient483ConnectionLine494SingleNamespaceDisconnectKeepsSharedEngine`：另一 namespace 与共享 Engine 保持连接，最后一个断开才关闭。 |
| 523 | should stop trying to reconnect | 已验证 | `TestOfficialClient483CanDisableReconnectLoop/disable_the_current_loop_after_reconnect_error`。 |
| 548 | should try to reconnect twice and fail when requested two attempts with incorrect address and reconnect enabled | 已验证 | `TestOfficialClient483ReconnectFailureEventsAndSecondLoop` 使用已释放的本地地址，严格两次后失败。 |
| 572 | should not try to reconnect with incorrect port when reconnection disabled | 已验证 | `TestOfficialClient483CanDisableReconnectLoop/disabled_from_construction`。 |
| 596 | should still try to reconnect twice after opening another socket asynchronously | 已验证 | `TestOfficialClient483ConnectionLine596AsyncSecondSocketKeepsTwoReconnectAttempts`。 |
| 656 | should use overridden setTimeout by default | 平台等价 | 官方断言 JavaScript 全局 timer 注入。 |
| 676 | should use native setTimeout with useNativeSetTimers | 平台等价 | Go 没有 JS 原生/覆写 timer 二选一。 |
| 699 | should emit date as string | 已验证 | Node 4.8.3：`Go time is serialized as an ISO date string`。 |
| 711 | should emit date in object | 已验证 | Node 4.8.3：嵌套在对象中的 Go `time.Time` 也被序列化为 ISO 字符串。 |
| 725 | should get base64 data as a last resort | 跨模块已验证 | Engine.IO 的 `TestOfficialClientForceBase64RoundTrip/polling` 以真实强制 Base64 会话验证原始字节回环，`TestOfficialClientTimestampAndBase64TransportURIs` 验证 `b64=1`；Socket 层的二进制 packet/ACK 传播另由本表 742、757、769 项验证。 |
| 742 | should get binary data (as an ArrayBuffer) | 已验证 | Node 4.8.3：binary payload ACK；Go 对应 `BufferInterface`。 |
| 757 | should send binary data (as an ArrayBuffer) | 已验证 | Node 4.8.3：binary payload ACK。 |
| 769 | should send binary data (as an ArrayBuffer) mixed with json | 已验证 | Node 4.8.3：nested binary ACK。 |
| 785 | should send events with ArrayBuffers in the correct order | 已验证 | Node 4.8.3：`binary attachment completes before the following event`；精确 binary→text 顺序在修复 Engine in-flight flush 门禁后 `-race -count=50` 通过。 |
| 803 | should send binary data (as a Blob) | 平台等价 | Go 没有浏览器 `Blob`；`BufferInterface` 为对应二进制载体。 |
| 815 | should send binary data (as a Blob) mixed with json | 平台等价 | 同上。 |
| 831 | should send events with Blobs in the correct order | 平台等价 | 同上。 |
| 846 | should reopen a cached socket | 已验证 | Node 4.8.3：inactive root 经 `Manager.Socket("/")` 返回同一实例、恢复 Active 并重新连接。 |
| 870 | should not reopen a cached but active socket | 已验证 | Node 4.8.3：重复获取 active root 返回同一实例，Engine `packetCreate` 只观察到一个 root CONNECT。 |
| 897 | should not reopen an already active socket | 已验证 | Node 4.8.3：root 与 `/custom` 重复获取后各自仅发送一个 CONNECT packet。 |
| 923 | should close the engine upon decoding exception | 已验证 | Node 4.8.3：注入坏 Socket.IO frame 后旧 Engine 进入 closed，Manager 自动换新 Engine 并触发 reconnect。 |

## `socket.ts`（48 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 6 | should have an accessible socket id equal to the server-side socket id (default namespace) | 已验证 | Node 4.8.3：`get-id`。 |
| 20 | should have an accessible socket id equal to the server-side socket id (custom namespace) | 已验证 | Node 4.8.3：`custom namespace`。 |
| 33 | clears socket.id upon disconnection | 已验证 | `TestOfficialClient483SocketLifecycleAndRecoveryState`。 |
| 47 | doesn't fire an error event if we force disconnect in opening state | 已验证 | `TestOfficialClient483ConnectionErrorBoundaries/force_disconnect_while_opening_suppresses_later_errors`，竞态模式 count=10。 |
| 60 | fire a connect_error event when the connection cannot be established | 已验证 | `.../unreachable_address_emits_connect_error`。 |
| 73 | fire a connect_error event on open timeout (polling) | 已验证 | `TestOfficialClient483OpenTimeoutByTransport/polling` 使用官方同值 `timeout=0`，断言 `connect_error` 严格携带 `timeout`。 |
| 89 | fire a connect_error event on open timeout (websocket) | 已验证 | `TestOfficialClient483OpenTimeoutByTransport/websocket`；Engine.IO 的可取消异步 dial 另由 client Engine 矩阵验证。 |
| 105 | doesn't fire a connect_error event when the connection is already established | 已验证 | `.../manager_errors_are_not_connect_error_after_the_namespace_connected`；自动恢复互操作另覆盖真实 Engine close。 |
| 121 | should change socket.id upon reconnection | 已验证 | Node 4.8.3：手工断开重连得到新 ID；恢复成功场景另验证 ID 保持。 |
| 142 | should enable compression by default | 已验证 | Node 4.8.3：`compression flag...` 的 Engine packetCreate。 |
| 156 | should disable compression | 已验证 | 同上，`Compress(false)`。 |
| 171 | query option: should accept an object (default namespace) | 已验证 | Node 4.8.3：`explicit query object on the default namespace`。 |
| 186 | query option: should accept a query string (default namespace) | 已验证 | Node 4.8.3 handshake 验证 URI query。 |
| 198 | query option: should accept an object | 已验证 | Node 4.8.3：`explicit query object on a custom namespace`。 |
| 213 | query option: should accept a query string | 已验证 | Node 4.8.3：`URI query string on a custom namespace`。 |
| 226 | query option: should properly encode the parameters | 已验证 | Node 4.8.3 收到解码后的 `a b`。 |
| 243 | auth option: should accept an object | 已验证 | Node 4.8.3 收到 static auth。 |
| 260 | auth option: should accept an function | 已验证 | Node 4.8.3：`dynamic auth provider`。 |
| 277 | should fire an error event on middleware failure from custom namespace | 已验证 | Node 4.8.3：`middleware rejection...`。 |
| 289 | should not try to reconnect after a middleware failure | 已验证 | 同一子测试监测 `reconnect_attempt`。 |
| 311 | should properly disconnect then reconnect | 已验证 | Node 4.8.3：`manual disconnect and reconnect`；另验证 server namespace disconnect 后需手工重连。 |
| 335 | should throw on reserved event | 已验证 | `TestOfficialClient483ReservedEvents`；Go 返回 error。 |
| 343 | should emit events in order | 已验证 | Node 4.8.3：连接前缓存的第一个事件 ACK 必须先于 `connect` handler 发出的第二个事件；与二进制顺序用例合跑 `-race -count=50` 通过。 |
| 365 | should emit an event and wait for the acknowledgement | 已验证 | 多个 Node 4.8.3 ACK 子测试。 |
| 377 | volatile packets: should discard a volatile packet when the socket is not connected | 已验证 | `TestOfficialClient483VolatilePacketIsDiscardedWhileDisconnected`。 |
| 394 | volatile packets: should discard a volatile packet when the pipe is not ready | 已验证 | `TestOfficialClient483VolatilePacketIsDiscardedWhileTransportIsNotWritable` 注入受控的非 writable Engine transport，断言既不写 Engine 也不进入 send buffer。 |
| 411 | volatile packets: should send a volatile packet when the socket is connected and the pipe is ready | 已验证 | Node 4.8.3：`volatile packet sends on a writable connection`。 |
| 427 | onAny: should call listener | 已验证 | `TestOfficialClient483AnyListeners`。 |
| 442 | onAny: should prepend listener | 已验证 | 同上。 |
| 464 | onAny: should remove listener | 已验证 | 同上。 |
| 486 | onAnyOutgoing: should call listener | 已验证 | 同上。 |
| 505 | onAnyOutgoing: should call listener with binary data | 已验证 | 同上。 |
| 524 | onAnyOutgoing: should prepend listener | 已验证 | 同上。 |
| 550 | onAnyOutgoing: should remove listener | 已验证 | 同上。 |
| 571 | timeout: should timeout after the given delay when socket is not connected | 已验证 | `TestOfficialClient483AckTimeoutRemovesBufferedPacketAndAccounting`。 |
| 586 | timeout: should timeout when the server does not acknowledge the event | 已验证 | Node 4.8.3：`compression flag and acknowledgement timeout`。 |
| 597 | timeout: should timeout when the server does not acknowledge the event in time | 已验证 | Node 4.8.3：`late acknowledgement is ignored after timeout`，并断言 callback 不会二次调用。 |
| 615 | timeout: should not timeout when the server does acknowledge the event | 已验证 | Node 4.8.3 的普通 ACK 均使用超时安全路径。 |
| 627 | timeout: should timeout when the server does not acknowledge the event (promise) | 平台等价 | Go `EmitWithAck` 返回 callback 注册器，不返回 Promise；callback 超时已测。 |
| 640 | timeout: should not timeout when the server does acknowledge the event (promise) | 平台等价 | 同上，成功 ACK 已测。 |
| 654 | timeout: should use the default timeout value | 已验证 | retry Node 4.8.3 子测试使用 `SetAckTimeout`。 |
| 668 | acknowledgement upon disconnection: should not ack upon disconnection (callback) | 平台等价 | Go ACK 签名始终包含 `error`，不存在 JS 的无错误参数 callback 分类。 |
| 689 | acknowledgement upon disconnection: should ack with an error upon disconnection (callback & timeout) | 已验证 | Node 4.8.3：`acknowledgement lifecycle across disconnect`。 |
| 710 | acknowledgement upon disconnection: should ack with an error upon disconnection (callback & ackTimeout) | 已验证 | Node 4.8.3：设置默认 `AckTimeout` 后，断线使待处理 ACK 以 `socket has been disconnected` 失败。 |
| 729 | acknowledgement upon disconnection: should ack with an error upon disconnection (promise) | 平台等价 | Go 无 Promise；等价的错误 callback 路径已由 689、710 项的真实 Node 4.8.3 断线场景验证。 |
| 747 | acknowledgement upon disconnection: should ack with an error upon disconnection (promise & timeout) | 平台等价 | 同上。 |
| 768 | acknowledgement upon disconnection: should not discard an unsent ack (callback) | 已验证 | Node 4.8.3：断开后缓存带 ACK packet，重连后收到 ACK。 |
| 792 | throttled timer: should buffer the event and send it upon reconnection | 已验证 | `TestOfficialClient483ExpiredPingBuffersUntilReconnect` 断言过期 heartbeat 时 packet 只入 send buffer，恢复连接后恰好写出一次；Engine 的实际过期判定由 `TestOfficialClientPingTimeoutUsesIntervalPlusTimeout` 验证。 |

## `node.ts`（5 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 14 | should stop once the timer is triggered | 平台等价 | Go runtime 的 timer/goroutine 不会像 Node timer 一样阻止进程退出。 |
| 18 | should stop once the timer is triggered (even when trying to reconnect) | 平台等价 | 同上。 |
| 22 | should stop once the timer is triggered (polling) | 平台等价 | 同上。 |
| 26 | should stop once the timer is triggered (websocket) | 平台等价 | 同上。 |
| 30 | should not stop with autoUnref set to false | 平台等价 | Go 无 Node event-loop ref/unref 生命周期。 |

## `connection-state-recovery.ts`（1 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 6 | should have an accessible socket id equal to the server-side socket id (default namespace) | 已验证 | Node 4.8.3：`connection state recovery`，断线后 ID 保持且 `Recovered()` 为 true。 |

## `retry.ts`（4 项）

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 6 | should preserve the order of the packets | 已验证 | `TestOfficialClient483RetryQueuePreservesOrder`；Node 4.8.3 retry 子测试。 |
| 57 | should fail when the server does not acknowledge the packet | 已验证 | `TestOfficialClient483RetryQueueUsesOneEntryAndRetries`；Node 4.8.3 实测两次到达后失败。 |
| 93 | should not drain the queue while the socket is disconnected | 已验证 | Node 4.8.3 retry 子测试在 `Connect()` 前先 `Emit()`。 |
| 114 | should not emit a packet twice in the 'connect' handler | 已验证 | Node 4.8.3 对 `retry-never` 的到达次数严格等于 `retries + 1`；修复了 `fromQueue` 丢失导致的递归入队。 |

## `typed-events.test-d.ts`（14 项）

这里需要区分两层 API：核心 `clients/socket.Socket` 保留 Go 风格的动态 `EventListener func(...any)` / `Emit(...any)`，没有把 TypeScript 的泛型 event-map 直接塞进 Socket 方法；仓库的可选 `typed` 模块则用 `Event[Request, Response]`、方向声明和生成的客户端包装函数提供 Go 等价的静态契约。官方的多位置参数通常建模为一个 Request 结构体，ACK 建模为 Response；用户需要显式使用生成包，这是 API 边界，不是协议能力缺失。

`typed/compile_contract_test.go` 同时编译正例和预期失败的反例；生成代码契约还验证 ClientToServer 只生成 `EmitX`、ServerToClient 只生成 `OnX/HandleX`，以及 ACK 返回类型。核心动态 API 与这个可选强类型层的结论分别写在表中。

| 行 | 官方用例 | 状态 | 本地证据或差距 |
|---:|---|---|---|
| 22 | no event map / on: infers correct types for listener parameters of reserved events | 跨模块已验证 | 核心 listener 是动态的；`typed.OnConnect` 固定零参数，`OnConnectError` 固定 `error`，`OnDisconnect` 将 reason 限定为 `DisconnectReason`。运行时测试及生成代码正向编译契约均已验证。 |
| 37 | no event map / on: infers 'any' for listener parameters of other events | 已验证 | 核心 `types.EventListener` 明确为 `func(...any)`，多个 listener/互操作测试用不同参数运行。 |
| 47 | no event map / on: infers 'any' for listener parameters of other events using enums | 平台等价 | Go 可将自定义 string enum 显式转换为 `types.EventName`；listener 参数仍为 `...any`。 |
| 69 | no event map / emit: accepts any parameters | 已验证 | 多个 unit/Node 互操作用例以不同参数类型调用 `Emit`。 |
| 89 | no event map / emitWithAck: accepts any parameters | 已验证 | 多个 Node 4.8.3 子测试调用 `EmitWithAck`。 |
| 113 | single event map / on: infers correct types for listener parameters | 跨模块已验证 | `typed.On` 从 `Event[Request, Response]` 推导 handler 的 Request；正向编译 fixture 验证。生成包装可进一步给出命名的 `OnX/HandleX`。 |
| 125 | single event map / on: does not accept arguments of wrong types | 跨模块已验证 | 负向编译 fixture 传入错误 handler 类型并确认编译器拒绝。 |
| 137 | single event map / emit: accepts arguments of the correct types | 跨模块已验证 | `typed.Emit` 与生成的 `EmitX` 接收确定的 Request 类型；正向编译 fixture 验证。 |
| 143 | single event map / emit: does not accept arguments of the wrong types | 跨模块已验证 | 负向 fixture 的错误 request 类型无法编译。 |
| 173 | listen and emit event maps / on: infers correct types for listener parameters | 跨模块已验证 | `Generate` 对 ServerToClient 事件生成强类型 `OnX/HandleX`；生成代码正向契约验证 handler 参数。 |
| 183 | listen and emit event maps / on: does not accept emit events | 跨模块已验证 | ClientToServer 事件不会生成 `OnX`；生成代码负向契约以 `undefined: OnOutgoing` 验证方向隔离。 |
| 193 | listen and emit event maps / emit: accepts arguments of the correct types | 跨模块已验证 | ClientToServer 事件生成强类型 `EmitX`；生成代码正向契约验证。 |
| 216 | listen and emit event maps / emit: does not accept arguments of wrong types | 跨模块已验证 | 生成代码负向契约同时验证错误 request 类型与 `undefined: EmitIncoming`。 |
| 229 | listen and emit event maps / emitWithAck: accepts arguments of the correct types | 跨模块已验证 | ACK 事件生成 `EmitX(ctx, emitter, Request) (Response, error)`；正例接收 `bool`，负例把返回值赋给 `string` 时编译失败。 |

## 独立边界：`socket.io-component-emitter`（16 项）

这 16 项来自 `packages/socket.io-component-emitter/test/emitter.js`，是 Socket.IO client 的依赖契约，但不是 `packages/socket.io-client/test` 的组成部分，因此不并入前面的 115 项运行时分母。`Socket` 和 `Manager` 嵌入的是根模块 `pkg/types.EventEmitter`；本目录只增加消费边界测试，不修改该底层模块。

| 行 | 官方用例 | 状态 | 本地证据或底层差距 |
|---:|---|---|---|
| 12 | Custom / with Emitter.call(this): should work | 平台等价 | Go 以组合/嵌入替代 JS prototype；`MakeSocket()` 会安装可用的 EventEmitter。 |
| 22 | .on(event, fn): should add listeners | 已验证 | `TestOfficialComponentEmitterClientBoundary/listeners_keep_registration_order...` 验证参数与注册顺序。 |
| 41 | .on(event, fn): should add listeners for events which are same names with methods of Object.prototype | 已验证 | 同一测试验证 `constructor` 与 `__proto__`。 |
| 61 | .once(event, fn): should add a single-shot listener | 已验证 | `.../once_can_be_removed_by_its_original_listener`。根模块另有 `TestEventsOnce`、`TestConcurrentOnce`。 |
| 79 | .off(event, fn): should remove a listener | 已验证 | client boundary removal 测试；根模块 `TestRemoveListener`。 |
| 95 | .off(event, fn): should work with .once() | 已验证 | client boundary 明确用原始 listener 删除 Once wrapper。 |
| 110 | .off(event, fn): should work when called from an event | 已验证 | `.../removal_during_emit_uses_the_current_listener_snapshot`。 |
| 129 | .off(event): should remove all listeners for an event | 已验证 | `.../event_and_global_listener_removal` 使用 `RemoveAllListeners(event)`。 |
| 146 | .off(event, fn): should remove event array to avoid memory leak | 已验证（`pkg/types`） | `TestRemoveListenerDeletesEmptyEvent`；最后 listener 删除后 event key 同步清除。 |
| 158 | .off(event, fn): should only remove the event array when the last subscriber unsubscribes | 已验证（`pkg/types`） | `TestConcurrentLastRemovalDoesNotDropNewListener` 另验证最后删除与并发 `On` 不会产生孤儿 Slice。 |
| 174 | .off(): should remove all listeners | 已验证 | client boundary 使用 `Clear()`；这是 Go 的无参 `off()` 等价 API。 |
| 198 | .listeners(event): when handlers are present, should return an array of callbacks | 已验证 | client boundary 与根模块 `TestEventsOnce`。 |
| 207 | .listeners(event): when no handlers are present, should return an empty array | 平台等价 | Go 返回 nil slice，`len==0`；不是 JS 的非 nil array 对象。 |
| 216 | .hasListeners(event): when handlers are present, should return true | 平台等价 | Go 未暴露同名方法，以 `ListenerCount(event) > 0` 等价表达；client boundary 已验证。 |
| 224 | .hasListeners(event): when no handlers are present, should return false | 平台等价 | 同上，`ListenerCount(event) == 0`。 |
| 233 | Emitter(obj): should mixin | 平台等价 | Go 用接口组合；`Socket`/`Manager` 嵌入 `types.EventEmitter`，client boundary 实际触发成功。 |

底层额外证据：`pkg/types/events_test.go` 还有 `TestOnceRemovesListenerBeforeInvocation`，以及并发 add/emit/once/remove/remove-all 的 race 用例；这些是 Go 并发环境的增强项，不属于官方 16 项分母。

## 本轮由映射直接发现并修复的问题

1. `multiplex` 未设置时被当成 `false`，与官方默认 `true` 相反，导致不同 namespace 无法复用 Manager。
2. 缓存命中仍会先执行 `NewManager(...)`，在 `autoConnect=true` 时产生一条无引用的额外连接。
3. URI query 在 Manager/Engine 创建后才写入 options，首个握手可能拿不到 query。
4. retry packet 丢失 `fromQueue=true`，连接后重发会递归创建新的 retry packet。
5. 默认压缩标志没有明确传入 Engine packet；现在与官方一样显式为 `true`。
6. ACK 超时移除 sendBuffer packet 时没有同步更新字节统计。
7. `CONNECT_ERROR` 的 `data` 字段被错误地当成必填；官方只要求 `message`。
