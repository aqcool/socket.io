# socket.io-adapter 2.5.8 官方运行时测试映射

## 基准与分母

- 官方仓库：`socketio/socket.io`
- 官方 tag：`socket.io-adapter@2.5.8`
- tag commit：`ac83bfad2eb8bdd1ceddd46ddb1b4ee692d7262d`
- 运行时测试来源：
  - `packages/socket.io-adapter/test/index.ts`：21 个未跳过的 `it()`
  - `packages/socket.io-adapter/test/cluster-adapter.ts`：27 个未跳过的 `it()`
  - `packages/socket.io-adapter/test/util.ts`：0 个测试，仅测试辅助函数
- 准确分母：**48 个运行时测试**，没有 `it.skip()`。

Go 侧的实际可执行枚举与分母一致：`TestOfficial258InMemoryAdapter` 含 21 个 `t.Run()`；`TestOfficial258ClusterAdapter` 含 26 个 `t.Run()`，加上独立的 `TestOfficial257BroadcastsToLocalClientWhenPublishAndReturnOffsetFails`，集群部分共 27 个。因此是 **21 + 27 = 48**，不是通过常量声明出来的覆盖数。

计数只包含官方 Mocha 运行时用例。官方 `npm test` 中的 Prettier、TypeScript 编译以及 Node.js 依赖加载不是运行时 `it()`，因此不进入 48 的分母；对应的 Go 门禁是 `gofmt`、编译、`go test -race` 和 `golangci-lint`。

以下映射均由可执行断言证明，不以 Go API 与 JavaScript API 同名作为通过依据。

## `test/index.ts`：21/21

所有表中测试都由 `TestOfficial258InMemoryAdapter` 的具名子测试执行，测试文件为 `official_258_in_memory_test.go`。

| # | 官方测试名称 | Go 可观察行为与证据 |
|---:|---|---|
| 1 | `should add/remove sockets` | `should add/remove sockets`：逐项断言 `rooms`、`sids`、`Del`、`DelAll` 的最终集合。 |
| 2 | `should return a list of sockets` | `should return a list of sockets`：3 个真实连接，断言全量、`r2` 和不存在的 `r4` 三种结果。 |
| 3 | `should return a list of rooms` | `should return a list of rooms`：断言已知 sid 的两个房间及未知 sid 返回 `nil`。 |
| 4 | `should exclude sockets in specific rooms when broadcasting` | 同名子测试：3 个真实连接，通过 outgoing 事件断言只有未排除客户端收到消息。 |
| 5 | `should exclude sockets in specific rooms when broadcasting to rooms` | 同名子测试：同时设置 `rooms` 与 `except`，断言交集过滤后仅目标客户端收到二进制事件。 |
| 6 | `should precompute the WebSocket frames when broadcasting` | 同名子测试：监听真实 Engine.IO `packetCreate`，断言存在预编码帧且内容为完整 `42["test",1]`。 |
| 7 | `fetchSockets / returns the matching socket instances` | `fetchSockets returns the matching socket instances`：3 个真实连接，断言返回 3 个 `SocketDetails`。 |
| 8 | `fetchSockets / returns the matching socket instances within room` | `fetchSockets returns matching socket instances within room`：`r1` 加 `except r2` 后只返回指定 sid。 |
| 9 | `should emit a 'create-room' event` | `should emit a create-room event`：断言首次建房只产生一次 `create-room(r1)`。 |
| 10 | `should not emit a 'create-room' event if the room already exists` | 同名子测试：第二个 sid 加入已有房间后不产生 `create-room`。 |
| 11 | `should emit a 'join-room' event` | `should emit a join-room event`：断言 `join-room(r1,s1)` 参数。 |
| 12 | `should not emit a 'join-room' event if the sid is already in the room` | 同名子测试：重复 `AddAll(s1,r1)` 不产生 `join-room`。 |
| 13 | `should emit a 'leave-room' event with del method` | 同名子测试：断言 `Del` 只产生 `leave-room(r1,s1)`。 |
| 14 | `should not throw when calling del twice` | 同名子测试：与官方一样，在 `leave-room` 回调内重入 `Del`，断言只回调一次且不递归。 |
| 15 | `should emit a 'leave-room' event with delAll method` | 同名子测试：断言 `DelAll` 产生 `leave-room(r1,s1)`。 |
| 16 | `should emit a 'delete-room' event` | 同名子测试：最后一个 sid 离开后只产生一次 `delete-room(r1)`。 |
| 17 | `should not emit a 'delete-room' event if there is another sid in the room` | 同名子测试：`s1` 离开而 `s2` 仍在时，不产生 `delete-room`。 |
| 18 | `should persist and restore session` | 同名子测试：持久化 `sid/pid/data/rooms`，广播取得真实 offset，再恢复并断言无漏包。 |
| 19 | `should restore missed packets` | 同名子测试：精确覆盖 all、room、except、ack-id、ACK 类型和 volatile，最终只恢复 `all`、`room`、`no except`，且每包保留 offset。 |
| 20 | `should fail to restore an unknown session` | 同名子测试：未知 pid 返回 `nil` 且无错误。 |
| 21 | `should fail to restore a known session with an unknown offset` | 同名子测试：使用真实已知 pid `def` 与未知 offset，返回 `nil`。官方源码此项误传 sid `abc` 作为 pid，Go 回归验证了测试标题所表达的更强语义。 |

### WebSocket 预编码的 Go 等价边界

官方第 6 项直接检查 Node.js `ws.Sender.frame()` 返回的两个内部 Buffer（RFC 6455 header 和 payload）。Go 使用 Gorilla WebSocket，RFC 6455 header 由 Gorilla 内部生成，适配器不能也不应暴露 Node `ws` 的私有数组结构，因此该内部结构本身为 N/A。Go 测试验证的是相同优化边界：编码只做一次，并通过 `WsPreEncodedFrame` 把完整 Engine.IO + Socket.IO payload 交给传输层。

## `test/cluster-adapter.ts`：27/27

除第 6 项外，所有表中测试都由 `TestOfficial258ClusterAdapter` 的同名子测试执行，测试文件为 `official_258_cluster_test.go`。fixture 启动 3 个真实 Socket.IO/Engine.IO HTTP 服务、3 个真实 EIO4 WebSocket 客户端和 3 个 `ClusterAdapterWithHeartbeat`；集群消息在每个节点交付前经过 `EncodeClusterMessage`/`DecodeClusterMessage`，避免用共享 Go 指针冒充真实传输。

| # | 官方测试名称 | Go 可观察行为与证据 |
|---:|---|---|
| 1 | `broadcasts to all clients` | 3 个节点都收到文本、数字和二进制参数；3 个真实 WebSocket 均读取二进制 header 与 `[3,4]` attachment。 |
| 2 | `broadcasts to all clients in a namespace` | 创建 `/custom` 的 3 个独立连接，从节点 0 广播后 3 个命名空间客户端都收到。 |
| 3 | `broadcasts to all clients in a room` | 只有远端 `room1` 客户端收到。 |
| 4 | `broadcasts to all clients except in room` | 只有两个不在 `room1` 的客户端收到。 |
| 5 | `broadcasts to local clients only` | 只有节点 0 本地客户端收到，两个远端通道保持为空。 |
| 6 | `broadcasts to local clients even when publishAndReturnOffset throws` | 独立 `TestOfficial257BroadcastsToLocalClientWhenPublishAndReturnOffsetFails`：真实客户端连接，自定义 `DoPublish` 返回错误，仍收到 `42["test",1]`，并断言失败发布确实执行一次。 |
| 7 | `broadcasts with multiple acknowledgements` | 3 个真实 socket 的待处理 ACK 被逐一应答，聚合得到 3 项；超时清理后 `ackRequests` 为 0。 |
| 8 | `broadcasts with multiple acknowledgements (binary content)` | 3 个二进制 `[]byte` 响应经过集群 codec 后仍全部保持二进制。 |
| 9 | `broadcasts with multiple acknowledgements (no client)` | 广播到空房间立即成功，响应数组为空。 |
| 10 | `broadcasts with multiple acknowledgements (timeout)` | 只应答 2/3，50ms 后返回错误和两个已收到的部分响应。 |
| 11 | `broadcasts with a single acknowledgement (local)` | 本地 `Socket.EmitWithAck` 返回单个值 `2`。 |
| 12 | `broadcasts with a single acknowledgement (remote)` | 节点 0 先 `FetchSockets` 得到远端 socket，再 `RemoteSocket.EmitWithAck`，返回单个值 `2`。 |
| 13 | `makes all socket instances join the specified room` | `SocketsJoin` 后 3/3 sockets 都有 `room1`。 |
| 14 | `makes the matching socket instances join the specified room` | 只有两个 `room1` 成员加入 `room2`。 |
| 15 | `makes the given socket instance join the specified room` | 以远端 sid 过滤后只有目标 socket 加入 `room3`。 |
| 16 | `makes all socket instances leave the specified room` | 集群操作后 3/3 sockets 都不含 `room1`。 |
| 17 | `makes the matching socket instances leave the specified room` | 只有两个 `room1` 成员离开 `room2`，非匹配 socket 保留房间。 |
| 18 | `makes the given socket instance leave the specified room` | 以远端 sid 过滤后只有目标 socket 离开 `room3`。 |
| 19 | `makes all socket instances disconnect` | 3 个真实 socket 均进入 disconnected 状态。 |
| 20 | `sends a packet before all socket instances disconnect` | 连续 broadcast + disconnect 后，3 个真实 WebSocket 读取到的首包均为 `42["bye"]`。 |
| 21 | `returns all socket instances` | 节点 0 获取 3 个远程表示，完成后 `customRequests` 为 0。 |
| 22 | `returns a single socket instance` | 按远端 sid 获取一项，逐项比较 handshake、data 和 rooms。 |
| 23 | `returns only local socket instances` | `Local().FetchSockets()` 只返回节点 0 的 sid。 |
| 24 | `sends an event to other server instances` | 两个远端 namespace 收到完整参数，本地 namespace 明确未触发。 |
| 25 | `sends an event and receives a response from the other server instances` | 两个远端分别响应 `2`、`"3"`，请求端无错误聚合两项。 |
| 26 | `sends an event but timeout if one server does not respond` | 一个远端响应、一个沉默，精确断言 `timeout reached: missing 1 responses` 和已收到的部分响应。 |
| 27 | `succeeds even if an instance leaves the cluster` | 一个远端响应、另一个在处理事件时关闭 adapter；心跳成员移除后请求立即成功且只返回有效响应。 |

## Node.js / TypeScript 专属内容

48 个 `it()` 中没有只能由 TypeScript 类型系统执行的项目，因此运行时分母没有排除项。以下仅是 fixture 或语言实现差异，不是缺失功能：

- Node `http.createServer()`、`EventEmitter`、`socket.io-client`：Go fixture 分别使用 `httptest.Server`、带锁的内存总线和真实 EIO4 WebSocket 客户端。
- Promise、`async/await`、`process.nextTick()`：属于 Node 调度方式；Go 测试通过同步消息交付、channel、超时和最终状态断言相同的先后关系。
- Node `Buffer`：Go 的协议等价类型为 `[]byte`/`types.BufferInterface`，并在真实 WebSocket attachment 与集群 codec 两个边界验证。
- Node `ws` 私有 frame 数组：只有其内部表示为 N/A；预编码优化和线上 payload 均已验证。

## 本次审计发现并修复的缺口

1. 重复 `Del`：通用 Go `Set.Delete()` 对不存在元素也返回 `true`，旧实现会重复发出 `leave-room`；在回调内重入时会无限递归。`inMemoryAdapter.Del/DelAll` 现在先检查成员存在性，只对真实删除发事件。
2. 远程单 ACK：`RemoteSocket.EmitWithAck()` 的参数列表在集群聚合时被提前压平，导致成功但响应为 `nil`。`ClusterAdapter.BroadcastWithAck` 现在在 `ExpectSingleResponse` 分支保留完整 ACK 参数列表。
3. 发布失败后的本地广播：生产控制流已经与 2.5.7 修复一致；新增真实客户端回归，防止重新引入提前返回。

## 验证命令

```bash
cd adapters/adapter
go test -race ./... -run '^(TestOfficial258InMemoryAdapter|TestOfficial258ClusterAdapter|TestOfficial257BroadcastsToLocalClientWhenPublishAndReturnOffsetFails)$' -count=1
go test -race ./...
golangci-lint run ./...
```
