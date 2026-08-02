# Redis Adapter 官方测试映射

本文档只对齐 Socket.IO 官方源码中的行为断言，不用项目内自定义用例数量代替官方分母。

## 固定基准与计数方法

- [`@socket.io/redis-adapter@8.3.0`](https://github.com/socketio/socket.io-redis-adapter/tree/5e82a3bff601f0d59f0ef3e80e338a0471115571)：官方提交 `5e82a3bff601f0d59f0ef3e80e338a0471115571`。
  - `test/index.ts`：24 个 `it`。
  - `test/specifics.ts`：7 个 `it`（Redis 4 与 Redis 3/ioredis 的两组 unknown-channel 断言分别计数）。
  - `test/custom-parser.ts`：1 个 `it`。
  - 唯一源码分母：32（31 个可执行断言 + 1 个上游 `it.skip`）。同一断言在 standalone、cluster、不同客户端矩阵中的重复运行不重复计数。
- [`@socket.io/redis-streams-adapter@0.3.1`](https://github.com/socketio/socket.io-redis-streams-adapter/tree/e4e38eb1065b04383749347f97b6442f60249a24)：官方提交 `e4e38eb1065b04383749347f97b6442f60249a24`。
  - `test/index.ts`：26 个 `it`。
  - `test/connection-state-recovery.ts`：4 个 `it`。
  - 唯一源码分母：30（29 个可执行断言 + 1 个上游 `it.skip`）。

状态含义：

- **Node↔Go**：真实启动官方 Node 适配器与本项目 Go 适配器，共用独立 Redis 实例并跨进程断言。
- **Go 等价**：断言属于共享 Adapter/ClusterAdapter 语义，执行逐条移植的 Go 官方等价测试；Redis 传输边界另由 Node↔Go 用例覆盖。
- **宿主 N/A**：官方断言依赖 JavaScript 可绕过类型系统的输入，Go 公共 API 无法表达。
- **上游 skip**：官方源码本身跳过，不计入可执行通过率。

## `@socket.io/redis-adapter@8.3.0`（32 项）

| ID | 官方断言 | 状态 | 本项目证据 |
| --- | --- | --- | --- |
| R01 | 向所有客户端广播（含二进制参数） | Node↔Go | `interop.cjs` 双向广播与二进制广播，运行标记 `R01` |
| R02 | 在自定义 namespace 中广播 | Node↔Go | `/custom` 官方 Node↔Go 双向广播，`R02` |
| R03 | 只向房间内客户端广播 | Node↔Go | 双向 room 广播，`R03` |
| R04 | 排除指定房间 | Node↔Go | `official-room-except` 正/反接收断言，`R04` |
| R05 | 多房间命中仍只投递一次 | Node↔Go | 两个目标房间加排除房间，逐 socket 精确计数，`R05` |
| R06 | 只向本节点客户端广播 | Node↔Go | Go `Local()` 发起，Node 客户端必须零投递，`R06` |
| R07 | 只向单个 socket 广播 | Node↔Go | 官方 Node 以 Go socket ID 定向，`R07` |
| R08 | 多客户端广播 ACK | Node↔Go | Node↔Go 双向 ACK 集合断言，`R08` |
| R09 | 二进制广播 ACK | Node↔Go | 双向 Buffer 字节精确断言，`R09` |
| R10 | 目标房间无客户端时返回空 ACK 数组 | Node↔Go | `official-empty-ack`，同时覆盖 `clientCount: 0` 线格式，`R10` |
| R11 | 部分客户端未 ACK 时超时并保留已有响应 | Node↔Go | `official-timeout-ack`，`R11` |
| R12 | 所有 socket 加入房间 | Node↔Go | Node 发起并跨 Go 节点轮询房间状态，`R12` |
| R13 | 匹配的 socket 加入房间 | Node↔Go | 双向 `socketsJoin` 房间筛选，`R13` |
| R14 | 指定 socket 加入房间 | Node↔Go | 用 Go socket ID 定向加入，`R14` |
| R15 | 所有 socket 离开房间 | Node↔Go | Node 发起并跨 Go 节点验证，`R15` |
| R16 | 匹配的 socket 离开房间 | Node↔Go | 双向 `socketsLeave` 房间筛选，`R16` |
| R17 | 指定 socket 离开房间 | Node↔Go | 用 Go socket ID 定向离开，`R17` |
| R18 | 断开所有 socket | Node↔Go | Go 先定向断开 Node，随后官方 Node `disconnectSockets()` 同时断开本地与 Go socket，并断言原因，`R18` |
| R19 | `fetchSockets()` 返回全部 socket 并清理请求 | Node↔Go | 双向数量断言及故障恢复后重复查询，`R19` |
| R20 | `fetchSockets()` 返回指定远端 socket 的 handshake/data/rooms | Node↔Go | 官方 Node 查询 Go socket 并断言 ID、data、handshake，`R20` |
| R21 | `local.fetchSockets()` 只返回本节点 | Go 等价 | `TestOfficial258ClusterAdapter/fetchSockets_returns_only_local_socket_instances` |
| R22 | `serverSideEmit()` 只发往其他服务实例 | Node↔Go | Go 本地 marker 与发起节点零投递断言，`R22` |
| R23 | `serverSideEmit()` 收集其他实例响应 | Node↔Go | 双向 server-side ACK，`R23` |
| R24 | 一个实例不响应时超时 | Node↔Go | Go 收到请求但故意不调用 ACK，官方 Node 必须报错，`R24` |
| R25 | 向数字类型 room 广播 | 宿主 N/A | 官方测试用 `@ts-ignore` 绕过 `Room` 字符串类型；Go `socket.Room` 是强类型字符串，公共 API 无数字 room |
| R26 | `close()` 后取消全部订阅 | 上游 skip | 官方 `it.skip("unsubscribes when close is called")`，其注释仍为 `TODO handle Redis cluster` |
| R27 | Redis 4：忽略未知 pattern channel | Go 等价 | `TestOfficialRedisAdapter830IgnoresUnknownChannels/redis4_pattern_subscription` |
| R28 | Redis 4：忽略未知 direct channel | Go 等价 | `.../redis4_direct_subscription` |
| R29 | Redis 3/ioredis：忽略未知 pattern channel | Go 等价 | `.../redis3_and_ioredis_pattern_subscription`；共享 Go 分派边界，断言 payload 不进入 parser |
| R30 | Redis 3/ioredis：忽略未知 direct channel | Go 等价 | `.../redis3_and_ioredis_direct_subscription` |
| R31 | `allRooms()` 汇总所有节点房间并清理请求 | Node↔Go | Go 发起、官方 Node 响应，断言双方 socket ID 与公共房间，`R31` |
| R32 | 自定义 parser 广播 | Node↔Go | `custom-parser-matrix.cjs` 使用真实 JSON parser 双向广播，动态输出 `1/1` |

结论：32/32 已分类；官方 31 个可执行断言中，30 个适用于 Go 宿主并全部有运行证据（主 Node↔Go 24 + custom parser 1 + unknown-channel Go 子项 4 + 共享等价 1），1 个数字 room 为宿主 N/A；另有 1 个官方上游 skip。可移植行为通过率为 **30/30**，真实功能缺口为 **0**。

## `@socket.io/redis-streams-adapter@0.3.1`（30 项）

| ID | 官方断言 | 状态 | 本项目证据 |
| --- | --- | --- | --- |
| S01 | 向所有客户端广播（含二进制参数） | Node↔Go | Streams 双向广播，`S01` |
| S02 | 在自定义 namespace 中广播 | Node↔Go | `/custom` 双向广播，`S02` |
| S03 | 只向房间内客户端广播 | Node↔Go | 双向 room 广播，`S03` |
| S04 | 排除指定房间 | Node↔Go | 正/反接收断言，`S04` |
| S05 | 只向本节点客户端广播 | Node↔Go | Go `Local()` 与远端零投递，`S05` |
| S06 | 多客户端广播 ACK | Node↔Go | 双向 ACK 集合，`S06` |
| S07 | 二进制广播 ACK | Node↔Go | 双向 Buffer 字节断言，`S07` |
| S08 | 目标房间无客户端时返回空 ACK 数组 | Node↔Go | `official-empty-ack`，`S08` |
| S09 | 部分客户端未 ACK 时超时并保留已有响应 | Node↔Go | `official-timeout-ack`，`S09` |
| S10 | 单个本地 socket ACK | Go 等价 | `TestOfficial258ClusterAdapter/broadcasts_with_a_single_acknowledgement_local` |
| S11 | 单个远端 socket ACK | Go 等价 | `.../broadcasts_with_a_single_acknowledgement_remote` |
| S12 | 所有 socket 加入房间 | Node↔Go | 跨节点状态断言，`S12` |
| S13 | 匹配的 socket 加入房间 | Node↔Go | 双向筛选，`S13` |
| S14 | 指定 socket 加入房间 | Node↔Go | 跨节点定向，`S14` |
| S15 | 所有 socket 离开房间 | Node↔Go | 跨节点状态断言，`S15` |
| S16 | 匹配的 socket 离开房间 | Node↔Go | 双向筛选，`S16` |
| S17 | 指定 socket 离开房间 | Node↔Go | 跨节点定向，`S17` |
| S18 | 断开所有 socket | Node↔Go | 官方 Node `disconnectSockets(true)` 断开所有 Node/Go 活跃 session，`S18` |
| S19 | 断开前广播的数据必须先送达 | Node↔Go | 对每个活跃 Node/Go session 断言先收到 `official-before-disconnect`、再收到 disconnect，`S19` |
| S20 | `fetchSockets()` 返回全部 socket 并清理请求 | Node↔Go | 双向及故障恢复后查询，`S20` |
| S21 | 返回指定远端 socket 的 handshake/data/rooms | Node↔Go | 官方 Node 查询 Go socket，`S21` |
| S22 | `local.fetchSockets()` 只返回本节点 | Go 等价 | `TestOfficial258ClusterAdapter/fetchSockets_returns_only_local_socket_instances` |
| S23 | `serverSideEmit()` 只发往其他实例 | Node↔Go | 发起节点零投递，`S23` |
| S24 | 收集其他实例的 server-side 响应 | Node↔Go | 双向 ACK，`S24` |
| S25 | 一个实例不响应时超时 | Go 等价 | `TestOfficial258ClusterAdapter/serverSideEmit_times_out_if_one_server_does_not_respond` |
| S26 | 有实例离开时请求仍成功 | 上游 skip | 官方 `it.skip("succeeds even if an instance leaves the cluster")` |
| S27 | 恢复 session，并保持原 socket ID | Node↔Go | Node session→Go 与 Go session→Node 双向恢复，`S27` |
| S28 | 只恢复匹配 room/except/namespace 的漏收包，offset 唯一 | Node↔Go | 精确恢复 `[1,2,3]`、排除 `[4,5,6]`，并校验 3 个 Redis offset，`S28` |
| S29 | 非法 session ID 不恢复 | Node↔Go | 官方客户端携带未知 PID 连接 Go，`recovered === false`，`S29` |
| S30 | 非法 offset 不恢复 | Node↔Go | 官方客户端携带合法 PID 与 `abc` offset 连接 Go，`recovered === false`，`S30` |

结论：30/30 已分类；29 个官方可执行断言全部有运行证据（主 Node↔Go 25 + 共享等价 4），1 个为官方上游 skip。可执行行为通过率为 **29/29**，真实功能缺口为 **0**。

## 本轮逐项审计修复

现有综合 interop 在扩充前没有覆盖空结果和 Sharded 私有房间路由，因此没有发现以下问题：

1. Legacy Redis response 的 `clientCount: 0`、`rooms: []`、`sockets: []` 会被 Go JSON `omitempty` 丢掉，官方 Node 请求端无法完成空目标 ACK、空远端 `fetchSockets()` 或 `allRooms()` 请求。现在 `RedisResponse.MarshalJSON()` 只在协议要求时显式保留这些零值，并有字节级回归。
2. 共享 ClusterAdapter 的 `BroadcastClientCount.clientCount: 0` 和 `FetchSocketsResponse.sockets: []` 同样会被 JSON/MessagePack 丢掉。字段现按官方协议强制存在；这会同时修正所有复用 ClusterAdapter 的非云适配器，非零值及 Go↔Go 解码语义不变。
3. Sharded 发布端使用官方“20 字符 Socket ID”启发式选择频道，但订阅端原来使用本地 `sids` 判断。由于当前 Go Socket ID 为 24 字符，两端会选择不同频道。发布与订阅现在统一使用同一官方长度规则，并有 20/24 字符及真实 Node↔Go 定向广播回归。

## 复现命令

测试必须指向独立 Redis，不能使用开发者已有的 `127.0.0.1:6379`：

```bash
cd adapters/redis/testdata/interop && npm ci
cd ../..
SOCKET_IO_REDIS_INTEROP_TEST=1 \
SOCKET_IO_REDIS_TEST_ADDR=127.0.0.1:<dedicated-port> \
SOCKET_IO_REDIS_TEST_PASSWORD=<dedicated-password> \
go test -mod=mod -race ./adapter \
  -run '^TestOfficialNodeRedisAdapterInterop$' -v -count=1 -timeout=6m

go test -mod=mod -race ./... -count=1
golangci-lint run ./...

cd ../adapter
go test -mod=mod -race ./... \
  -run '^TestOfficial258ClusterAdapter$' -v -count=1
```

`interop.cjs` 只在对应断言完成后才把 ID 加入 `officialSourceRowsExercised`；运行结果应分别动态输出 Redis 主矩阵 24 项、Streams 主矩阵 25 项。Redis 的 R32 由独立 custom-parser 进程动态输出 `1/1`，R27–R30 与共享 ClusterAdapter 的 5 项由上述 Go 测试实际执行，不以文档常量冒充测试结果。

## 2026-08-01 实际验证记录

- 三模式官方互操作命令：PASS，67.29 秒（全新独立 Redis 实例）。
  - Pub/Sub：28.62 秒，运行时动态登记 R01–R20、R22–R24、R31，共 24 项；独立 custom parser 动态输出 `1/1`。
  - Streams：30.14 秒，运行时动态登记 25 项：S01–S09、S12–S21、S23、S24、S27–S30。
  - Sharded：8.53 秒，动态登记 22 项；包括修复后的官方 Node→24 字符 Go socket ID 定向广播与全量断开。
- `TestOfficialRedisAdapter830IgnoresUnknownChannels`：4/4 子项 PASS。
- `TestOfficial258ClusterAdapter`：26/26 Go 官方等价子项 PASS；本映射从中引用 Redis 的 1 项和 Streams 的 4 项。
- Legacy JSON 零值/空集合回归：3/3 PASS。
- Cluster JSON/MessagePack 零值/空集合回归：2/2 PASS。
- `adapters/redis` 全模块 `go test -race ./...`：4 个 package PASS。
- `adapters/adapter` 全模块 `go test -race ./...`：PASS。
- 两个 Go module 的 `golangci-lint run ./...`：均为 `0 issues`。
- `interop.cjs`：`node --check` PASS，Prettier PASS；`git diff --check` PASS。
