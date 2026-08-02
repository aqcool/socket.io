# MongoDB Adapter 0.4.0 官方测试映射

## 基准与计数口径

- 官方仓库：`socketio/socket.io-mongo-adapter`
- 官方版本/tag：`@socket.io/mongo-adapter@0.4.0`
- 官方 commit：`eff10ab63ecfd5753d7985f46fd4278058f8570b`
- 官方测试源码：
  - `test/index.ts`：26 个可执行 `it()`；
  - `test/connection-state-recovery.ts`：10 个可执行 `it()`；
  - `test/util.ts`：仅辅助函数，0 个测试。
- 准确分母：**36 个可执行运行时测试**，没有 `it.skip()`。

本文件记录的是“官方行为到 Go 断言的映射闭合”，不是把上游 TypeScript 测试原样搬到 Go 后得到的测试运行报告，也不把 `testdata/official-interop/matrix.cjs` 的一条综合流程冒充 36 个测试。证据分为三层：

1. **共享行为回归（24 项）**：前 24 项属于 Cluster Adapter 通用语义；`adapters/adapter/official_258_cluster_test.go` 以进程内总线逐项断言广播、ACK、Room、FetchSockets 和 ServerSideEmit 等行为。这里的“共享层精确”只表示 API 输入、结果、错误和生命周期断言对应，不表示每一项都逐次经过真实 MongoDB。
2. **真实 Node↔Go 数据库边界（额外综合门禁）**：`TestOfficialMongoAdapterBilateralInterop` 使用官方 Node Adapter 0.4.0 和真实 MongoDB，双向验证普通/二进制广播、二进制广播 ACK、Room、FetchSockets、ServerSideEmit ACK、远程 Join、恢复、节点退出及滚动重启。它证明 BSON/Change Stream 边界可互操作，但不拆成 24 项，也不单独充当 36 项分母。
3. **MongoDB 专属精确断言**：本模块用 12 个具名真实 MongoDB 集成子测试逐项执行 Change Stream 生命周期 2 项和恢复 10 项。

因此准确结论是：**36/36 个官方行为已完成映射且都有自动化断言**，其中 24 项在共享内存总线回归，12 项逐项经过真实 MongoDB；另有 1 条不计入分母的官方 Node↔Go/真实 MongoDB 综合互操作门禁。上游 skip 0，宿主 N/A 0，已知功能缺口 0。不能将其表述成“上游 36 个 TypeScript 测试原样通过”或“36 项都逐项经过真实 MongoDB”。

## `test/index.ts`：26/26

| # | 官方测试名称 | 分类 | 可执行证据 |
|---:|---|---|---|
| 1 | `broadcasts to all clients` | 共享层精确 + Node↔Go | `TestOfficial258ClusterAdapter/broadcasts to all clients`；互操作矩阵双向断言文本、数字和 BSON Binary。 |
| 2 | `broadcasts to all clients in a namespace` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作 fixture 建立 `/custom` 并验证官方命名空间路由。 |
| 3 | `broadcasts to all clients in a room` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向验证 `shared-room`。 |
| 4 | `broadcasts to all clients except in room` | 共享层精确 | 同名共享层子测试逐个断言排除集合；Mongo emitter 的真实传输矩阵另验证 `except` BSON 结构。 |
| 5 | `broadcasts to local clients only` | 共享层精确 | 同名共享层子测试断言两个远端均不接收；`local` 标志不会进入 `DoPublish`。 |
| 6 | `broadcasts with multiple acknowledgements` | 共享层精确 + Node↔Go | 同名共享层子测试断言 3 个响应及请求清理；互操作矩阵双向执行广播 ACK。 |
| 7 | `broadcasts with multiple acknowledgements (binary content)` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向断言二进制 ACK 字节值。 |
| 8 | `broadcasts with multiple acknowledgements (no client)` | 共享层精确 | 同名共享层子测试断言无错误和空响应。 |
| 9 | `broadcasts with multiple acknowledgements (timeout)` | 共享层精确 | 同名共享层子测试断言错误和已收到的部分响应。 |
| 10 | `broadcasts with a single acknowledgement (local)` | 共享层精确 | 同名共享层子测试断言本地单 ACK 值。 |
| 11 | `broadcasts with a single acknowledgement (remote)` | 共享层精确 | 同名共享层子测试先 FetchSockets，再对 RemoteSocket 断言单 ACK 值。 |
| 12 | `makes all socket instances join the specified room` | 共享层精确 | 同名共享层子测试断言 3/3 socket 入房。 |
| 13 | `makes the matching socket instances join the specified room` | 共享层精确 | 同名共享层子测试断言只改变匹配 socket。 |
| 14 | `makes the given socket instance join the specified room` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向执行按远端 sid Join。 |
| 15 | `makes all socket instances leave the specified room` | 共享层精确 | 同名共享层子测试断言 3/3 socket 离房。 |
| 16 | `makes the matching socket instances leave the specified room` | 共享层精确 | 同名共享层子测试断言过滤条件和非匹配 socket 状态。 |
| 17 | `makes the given socket instance leave the specified room` | 共享层精确 | 同名共享层子测试断言仅目标 sid 离房。 |
| 18 | `makes all socket instances disconnect` | 共享层精确 | 同名共享层子测试断言 3 个真实连接均断开；真实 Mongo emitter 矩阵验证同一消息类型的 BSON 解码。 |
| 19 | `returns all socket instances` | 共享层精确 + Node↔Go | 同名共享层子测试断言 3 项及请求清理；互操作矩阵两方向断言跨 Node/Go 总数。 |
| 20 | `returns a single socket instance` | 共享层精确 | 同名共享层子测试逐项比较 handshake、data、rooms。 |
| 21 | `returns only local socket instances` | 共享层精确 | 同名共享层子测试断言仅本节点 sid。 |
| 22 | `sends an event to other server instances` | 共享层精确 + Node↔Go | 同名共享层子测试断言不回送本节点；互操作矩阵双向执行 ServerSideEmit。 |
| 23 | `sends an event and receives a response from the other server instances` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向断言 ServerSideEmitWithAck 响应。 |
| 24 | `sends an event but timeout if one server does not respond` | 共享层精确 | `TestOfficialMongoAdapter040ServerSideEmitTimeoutText` 精确断言 `timeout reached: only 1 responses received out of 2` 和部分响应 `[2]`；Mongo `requestsTimeout` 默认值及自定义值另有 option 回归。 |
| 25 | `should not throw when receiving a drop event` | 真实 Mongo 集成 | `TestOfficialMongoAdapter040ChangeStreamLifecycle/should not throw...` 先证明确认跨节点可达，Drop collection 后再断言 watch loop 重建并恢复投递；强于上游仅等待 100 ms 不抛错。 |
| 26 | `should resume the change stream upon reconnection` | 真实 Mongo 集成 | `TestOfficialMongoAdapter040ChangeStreamLifecycle/should resume...` 对单节点副本集强制 stepdown，待 driver 重连后双向断言 ServerSideEmit 恢复。 |

## `test/connection-state-recovery.ts`：10/10

测试入口为 `TestOfficialMongoAdapter040ConnectionStateRecovery`。它真实创建一份 capped collection 和一份带 `createdAt` TTL index 的普通 collection；每种存储模式执行同一组 5 项断言。

| # | 官方上下文 / 测试名称 | 分类 | 可执行证据 |
|---:|---|---|---|
| 27 | capped / `should restore the session` | 真实 Mongo 集成 | 断言 sid、pid、rooms、data 恢复。 |
| 28 | capped / `should restore any missed packets` | 真实 Mongo 集成 | 依次写入 sid room、全局、room1、room2、except room1、其他 namespace，最终只恢复值 `[1,2,3]` 且保留 ObjectID offset。 |
| 29 | capped / `should restore the session only once` | 真实 Mongo 集成 | 首次成功；第二次命中最新 tombstone 并返回 nil。 |
| 30 | capped / `should fail to restore an unknown session (invalid session ID)` | 真实 Mongo 集成 | 使用真实已知 offset 和未知 pid，返回 nil。 |
| 31 | capped / `should fail to restore an unknown session (invalid offset)` | 真实 Mongo 集成 | 非 ObjectID offset 返回错误且不恢复。 |
| 32 | TTL / `should restore the session` | 真实 Mongo 集成 | 断言 sid、pid、rooms、data 恢复。 |
| 33 | TTL / `should restore any missed packets` | 真实 Mongo 集成 | 与 capped 模式相同的六路过滤，最终只恢复 `[1,2,3]`。 |
| 34 | TTL / `should restore the session only once` | 真实 Mongo 集成 | `FindOneAndDelete` 原子消费，第二次返回 nil。 |
| 35 | TTL / `should fail to restore an unknown session (invalid session ID)` | 真实 Mongo 集成 | 使用真实已知 offset 和未知 pid，返回 nil。 |
| 36 | TTL / `should fail to restore an unknown session (invalid offset)` | 真实 Mongo 集成 | 非 ObjectID offset 返回错误且不恢复。 |

## 审计中发现并修复的功能缺口

1. **capped collection 恢复只能执行一次**：旧实现无条件使用 `FindOneAndDelete`，而 capped collection 禁止删除。现在与官方 0.4.0 一致：读取最新 session 后追加 tombstone，后续恢复命中 tombstone 并失败。
2. **不应自动在 capped collection 上创建 TTL index**：Builder 原先无条件创建 `expiresAt` TTL index，会破坏官方支持的 capped 模式。现在索引生命周期仍由调用方按官方方式选择 capped 或 `createdAt` TTL。
3. **跨语言二进制广播 ACK**：Socket.IO 客户端 ACK 在 Go 内是 `BufferInterface`；直接 BSON 编码会错误写成 `{buffer:{}}`。发布前现在递归转换为 BSON Binary 对应的 `[]byte`，Node→Go 与 Go→Node 二进制 ACK 均通过。
4. **普通/具名 Go 容器内的深层二进制**：旧归一化只递归 `bson.D`、`bson.M`、`bson.A`，普通 `[]any`、`map[string]any` 及具名 slice/map 内的 Buffer 仍可能被错误编码。现在递归处理这些容器，同时保留 `[]byte`、`bson.Raw`、ObjectID 等 BSON 标量类型。
5. **0.4.0 可观察超时契约**：Mongo 0.4.0 与新版共享 Cluster Adapter 的错误文字不同。共享层默认仍返回 PostgreSQL/新版所需的 `missing N responses`；Mongo 明确选择 `only X responses received out of Y`，并实现官方 `requestsTimeout`（默认 5 秒）配置，避免修复 Mongo 时回归 PostgreSQL 或 Redis。

## 验证命令

```bash
cd adapters/mongo/testdata/official-interop && npm ci
cd ../..

SOCKET_IO_MONGO_STEPDOWN_TEST=1 \
SOCKET_IO_MONGO_TEST_URI='mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true' \
go test -race ./adapter \
  -run '^(TestOfficialMongoAdapter040ConnectionStateRecovery|TestOfficialMongoAdapter040ChangeStreamLifecycle)$' \
  -v -count=1

SOCKET_IO_MONGO_OFFICIAL_INTEROP=1 \
SOCKET_IO_MONGO_TEST_URI='mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true' \
go test -race ./adapter -run '^TestOfficialMongoAdapterBilateralInterop$' -v -count=1

go test -race ./... -count=1
go test -race ./adapter -run '^TestOfficialMongoAdapter040ServerSideEmitTimeoutText$' -count=1
golangci-lint run ./...
git diff --check -- .

(cd ../adapter && go test -race ./... \
  -run '^TestOfficial258ClusterAdapter/serverSideEmit_times_out_if_one_server_does_not_respond$' \
  -count=1)
```

`SOCKET_IO_MONGO_STEPDOWN_TEST=1` 会短暂 stepdown 整个副本集，必须只在独立测试实例上运行；默认全模块测试会跳过这一项，避免干扰并行包测试。
