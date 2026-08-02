# PostgreSQL Adapter 0.5.0 官方测试映射

## 基准与计数口径

- 官方仓库：`socketio/socket.io-postgres-adapter`
- 官方版本/tag：`@socket.io/postgres-adapter@0.5.0`
- 官方 commit：`a9300c13376d1e953b3beaba5bf150eca2f365fb`
- 官方测试源码：
  - `test/index.ts`：26 项，其中 25 个可执行 `it()`、1 个 `it.skip()`；
  - `test/node-cluster.ts`：1 个可执行 `it()`；
  - `test/fixtures/primary.ts`、`test/util.ts`：辅助代码，0 个测试。
- 原始分母：**27 项 = 26 个上游可执行测试 + 1 个上游 skip**。

Go 没有 Node.js `cluster.isPrimary` / `cluster.fork()` 的 primary-worker IPC 宿主模型，进程或实例直接各自连接 PostgreSQL。因此 `test/node-cluster.ts` 的 1 项属于宿主 N/A，而不是缺少 PostgreSQL 跨进程功能。

本文件是官方行为到 Go 断言的映射审计，不是上游 TypeScript 测试原样执行报告。精确结果必须分开表达：

- **Go 可适用官方行为：25/25 已映射并通过对应自动化断言**；
- 上游主动跳过：1；
- Node.js 宿主 N/A：1；
- 原始 27 项分类闭合：**25 通过 + 1 上游 skip + 1 宿主 N/A = 27**；
- 不能写成“27/27 测试通过”，因为其中两项没有执行，也不能写成“25 项都逐项经过真实 PostgreSQL”。

与 Mongo 映射相同，现有综合互操作流程不充当 27 项分母。25 个通用行为由共享 `ClusterAdapterWithHeartbeat` 的同名回归在进程内总线上逐项断言；这里的“共享层精确”表示 API 语义、结果、错误和生命周期对应，不代表每项都逐次穿过 PostgreSQL。`TestOfficialPostgresAdapterBilateralInterop` 是额外的真实数据库边界门禁：它用官方 Node Adapter 0.5.0 和真实 PostgreSQL 双向验证 JSON NOTIFY、MessagePack attachment、20 KB 大消息、二进制广播 ACK、Room、FetchSockets、ServerSideEmit ACK、远程 Join、节点退出和滚动重启，但不拆成 25 条计数。

## `test/index.ts`：25 个执行，1 个上游 skip

| # | 官方测试名称 | 分类 | 可执行证据 |
|---:|---|---|---|
| 1 | `broadcasts to all clients` | 共享层精确 + Node↔Go | `TestOfficial258ClusterAdapter` 同名子测试；互操作矩阵双向验证 JSON/attachment 广播。 |
| 2 | `broadcasts to all clients in a namespace` | 共享层精确 + Node↔Go | 同名共享层子测试；真实 fixture 建立 `/custom`。 |
| 3 | `broadcasts to all clients in a room` | 共享层精确 + Node↔Go | 同名共享层子测试；双向验证 `shared-room`。 |
| 4 | `broadcasts to all clients except in room` | 共享层精确 | 同名共享层子测试逐个断言排除结果；Postgres emitter 真实矩阵另验证 `except` 消息格式。 |
| 5 | `broadcasts to local clients only` | 共享层精确 | 同名共享层子测试断言两个远端无消息；`local` 不进入 PostgreSQL publish。 |
| 6 | `broadcasts with multiple acknowledgements` | 共享层精确 + Node↔Go | 同名共享层子测试断言响应与清理；互操作矩阵双向执行广播 ACK。 |
| 7 | `broadcasts with multiple acknowledgements (binary content)` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵经 MessagePack attachment 双向断言二进制 ACK 字节。 |
| 8 | `broadcasts with multiple acknowledgements (no client)` | 共享层精确 | 同名共享层子测试断言空响应无错误。 |
| 9 | `broadcasts with multiple acknowledgements (timeout)` | 共享层精确 | 同名共享层子测试断言错误和部分响应。 |
| 10 | `broadcasts with a single acknowledgement (local)` | 共享层精确 | 同名共享层子测试断言本地值。 |
| 11 | `broadcasts with a single acknowledgement (remote)` | 共享层精确 | 同名共享层子测试先 FetchSockets，再断言 RemoteSocket ACK。 |
| 12 | `makes all socket instances join the specified room` | 共享层精确 | 同名共享层子测试断言 3/3 socket。 |
| 13 | `makes the matching socket instances join the specified room` | 共享层精确 | 同名共享层子测试断言匹配过滤。 |
| 14 | `makes the given socket instance join the specified room` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向执行按远端 sid Join。 |
| 15 | `makes all socket instances leave the specified room` | 共享层精确 | 同名共享层子测试断言全量 Leave。 |
| 16 | `makes the matching socket instances leave the specified room` | 共享层精确 | 同名共享层子测试断言匹配 socket 离房、非匹配保留。 |
| 17 | `makes the given socket instance leave the specified room` | 共享层精确 | 同名共享层子测试断言仅目标 sid 离房。 |
| 18 | `makes all socket instances disconnect` | 共享层精确 | 同名共享层子测试断言 3 个真实连接断开；Postgres emitter 真实矩阵验证同一消息类型。 |
| 19 | `sends a packet before all socket instances disconnect` | **上游 skip** | 官方源码使用 `it.skip()`；不计入 25 个可适用执行项，也不伪造通过。共享 adapter 的更上层精确测试已覆盖先发包后断开，但不改变上游 skip 分类。 |
| 20 | `returns all socket instances` | 共享层精确 + Node↔Go | 同名共享层子测试断言 3 项与请求清理；互操作矩阵断言 Node/Go 总数。 |
| 21 | `returns a single socket instance` | 共享层精确 | 同名共享层子测试比较 handshake、data、rooms。 |
| 22 | `returns only local socket instances` | 共享层精确 | 同名共享层子测试断言仅本节点 sid。 |
| 23 | `sends an event to other server instances` | 共享层精确 + Node↔Go | 同名共享层子测试断言不回送本节点；互操作矩阵双向执行。 |
| 24 | `sends an event and receives a response from the other server instances` | 共享层精确 + Node↔Go | 同名共享层子测试；互操作矩阵双向断言 ServerSideEmitWithAck。 |
| 25 | `sends an event but timeout if one server does not respond` | 共享层精确 | 同名共享层子测试精确断言 `timeout reached: missing 1 responses` 和部分响应；Mongo 0.4.0 的 legacy 格式通过适配器级 formatter 隔离，不改变 PostgreSQL 的官方可观察文本。 |
| 26 | `succeeds even if an instance leaves the cluster` | 共享层精确 | 同名共享层子测试在请求中关闭一个 adapter，断言剩余响应成功完成。PostgreSQL 实现的 `Close()` 发布 `ADAPTER_CLOSE`。 |

## `test/node-cluster.ts`：1 个宿主 N/A

| # | 官方测试名称 | 分类 | 说明 |
|---:|---|---|---|
| 27 | `@socket.io/postgres-adapter within Node.js cluster / should work` | **宿主 N/A** | 官方 fixture 启动 3 个 Node primary，每个 primary 再 `cluster.fork()` 3 个 worker，并用 `process.send()` 在 worker 与 primary 间转发。Go 没有这一运行时层；每个 Go 进程/实例直接运行 Adapter 并通过 LISTEN/NOTIFY 互通。真实 Node↔Go 的节点退出与滚动重启已验证部署层结果，但不冒充执行 Node `cluster` API。 |

## 官方范围外的 Go 扩展

官方 PostgreSQL Adapter 0.5.0 的 `doPublish()` 明确返回空 offset，并注释“不支持 connection state recovery”。本仓库新增的 PostgreSQL 持久化恢复属于向后兼容扩展，由 `TestPostgresCrossNodeConnectionStateRecovery` 验证；它不进入官方 27 项分母，也不用于抬高通过率。

## 验证命令

```bash
cd adapters/postgres/testdata/official-interop && npm ci
cd ../..

SOCKET_IO_POSTGRES_OFFICIAL_INTEROP=1 \
SOCKET_IO_POSTGRES_TEST_URI='postgres://postgres:socketio@127.0.0.1:5432/socketio?sslmode=disable' \
go test -race ./adapter -run '^TestOfficialPostgresAdapterBilateralInterop$' -v -count=1

SOCKET_IO_POSTGRES_TEST_URI='postgres://postgres:socketio@127.0.0.1:5432/socketio?sslmode=disable' \
go test -race ./... -count=1

golangci-lint run ./...
git diff --check -- .
```
