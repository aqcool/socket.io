# `@socket.io/admin-ui@0.5.1` 官方测试映射

## 对齐基线

- 官方包：`@socket.io/admin-ui@0.5.1`
- 官方提交：`232d87af04777b108725bd70c248b5e929a5057d`
- 官方测试源：`test/index.ts`、`test/events.ts`
- 统计口径：按源码中唯一的 `it(...)` 测试定义计数，共 21 项；`index.ts` 中 Socket.IO v3/v4 参数化运行不重复计入分母。
- 互操作客户端：官方 `socket.io-client@4.8.3`，由 `testdata/official-admin-ui/package-lock.json` 锁定。

Node 夹具不是只返回一个汇总成功标记。每一项都完成对应字段、事件参数、二进制数据、ACK、时间戳或状态变化断言后，才把编号加入 `verifiedCases`；Go 测试还会精确比较预期的 18 个 Node 可执行编号，缺项或多项都会失败。

## 21 项映射

| 编号 | 官方唯一测试 | 本项目覆盖位置 | 结果 |
| ---: | --- | --- | --- |
| 1 | creates an admin namespace | `matrix.cjs`：默认 `/admin` 连接及完整功能清单 | 已验证 |
| 2 | creates an admin namespace with custom name | `matrix.cjs`：自定义 `/custom` 连接 | 已验证 |
| 3 | should work with `io.listen()` | Go 没有 Node `io.listen()` 宿主 API；`startAdminInteropServer` 在创建 HTTP listener 前调用 `Instrument`，覆盖等价的“先插桩、后监听”生命周期 | 不适用（宿主 API N/A） |
| 4 | prevents anonymous connection | `matrix.cjs`：匿名连接必须收到 `invalid credentials` | 已验证 |
| 5 | allows connection with valid credentials | `matrix.cjs`：bcrypt 凭据登录并校验 16 位 session ID | 已验证 |
| 6 | allows connection with a valid session ID | `matrix.cjs`：使用已保存 session ID 重连 | 已验证 |
| 7 | prevents connection with an invalid session ID | `matrix.cjs`：无效 session ID 必须被拒绝 | 已验证 |
| 8 | returns the list of supported features | `matrix.cjs`：精确比较 v4 的 9 个功能及顺序 | 已验证 |
| 9 | readonly mode supported features | `matrix.cjs`：精确比较 `AGGREGATED_EVENTS`、`ALL_EVENTS` | 已验证 |
| 10 | production mode supported features | `matrix.cjs`：精确比较仅 `AGGREGATED_EVENTS` | 已验证 |
| 11 | returns all sockets upon connection | `matrix.cjs`：校验应用 socket 与 admin socket 的 ID、命名空间和数量 | 已验证 |
| 12 | emits administrative events | `matrix.cjs`：连接序列化、握手字段、初始/显式进房、离房、断开及 ISO 时间戳 | 已验证 |
| 13 | emits event when `socket.data` is updated | `matrix.cjs`：校验初始 data 与更新后的嵌套数据 | 已验证 |
| 14 | performs administrative tasks | `matrix.cjs`：逐项校验 emit、join、leave、disconnect 及房间状态 | 已验证 |
| 15 | supports dynamic namespaces | `matrix.cjs`：正则父命名空间 `/dynamic-101` | 已验证 |
| 16 | InMemoryStore works | `TestOfficialAdminUI051InMemoryStore` | 已验证 |
| 17 | RedisStore works | `TestOfficialAdminUI051RedisStore`：真实 Redis 查找/保存；另校验默认键前缀与 TTL、可配置边界 | 已验证 |
| 18 | tracks events sent to client | `matrix.cjs`：事件名、标量、Buffer、socket ID、ISO 时间戳 | 已验证 |
| 19 | tracks events sent to client with ACK | `matrix.cjs`：ACK 往返且管理事件中剥离 ACK 回调 | 已验证 |
| 20 | tracks events received from client | `matrix.cjs`：事件名、标量、Buffer、socket ID、ISO 时间戳 | 已验证 |
| 21 | tracks events received from client with ACK | `matrix.cjs`：ACK 返回值及管理事件参数精确比较 | 已验证 |

结论：21/21 已映射，其中 20 项适用于 Go 并有运行时断言，1 项仅属于 Node 宿主 API，明确标为 N/A，不计作通过。

## RedisStore 官方等价语义

`RedisStore` 使用官方相同的默认值：键名为 `socket.io-admin#<sessionId>`，TTL 为 86400 秒。`RedisStoreOptions` 可配置前缀、TTL 和 context；保存通过带过期时间的单条 Redis `SET` 原子完成。单元测试覆盖 nil/typed-nil client、负 TTL、默认值和自定义值，真实 Redis 测试覆盖不存在、保存后存在、默认 TTL 和自定义 TTL。

## 复现命令

```sh
cd instrumentation/testdata/official-admin-ui
npm ci

cd ../..
go test ./...
SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP=1 \
  go test -run TestOfficialAdminUI051ProtocolInterop -count=1 -v

# 使用专用测试 Redis；不要指向生产实例
SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP=1 \
SOCKET_IO_ADMIN_UI_REDIS_ADDR=127.0.0.1:6379 \
  go test -run TestOfficialAdminUI051RedisStore -count=1 -v
```
