# 官方 Redis external emitter 测试映射

## 锁定版本

- `@socket.io/redis-emitter@5.1.0`（16 个核心用例 + 1 个自定义 parser 用例）
- `@socket.io/redis-streams-emitter@0.1.1`（Socket.IO 4.8.3 单仓库，12 个用例）
- 接收端：`@socket.io/redis-adapter@8.3.0`、`@socket.io/redis-streams-adapter@0.3.1` 与本仓库 Go Adapter

## Redis Streams emitter：12/12

`emitter-matrix.cjs` 逐项验证：

1. 向全部客户端广播（含二进制）
2. 向自定义命名空间广播
3. 向 Room 广播
4. 排除 Room 广播
5. 全部 Socket 加入 Room
6. 匹配 Room 的 Socket 加入 Room
7. 指定 Socket ID 加入 Room
8. 全部 Socket 离开 Room
9. 匹配 Room 的 Socket 离开 Room
10. 指定 Socket ID 离开 Room
11. 断开全部 Socket
12. `serverSideEmit`

同一夹具还反向验证新增 Go `RedisStreamsEmitter` 到官方 Node Adapter，覆盖所有线路消息类型。

## Redis Pub/Sub emitter：17/17

`emitter-matrix.cjs` 覆盖官方 16 个核心用例：任意数据、`toJSON()`、全部广播修饰符与保留事件、根命名空间、自定义命名空间、自动补 `/`、Room、Socket ID 定向与排除、Join 两项、Leave 两项、Disconnect 两项和 `serverSideEmit`。

`custom-parser-matrix.cjs` 单独覆盖第 17 项：官方 JSON parser emitter → Go JSON parser Adapter，以及 Go JSON parser emitter → 官方 Adapter。它使用唯一频道前缀，不与默认 MessagePack 矩阵混用。

所有结论来自 `SOCKET_IO_REDIS_INTEROP_TEST=1` 门控的真实 Redis 测试；默认跳过状态不计为通过。CI 或人工验收入口为模块根目录的 `make test-official-interop`，未显式开启门控时会直接失败。
