# 官方 MongoDB external emitter 测试映射

## 锁定版本

- `@socket.io/mongo-emitter@0.2.0`，共 12 项。
- 当前接收端：`@socket.io/mongo-adapter@0.4.0` 与本仓库 Go Adapter。
- 官方 emitter 仓库自身的历史开发依赖是 Mongo Adapter 0.1.0；本矩阵额外使用当前 0.4.0 做反向验证。

## 12/12 行为

`emitter-matrix.cjs` 在真实副本集上逐项验证：

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

同一夹具还反向验证 Go emitter 到当前官方 Mongo Adapter 0.4.0，覆盖广播、命名空间、Room/Except、Join/Leave、Disconnect 和 `serverSideEmit` 的全部线路消息类型。

所有结论来自 `SOCKET_IO_MONGO_OFFICIAL_INTEROP=1` 门控的真实 MongoDB 副本集测试；默认跳过状态不计为通过。CI 或人工验收入口为模块根目录的 `make test-official-interop`，未提供开关和 URI 时会直接失败。
