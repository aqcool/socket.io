# 官方 PostgreSQL external emitter 测试映射

## 版本边界

- `@socket.io/postgres-emitter@0.1.1`：Socket.IO 4.8.3 单仓库版本，共 12 项。
- 该官方 emitter 测试的历史接收端是 `@socket.io/postgres-adapter@0.1.1`。
- 当前 Adapter 互操作基线另为 `@socket.io/postgres-adapter@0.5.0`，不能与历史测试基线混写。

## 12/12 行为

`emitter-matrix.cjs` 逐项验证：

1. 向全部客户端广播（含二进制 attachment）
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

官方 emitter 0.1.1 省略顶层 `nsp`，Go Adapter 会根据当前 LISTEN 频道回填命名空间。反向测试使用独立 Node 进程验证 Go emitter（包含当前协议的顶层 `nsp`）可被当前官方 Adapter 0.5.0 接收。

所有结论来自 `SOCKET_IO_POSTGRES_OFFICIAL_INTEROP=1` 门控的真实 PostgreSQL 测试；默认跳过状态不计为通过。CI 或人工验收入口为模块根目录的 `make test-official-interop`，未提供开关和 URI 时会直接失败。
