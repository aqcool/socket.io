# Socket.IO 客户端兼容性

本项目的服务端同时实现 Engine.IO 3 和 Engine.IO 4，可与官方 Socket.IO JavaScript 客户端 v2、v3 和 v4 配合使用。

## 对齐基准

当前兼容基准为官方 `socket.io@4.8.3` 和 `socket.io-client@4.8.3`。旧客户端兼容测试固定使用 `socket.io-client@2.5.0` 与 `socket.io-client@3.1.3`，所有版本都通过 `package-lock.json` 锁定，避免测试时被 npm 自动升级。

Go API 已覆盖官方 4.x 的主要服务端操作，包括 `EmitWithAck`、`ServerSideEmitWithAck`、`FetchSockets`、`SocketsJoin`、`SocketsLeave` 和 `DisconnectSockets`。官方已经废弃但仍保留的 `allSockets()` 也提供了对应的 `AllSockets()`；新代码应优先使用 `FetchSockets()`。

## 兼容矩阵

| 官方 JavaScript 客户端 | Socket.IO 协议 | Engine.IO 协议 | 默认配置 | `AllowEIO3(true)` |
| --- | ---: | ---: | --- | --- |
| `socket.io-client@2.5.0` | v4 | EIO 3 | 拒绝 | 支持 |
| `socket.io-client@3.1.3` | v5 | EIO 4 | 支持 | 支持 |
| `socket.io-client@4.8.3` | v5 | EIO 4 | 支持 | 支持 |

`AllowEIO3` 默认关闭。只有仍需兼容 Socket.IO v2 客户端时才建议启用：

```go
options := socket.DefaultServerOptions()
options.SetAllowEIO3(true)

server := socket.NewServer(nil, options)
```

## EIO 校验规则

服务端会在新握手、Polling 后续请求和 WebSocket 升级请求中严格校验 `EIO`：

- 只接受精确值 `EIO=4`；
- 启用 `AllowEIO3(true)` 后额外接受精确值 `EIO=3`；
- 缺失、空值、重复参数、非数字、带空白以及其他版本均返回错误码 `5`（`Unsupported protocol version`）；
- 已建立会话的后续请求必须继续使用该会话创建时的 Engine.IO 版本，版本不一致会被拒绝。

## 自动化验证范围

仓库使用官方 npm 包运行端到端兼容矩阵，覆盖：

- HTTP long-polling；
- WebSocket；
- Polling 升级到 WebSocket；
- 双向事件和 ACK；
- 服务端多参数 ACK、ACK 超时、迟到 ACK 丢弃以及 ACK/超时竞态下回调至多执行一次；
- 多客户端广播 ACK 覆盖全部成功、部分客户端超时和零接收者立即返回；ACK ID 会在发布和启动超时计时器前分配，避免编码、超时清理与 Adapter 写入之间的数据竞态；未设置 timeout 时不会被当作零时长超时；
- `onAny` 和 `onAnyOutgoing` 通配监听，并验证 ACK 包不会进入通配事件监听器；
- UTF-8、多参数事件和多参数 ACK；
- 手动重连每次连接只触发一次 `connect`，服务关闭后的 `reconnect_failed` 只触发一次；服务端关闭并在同一端口重新监听后，官方 v2/v3/v4 客户端可以自动重连并继续收发；
- 单个及多个二进制附件、嵌套二进制事件和 ACK；
- 官方 v4 客户端在显式 8 MiB `MaxHttpBufferSize` 下完成 6.1 MB JSON 和 4.7 MB 二进制的 WebSocket 双向 ACK 往返；默认 1 MB 安全上限保持不变；
- `message`/`send` 别名；
- 自定义 Namespace；
- v2 查询参数认证以及 v3/v4 `auth` 认证；
- Handshake 单值 Header/Query 与官方一致地暴露为字符串，重复 Query 保留为字符串数组；次级 Namespace 的认证数据进入 `auth` 而不会污染底层连接 Query；无 HTTP request 的 WebTransport Handshake 返回安全连接及空 Header/Query；
- Namespace 认证拒绝；
- Namespace middleware 覆盖注册顺序、异步完成屏障、结构化 `connect_error`、短路、自定义 Namespace 隔离以及连接前后 `Connected`/Socket 表的原子状态切换；transport 在 middleware 完成前关闭时不会产生幽灵连接；
- Socket 入站事件中间件的参数修改、放行和拒绝；
- 官方 v2/v3/v4 客户端连接异步匹配的动态 Namespace、同名子 Namespace 复用及不同子 Namespace 隔离；
- 静态和动态 Namespace 只在首次创建时触发 `new_namespace`；并发动态匹配只创建并公布一个子 Namespace，手动创建的正则匹配子 Namespace 会挂到父 Namespace；
- 动态 Namespace 空闲清理开关：开启后删除并允许重建，关闭后继续保留；
- 动态 Namespace 中间件拒绝时遵守空闲清理开关，并且存在其他已连接 Socket 时不会误删；静态 Namespace 永不受动态空闲清理影响；
- `disconnecting` 在离开 Room 前触发、`disconnect` 在 Room 清空后触发；关闭底层连接会向同一 Engine.IO 会话上的所有 Namespace 依次发送 DISCONNECT，再发送 Engine.IO CLOSE；
- Polling 和 WebSocket 均验证普通发送后丢弃 volatile、空闲时投递、连续 volatile 只投递一次，以及失败 volatile 不阻断后续普通发送；二进制 Socket 发送和服务端广播也保持相同的连续 volatile 语义；广播默认启用压缩并支持逐次关闭压缩；
- `AllSockets` 只返回目标 Namespace/Room 中的 Socket ID；默认和自定义 Namespace 均支持按 Socket ID 排除，Room 排除不会误伤其他客户端；
- 未在 `ConnectTimeout` 内加入任何 Namespace 的客户端会收到有序关闭包并被清理；服务端广播保留事件会像官方实现一样立即报告编程错误；
- 服务端主动断开；
- 多客户端 Room 并集去重、`except` 排除、Socket 广播排除发送者以及离开 Room 后停止接收；
- 默认嵌入并服务官方 Socket.IO 4.8.3 浏览器端普通、min、msgpack、ESM bundle 及 source map；Attach、Listen 和 Go `ServeHandler` 均支持正确 Content-Type、ETag、强弱 304、CORS、gzip/brotli，且可通过 `ServeClient(false)` 关闭；
- 官方 v4 客户端断线后的 Socket ID、Room、Socket Data 和遗漏事件恢复；
- 恢复连接默认跳过 Namespace 中间件，而服务端 Namespace 主动断开不会错误复用旧会话；
- 显式关闭 `SkipMiddlewares` 时，成功恢复仍会重新执行 Namespace 中间件；未知恢复会话会安全降级为新会话，未启用恢复时不会签发私有会话 ID；
- 未启用连接状态恢复时，即使 CONNECT auth 携带伪造 PID/offset 且随后断开，也不会调用 Adapter 的 `RestoreSession` 或 `PersistSession`；
- Engine.IO 连接回调尚未完成时收到首个数据包的并发顺序，避免快速客户端丢失首事件；
- Engine.IO 会在异步写出 OPEN 包前原子注册 SID，客户端读到自定义 SID 时服务端客户端表已可见；
- WebSocket/WebTransport 读取循环只在 Engine.IO packet/error/close 监听器安装完成后启动，避免快速客户端的升级 `ping probe` 丢失；
- WebSocket 和 WebTransport 关闭时会同步关闭底层连接，且重复关闭只执行一次；
- Engine.IO 3/4 客户端完成 Polling 握手后即使不再发起 Poll/Pong，也会在心跳期限内以 `ping timeout` 清理连接；
- Engine.IO 错误请求通过真实 HTTP 验证未知 transport、原型污染名称、未知/非法 SID、非法握手方法和 `AllowRequest` 拒绝，同时校验状态码、JSON 错误码与 `connection_error`；
- Polling HTTP 压缩覆盖 gzip、deflate、自定义阈值、全局/逐消息禁用以及 `Accept-Encoding` q-value 选择，并保留官方兼容响应头行为；
- Polling、直接 WebSocket 和 Polling→WebSocket 升级会按官方次数触发 `initial_headers`/`headers`，监听器的响应头修改会进入真实握手响应；
- Engine.IO SID Cookie 会写入真实 SID，并覆盖默认名称、Path、HttpOnly、SameSite 以及显式关闭 Path/HttpOnly 的变体；
- `MaxHttpBufferSize` 同时约束带 Content-Length、chunked Polling 和 WebSocket 入站载荷；超限数据不会触发 `message`，并关闭对应会话；
- 候选 WebSocket 升级在超时或原 Socket 关闭时会被关闭并清除 upgrading 状态；
- 非法 Origin 会以官方 `INVALID_ORIGIN` 上下文拒绝；自定义 `SetGenerateId` 可返回固定 ID 或错误，错误会以 `ID_GENERATION_ERROR` 拒绝 Polling/WebSocket 握手；使用 `ClusterServer` 跨实例或与Node混部时，自定义SID还必须是20字符并满足本项目SID安全字符集，建议使用base64url；
- Polling 和 WebSocket 收到非法 Engine.IO 包时都会以 `parse error` 关闭 Socket 与底层传输，非法数据不会进入 `message`；
- Socket.IO 畸形或空二进制附件头通过真实 WebSocket 触发与官方一致的 `Illegal attachments` Socket 错误；收到 ERROR 包而应用未注册错误监听器时不会导致服务端崩溃；
- `onAny`/`onAnyOutgoing` 覆盖 prepend 顺序、单个移除、全部清空、广播和二进制广播；middleware 拒绝会清理预先加入的 Room，断开后的 Socket 无法重新加入 Room，迟到二进制包不会调用已断开 Namespace 的事件处理器；
- 同一会话的 Polling GET 重叠会拒绝后到请求并关闭会话；客户端取消 Poll 或服务端在 POST 上传期间关闭时，请求、传输和 Socket 会及时释放；
- HTTP 请求携带 `Connection: close` 只关闭当前 TCP 连接，不会误关闭 Engine.IO Polling 会话，后续 POST/GET 可继续使用 SID；
- Polling 响应提交与请求取消通过原子生命周期门控串行化，取消后不会继续访问已由 `net/http` 收尾的 ResponseWriter；
- `packet` 与 `packetCreate` 监听器可以读取包内容而不消耗实际收发数据，PING/PONG 也按官方顺序触发对应事件；
- WebSocket 未启用逐消息压缩时使用预编码帧，启用压缩时忽略预编码帧并按原始数据重新编码；
- Socket.IO 服务关闭会先清理 Namespace Socket 和 Engine.IO Client，再关闭 HTTP Server 并释放监听端口；关闭未启动的 HTTP Server 会返回可由 `errors.Is` 判断的 `types.ErrServerNotRunning`；
- 重复 CONNECT、连接前 EVENT 和非法 Socket.IO 包会保持官方的有序关闭行为：当前 POST 返回 `200 ok`，已排队的 CONNECT 响应先送达，随后 Polling 返回 `6␞1`，不会过早删除 SID；
- Polling/WebSocket 发送回调保持入队顺序，丢弃关闭不会执行未发送回调，graceful close 会先写出待发送数据并执行回调；
- 默认配置拒绝 EIO 3 客户端。

除单次完整矩阵外，关键竞态还会在 Go race detector 下重复验证：完整 v2/v3/v4 矩阵通过 `go test -count=10` 连续运行 10 轮，v3 Namespace `auth` 快速连接/断开场景连续运行 500 轮，v2 升级连续运行 200 轮，v3/v4 升级各连续运行 100 轮。定向模式可通过 `SOCKET_IO_MATRIX_REPETITIONS` 设置内部重复次数，并用 `SOCKET_IO_MATRIX_ONLY=upgrade`、`namespace-auth` 或 `features` 选择场景。

Engine.IO 另有不经过 Socket.IO 的原始客户端矩阵，锁定 `engine.io-client@3.5.6`、`4.1.4`、`6.6.6`，验证 EIO 3/4、Polling、直接 WebSocket、Polling→WebSocket 升级、文本/二进制和心跳。默认严格服务器必须拒绝 EIO 3，兼容服务器只在显式开启 `AllowEIO3` 后接受。

本地运行：

```bash
cd servers/socket/testdata/compatibility
npm ci

cd ../..
SOCKET_IO_COMPAT_TEST=1 go test \
  -run TestOfficialJavaScriptClientCompatibilityMatrix \
  -race -v -count=1

# 压力复验示例
SOCKET_IO_COMPAT_TEST=1 \
go test -race \
  -run TestOfficialJavaScriptClientCompatibilityMatrix \
  -v -count=10

# 定向复验 Namespace auth 竞态
SOCKET_IO_COMPAT_TEST=1 \
SOCKET_IO_MATRIX_ONLY=namespace-auth \
SOCKET_IO_MATRIX_REPETITIONS=500 \
go test -race \
  -run TestOfficialJavaScriptClientCompatibilityMatrix/v3 \
  -v -count=1

# 原始 Engine.IO 官方客户端矩阵（此时位于 servers/socket）
cd ../engine/testdata/compatibility
npm ci

cd ../..
ENGINE_IO_COMPAT_TEST=1 go test -race \
  -run TestOfficialEngineJavaScriptClientCompatibilityMatrix \
  -v -count=10
```

普通 `go test` 在没有设置 `SOCKET_IO_COMPAT_TEST=1` 或 `ENGINE_IO_COMPAT_TEST=1` 时会跳过对应的 Node.js 端到端测试；CI 会安装锁定版本并强制运行完整矩阵。

## Redis Adapter 跨实现兼容

仓库使用官方 `@socket.io/redis-adapter@8.3.0`、`@socket.io/redis-streams-adapter@0.3.1`、真实 Redis 服务以及分别连接到 Node 和 Go 服务端的官方 JavaScript 客户端，验证普通 Pub/Sub、Sharded Pub/Sub 和 Redis Streams 三种 Adapter：

- 全局广播和 Room 定向广播；
- 文本及二进制广播和广播 ACK；
- `fetchSockets`；
- `serverSideEmitWithAck`；
- `socketsJoin` 和 `socketsLeave`；
- `disconnectSockets`；
- Redis 连接被服务端强制重置后，普通 Pub/Sub、Sharded Pub/Sub 和 Streams 会重新连接、恢复订阅，并继续完成双向广播、集群查询和服务端 ACK；
- Redis 代理被完全关闭 2 秒、所有连接同时不可用后，三种 Adapter 均能在代理恢复后重新连接并恢复上述集群操作；
- Redis 容器实际重启并清空易失状态后，三种 Adapter 均能重建连接与订阅，Redis Streams 也能从新的 Stream 继续双向广播；
- 普通 Pub/Sub 使用官方 `@socket.io/redis-adapter@8.2.1` 与 `8.3.0`、Redis Streams 使用官方 `@socket.io/redis-streams-adapter@0.3.0` 与 `0.3.1` 验证滚动升级：新旧节点同时在线期间继续广播、查询 Socket 和执行服务端 ACK，旧节点被 `SIGKILL` 后剩余节点继续工作；
- Sharded Pub/Sub 验证 Go 节点能够同时接收官方 `8.2.1` 的旧格式和 `8.3.0` 的新格式，并在旧节点退出后继续与新节点工作；
- 第三个官方 Node Adapter 节点或独立 Go Adapter 节点被 `SIGKILL` 后，存活的 Node 与 Go 节点能够收敛集群成员、排除失联 Socket，并继续执行集群查询和服务端 ACK；
- 在连接重置和 Node/Go 节点异常退出之后，Node→Go 与 Go→Node 各连续发送 300 条广播，验证顺序一致、无丢失且无重复；
- Redis Streams 会话在 Node 与 Go 服务端之间双向恢复，包括原 Socket ID、Room 和遗漏事件。

本地运行：

```bash
cd adapters/redis/testdata/interop
npm ci

cd ../../
SOCKET_IO_REDIS_INTEROP_TEST=1 \
SOCKET_IO_REDIS_TEST_ADDR=127.0.0.1:6379 \
SOCKET_IO_REDIS_TEST_PASSWORD=root \
go test ./adapter -run TestOfficialNodeRedisAdapterInterop -v -count=1
```

该测试已经加入 CI，使用 `package-lock.json` 固定官方 Node.js 依赖版本。

### Sharded Pub/Sub 滚动升级边界

官方 `@socket.io/redis-adapter@8.3.0` 的 Sharded 集群消息新增了 `nsp` 字段，并会丢弃 `8.2.1` 发出的无 `nsp` 消息。因此，未经修改的官方 Node `8.2.1` 节点向官方 Node `8.3.0` 节点直接广播并不兼容，这不是 Go 实现能够修复的 Node→Node 行为。

Go Sharded Adapter 会根据已经按 Namespace 隔离的 Redis Channel 为旧消息补全 Namespace，所以 Go 节点可以同时与两代官方节点通信。滚动升级时：

- Go 节点可以与官方 `8.2.1`、`8.3.0` 节点重叠运行；
- 官方 Sharded Node 节点从 `8.2.1` 升级到 `8.3.0` 时，应排空旧节点后协调替换，不能假设两代官方 Node 节点之间完全双向广播；
- 普通 Pub/Sub 和 Redis Streams 不受这一 Sharded 消息格式变化影响，已通过完整的新旧版本重叠测试。

## MongoDB 与 PostgreSQL Adapter 跨实现兼容

MongoDB 矩阵锁定官方 `@socket.io/mongo-adapter@0.4.0`，PostgreSQL 矩阵锁定官方 `@socket.io/postgres-adapter@0.5.0`，并分别使用真实副本集和 PostgreSQL 服务验证：

- Node→Go 与 Go→Node 的文本、二进制和 Room 广播；
- `fetchSockets`、`serverSideEmitWithAck` 和远程 `socketsJoin`；
- MongoDB 的 Node 保存/Go 恢复和 Go 保存/Node 恢复，包括原 Socket ID 与遗漏事件；
- PostgreSQL 的 JSON `NOTIFY` 路径、二进制及超过 8 KB 阈值的 MessagePack attachment 路径；
- 官方 Node 节点退出后的成员收敛，以及重新启动官方节点后的双向广播。

MongoDB 官方 0.4.0 将消息类型 13 定义为 `SESSION`，而新版通用 Cluster Adapter 将同一数字用于 `ADAPTER_CLOSE`。MongoDB Go Adapter 因此使用官方 MongoDB 的专属解释，并在本地关闭时不发布冲突消息。PostgreSQL 官方 0.5.0 已采用新版 Cluster Adapter 协议，不存在该冲突。

官方 PostgreSQL Adapter 源码明确返回空 offset，并注明暂不支持连接状态恢复。因此跨实现矩阵不宣称 PostgreSQL Node↔Go 恢复；Go PostgreSQL Adapter 自身提供的恢复扩展由真实 PostgreSQL 上的 Go↔Go 集成测试覆盖。

本地运行：

```bash
(cd adapters/mongo/testdata/official-interop && npm ci)
(cd adapters/mongo && \
  SOCKET_IO_MONGO_OFFICIAL_INTEROP=1 \
  SOCKET_IO_MONGO_TEST_URI='mongodb://localhost:27017/?replicaSet=rs0' \
  go test ./adapter -run TestOfficialMongoAdapterBilateralInterop -v)

(cd adapters/postgres/testdata/official-interop && npm ci)
(cd adapters/postgres && \
  SOCKET_IO_POSTGRES_OFFICIAL_INTEROP=1 \
  SOCKET_IO_POSTGRES_TEST_URI='postgres://postgres:postgres@localhost:5432/socketio?sslmode=disable' \
  go test ./adapter -run TestOfficialPostgresAdapterBilateralInterop -v)
```

## Cluster Engine 跨实现兼容

`engine.ClusterServer` 已实现不依赖粘性会话的跨监听器读写锁、Polling 包转发、远端关闭和 Upgrade 接管。当前证据分为三组：

- 官方1–12项由多个Go `ClusterServer` 通过进程内 `MemoryClusterBus` 逐项验证；
- 官方13–14项由三个Go实例在无粘性round-robin入口下验证心跳和二进制，属于部署行为等价，不是Node OS worker/IPC API复刻；
- 官方15–18项已通过 `adapters/redis/enginebus` 建立精确的0.1.0 MessagePack/Channel线路，并启动真实官方Node `RedisEngine`，对 `redis` 与 `ioredis` 分别验证Node/Go两种会话owner；真实Redis矩阵及高频 `-race` 门禁均已通过，Cluster Engine按18/18闭合。

这里兼容的是直接 `RedisEngine` 线协议，不是Node专属的 `setupPrimary()`、`NodeClusterEngine`、`setupPrimaryWithRedis()`、primary/worker IPC relay、进程句柄或类继承API。官方Cluster Engine对未知SID只检查长度20；本项目还会通过 `utils.IsValidSid` 校验安全字符。因而自定义SID应为20字符并满足本项目安全字符集，建议使用base64url；包含特殊符号的20字符SID可能被官方Node路由、但会被Go以400拒绝，这是刻意的安全加严和已知差异。详细配置见[Go集群部署](CLUSTER_DEPLOYMENT.md)。

官方 `engine.io@6.6.9` 与 `@socket.io/cluster-engine@0.1.0` 没有把WebTransport session接入跨节点owner查找，Go当前也不提供跨节点WebTransport接管；内部transport枚举包含 `webtransport` 不代表该入口可用。这是双方共同边界，不作为Go独有兼容缺口。

官方Cluster Engine 0.1.0在快速接管时会用固定的Engine.IO协议版本4创建新Socket，本项目保持相同行为。因此v2客户端的EIO 3兼容结论适用于普通单节点和已列出的客户端矩阵，不扩展为“Cluster Engine跨节点快速接管支持EIO 3”；这是双方同源限制，不是Go独有差异。

`sticky.Router` 仍可作为owner-affinity部署替代，但它不再是唯一的Cluster Engine方案，其10项Sticky映射也不计入Cluster Engine的18项分母。

## 已验证边界与持续验收

通过兼容矩阵表示上述行为已由真实官方客户端或逐项Go等价回归验证，不表示整个官方测试套件已被原样移植。详细源行分类与证据见[官方测试对齐映射](OFFICIAL_TEST_MAPPING.md)。Cluster Engine 18/18和严格运行时源行1004/1004均已闭合；后续继续执行每周3小时回归门禁。

数小时级持续压力与长时间网络分区验收已经闭合：72个官方客户端的完整3小时运行覆盖20分钟网络分区、滚动替换、持续ACK/广播和资源回落，详见 [2026-08-01验收记录](soak/2026-08-01.md)。

按照项目范围，Google Cloud、AWS 和 Azure Adapter 不在兼容目标内。
