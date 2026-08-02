# 官方测试对齐映射

本文档用于追踪本项目与官方 Socket.IO 测试套件的行为对齐情况。基准固定为：

- `socket.io@4.8.3`；
- Socket.IO 4.8.3 tag 对应的官方仓库提交 `9978574e4f1d4e21593497f94c40053cd0fff359`；
- 按 4.8.3 发布包的 semver 依赖范围当前解析并锁定的闭包：`engine.io@6.6.9`、`socket.io-parser@4.2.7`、`socket.io-adapter@2.5.8`、`engine.io-parser@5.2.3`；
- Google Cloud、AWS 和 Azure Adapter 不在项目范围内。

Socket.IO tag 提交本身仍引用较早的工作区版本，因此不能把提交 `9978574…` 描述成“包含 Engine.IO 6.6.9”。核心 tag 与发布包当前解析出的依赖闭包分别记录、分别核验。

统计数字为官方测试文件中顶层及嵌套 `it()` 测试声明数，不包含 `describe()`。严格运行时分母如下：

| 组成 | 官方源测试行 | 说明 |
| --- | ---: | --- |
| 服务端、Parser 与基础 Adapter | 520 | Socket.IO 服务端203、Engine.IO 服务端197、Engine.IO Parser 38、Socket.IO Parser 34、基础 Adapter 48 |
| 官方客户端 | 239 | Engine.IO Client 108、Socket.IO Client 115、`component-emitter` 16 |
| 非云 external emitter | 53 | Redis、Redis Streams、MongoDB、PostgreSQL |
| Cluster Adapter | 18 | 三进程Go部署等价映射 |
| Sticky | 10 | Go入口路由部署等价映射 |
| Admin UI | 21 | 20项Go可适用行为、1项Node宿主N/A |
| 非云官方 Adapter | 125 | 120项Go可适用行为、3项上游 `skip`、2项宿主N/A |
| Cluster Engine | 18 | 18/18已分类或映射；进程内、三实例、Redis及官方Node互操作门禁通过 |
| **合计** | **1004** | Google Cloud、AWS、Azure Adapter不在分母内 |

当前1004/1004项已经完成分类/映射闭合。这里统计的是官方**源测试行的追踪状态**，不是1004个互不重复的Go测试执行数：同一份互操作或Go回归证据可以支撑多个官方断言，分母也保留上游 `skip`、宿主N/A及平台等价项。因此只能表述为“全量源行已分类或映射”，不能写成“1004个上游JavaScript测试全部原样执行通过”。

编译期类型契约共36项（Socket.IO服务端22项、Socket.IO客户端14项）另行统计，不与1004项运行时分母相加。状态含义：

- **真实互操作**：由锁定版本的官方 JavaScript 客户端或 Adapter 端到端验证；
- **Go 回归**：已有对应 Go 单元或集成测试，但尚未逐条证明与官方断言完全等价；
- **部分覆盖**：只覆盖该文件的一部分行为；
- **待映射**：尚未建立足够证据。

## Socket.IO 服务端

| 官方测试文件 | 用例数 | 当前证据 | 状态 | 下一项 |
| --- | ---: | --- | --- | --- |
| `handshake.ts` | 4 | 真实 HTTP OPTIONS/GET 验证 CORS origin、methods、allowed headers、credentials；`AllowRequest` 放行返回 Engine.IO OPEN，拒绝返回 403 | Go 等价回归 | — |
| `close.ts` | 8 | 8 项均已建立下方索引；两种 HTTP Server 创建方式验证 Socket/Engine 客户端清理与端口释放，未启动 HTTP Server 的两种关闭形式安全返回，协议违规返回官方 `6␞1` 干净关闭序列；官方 v2/v3/v4 客户端验证 namespace/server close 断开原因 | 真实互操作 + Go 等价回归 | — |
| `connection-state-recovery.ts` | 7 | 7 项均已建立下方索引；官方 v4 客户端恢复 SID、Room、Data、遗漏事件、两种 middleware 模式、未知会话和默认关闭；探针 Adapter 证明关闭恢复时不调用 Persist/Restore | 真实互操作 + Go 等价回归 | — |
| `messaging-many.ts` | 20 | 20 项均已建立下方索引；Namespace/Room 广播、二进制、并集去重、排除、Room 生命周期、广播 ACK 成功/超时/零接收者和 WebSocket 预编码由官方客户端互操作或真实传输 Go 回归覆盖 | 真实互操作 + Go 等价回归 | — |
| `middleware.ts` | 10 | 10 项均已建立下方索引；调用/短路/结构化错误、同步与异步顺序、连接状态、自定义 Namespace、握手及 transport 关闭竞态由官方客户端或真实传输覆盖 | 真实互操作 + Go 等价回归 | — |
| `namespaces.ts` | 42 | 42 项均已建立下方索引；默认/自定义/动态 Namespace、监听器、查询、广播、排除、volatile、压缩、清理和并发创建由官方客户端互操作或真实传输 Go 回归覆盖 | 真实互操作 + Go 等价回归 | — |
| `server-attachment.ts` | 19 | 19 项均已建立下方索引；嵌入官方 4.8.3 普通/min/msgpack/ESM bundle 及 source map，覆盖 query、Content-Type、ETag、强弱 304、CORS、gzip/brotli、关闭静态服务、Attach/选项合并/构造器/Listen，并额外覆盖 Go `ServeHandler` | Go 等价回归 | — |
| `socket-middleware.ts` | 2 | 2 项均已建立下方索引；入站事件 middleware 修改、顺序、放行、错误短路和 `error` 事件 | 真实互操作 + Go 等价回归 | — |
| `socket-timeout.ts` | 5 | 5 项均已建立下方索引；ACK 成功、无响应超时、0ms 超时、迟到 ACK 丢弃和回调至多一次 | 真实互操作 + Go 等价回归 | — |
| `socket.io.test-d.ts` | 22 | 22 项均已建立下方映射；`typed` 泛型 API、方向感知生成器、保留事件类型以及真实 Go 编译器正/反例覆盖监听、发送、ACK、广播、节点事件和 Adapter 类型 | Go 编译期等价验证 | — |
| `socket.ts` | 62 | 62 项均已建立下方索引；事件、二进制/ACK、volatile、Handshake、关闭重启、错误包、Room 清理及 catch-all 生命周期由官方客户端互操作或真实传输 Go 回归覆盖 | 真实互操作 + Go 等价回归 | — |
| `utility-methods.ts` | 9 | 9 项均已建立下方索引；3 个真实 Socket 覆盖全部与 Room 过滤下的 Fetch/Join/Leave/Disconnect，自定义 Adapter 验证 RemoteSocket 元数据 | Go 等价回归 | 非分母增强：继续扩展分布式 Adapter 的过滤组合 |
| `uws.ts` | 12 | 12 项均已建立下方索引；锁定的官方 v2/v3/v4 客户端在 Go HTTP 引擎上同时覆盖自动升级、WebSocket-only、Polling-only 与自定义 Namespace，验证广播、二进制、volatile、Room、中间件、离开、主动断开和静态资源 | 真实互操作 + Go 部署等价回归 | — |
| `v2-compatibility.ts` | 3 | 3 项均已建立下方索引；官方 v2.5.0 客户端验证 EIO 3 开关、客户端/服务端公开 Socket ID 一致及 Namespace query 进入 Handshake auth | 真实互操作 | — |

Socket.IO 官方运行时测试共 203 项，另有 22 项 TypeScript 编译期类型测试。203/203 项运行时行为均已建立逐项索引、真实互操作或明确的 Go 等价证据；22/22 项类型测试也已建立 Go 编译期等价映射。`uws.ts` 的 `attachApp()` 是 Node/uWebSockets.js 专属表面 API，Go 不复制该 API；本项目使用 Go HTTP 引擎对其 12 项可观察行为进行等价验证。运行时与编译期类型仍分别统计，不能合并成一个模糊百分比。

### `socket.io.test-d.ts` 逐项索引

TypeScript 以事件参数元组和事件映射表达类型，Go 以请求 struct、ACK 响应类型及按方向生成的函数表达相同约束。动态 API 保持官方“未提供事件映射时接受任意参数”的语义；启用 `typed` 后，正例包必须被真实 Go 编译器接受，反例包必须被编译器拒绝。

| 官方编号 | 类型行为 | Go 等价证据 |
| --- | --- | --- |
| 1 | 保留的 disconnect/disconnecting 监听器推断 `DisconnectReason` | `DisconnectReason` 字符串类型、9 个官方原因常量、`DisconnectEvent`/`DisconnectingEvent`；正例编译 fixture 与 `TestTypedReservedDisconnectReason` |
| 2–3 | 无事件映射的普通字符串或枚举事件监听参数为 `any` | 核心 `On` API 使用 `...any`；Go 字符串别名/常量均可作为事件名，保持动态模式 |
| 4 | 无事件映射的 `emit` 接受任意参数 | 核心 Server/Namespace/Socket/BroadcastOperator 的 `Emit(string, ...any)` 动态模式 |
| 5 | 无事件映射的 `send` 接受任意参数 | 核心 `Send(args ...any)` 动态模式 |
| 6 | 无事件映射的 `emitWithAck` 接受任意参数并返回动态值 | 核心 ACK API保持动态；`typed.EmitAck` 为选择强类型后的响应路径 |
| 7 | 单一事件映射正确推断监听参数 | 正例 fixture 的 `typed.On(... func(context.Context, request))`；生成的 `On*`/`Handle*` 固定请求类型 |
| 8 | 错误事件参数、handler 参数或事件名不能通过类型检查 | 反例 fixture 的错误请求及 `func(context.Context, string)` 必须编译失败；生成函数名固定事件集合 |
| 9 | 未声明 `message` 时不存在强类型 send | 方向感知生成器只为声明的 ServerToClient 事件生成 `Send*`，不会凭空生成 message 封装 |
| 10 | 声明 `message` 后只接受正确参数 | `messageRequest` 生成 `SendMessage(..., messageRequest)`；请求 struct 字段类型由编译器检查 |
| 11 | BroadcastOperator 在动态模式工作 | 正例 fixture 将真实 `*socket.BroadcastOperator` 传给 `typed.Emit`；核心 API仍支持动态广播 |
| 12 | `to/in/except/compress/volatile/local/timeout` 后保持发送类型 | 这些修饰器均返回 `*BroadcastOperator`，继续满足同一强类型 emitter 路径；正例 fixture 验证该具体类型 |
| 13 | 广播 `emit` 的事件参数类型正确 | `typed.Emit[Request, NoAck]` 与生成的 `Send*`；错误请求由反例 fixture 拒绝 |
| 14 | 广播 `emitWithAck` 只接受 ACK 事件并推断响应 | ServerToClient ACK 事件生成 `Send*(context.Context, ..., Request) (Response, error)`，内部调用 `typed.EmitAck` |
| 15 | Server/Namespace/Socket 普通 emit 推断正确类型 | `typed.Emit` 同时支持 fluent Server 和返回 error 的 Namespace/Socket/BroadcastOperator；正例 fixture覆盖前三类服务端 emitter |
| 16 | 普通 emit 不暴露 ACK 事件的错误表面 | `NoAck` 事件生成普通 `Send*`；有响应事件只生成 context + ACK 返回值形式，不生成普通发送封装 |
| 17 | Socket `emitWithAck` 推断请求和单一 ACK 响应 | 正例 fixture 的 `typed.EmitAck` 与错误响应赋值反例；Context 同时表达官方 timeout 修饰语义 |
| 18 | ClientToServer 与 ServerToClient 监听/发送类型分离 | 生成器根据 Direction 分别在 client/server 文件生成 `Emit/On/Handle/Send`，`TestGenerateAllArtifacts` 验证输出 |
| 19 | 发送方向事件不能注册成错误方向的监听器 | 生成器不会在服务端为 ServerToClient 事件生成 `Handle*`，也不会在客户端生成同事件的 `Emit*`；生成器测试显式断言不存在 |
| 20 | serverSideEmit 的监听、发送及 ACK 响应有独立类型 | `ServerSideEmitter`、`ServerSideEmit`、`ServerSideEmitAck`；ServerToServer ACK 生成 `Broadcast*(...) ([]Response, error)`，正例/反例 fixture覆盖请求类型 |
| 21 | 合法 Adapter constructor 可配置 | 正例 fixture 将 `*socket.AdapterBuilder` 传入强类型 `SetAdapter(AdapterConstructor)` |
| 22 | 非 Adapter 返回值/对象被拒绝 | 反例 fixture 的字符串 Adapter 必须被真实 Go 编译器拒绝 |

### `socket.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–2 | 手动重连不重复触发；服务端关闭后 `reconnect_failed` 只触发一次 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `manualReconnect`、`reconnectFailedOnce` |
| 3–12 | 双向事件、`send`/`message`、`null`、UTF-8、单/多/嵌套二进制 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkFeatures` |
| 13–22 | Polling/WS 普通与 volatile、连续 volatile、二进制 volatile、失败后普通发送 | `TestOfficialSocketPollingVolatileBehavior`、`TestOfficialSocketWebSocketVolatileBehavior`、`TestOfficialSocketWebSocketConsecutiveBinaryVolatileBehavior` |
| 23–24 | 服务端 `send` 与客户端回调 | `checkFeatures` |
| 25 | Namespace 连接前缓冲事件立即按序送达 | `checkBufferedNamespaceEventOrder` |
| 26–33 | 双向普通/多参数/二进制 ACK 与 `EmitWithAck` | `checkFeatures`、`TestOnAckConsumesCallbackOnce`、`TestAckTimeoutIgnoresLateResponse` |
| 34–38 | Client、Conn、Request 访问器；请求 query；次级 Namespace auth/query | `TestOfficialSocketHandshakeAndAccessors` |
| 39 | 无 Engine.IO request 的 WebTransport Handshake 默认值 | `TestOfficialSocketHandshakeWithoutEngineRequest` |
| 40–41 | 6.1 MB JSON、4.7 MB 二进制双向往返 | `checkLargePayloads`（官方 v4 客户端，显式 8 MiB 上限） |
| 42 | 服务关闭并在同端口重启后继续连接和收发 | `checkServerRestart`、`TestOfficialSocketServerCanEmitAfterCloseAndRestart` |
| 43–44 | 默认压缩及逐消息关闭压缩 | `TestOfficialNamespaceCompressionFlags` |
| 45–48 | 原始/空二进制头、无应用 error handler 的 ERROR 包 | `TestOfficialSocketMalformedBinaryPackets`、`TestOfficialSocketErrorPacketWithoutApplicationHandlerDoesNotCrash` |
| 49 | 修改 JavaScript `Object.prototype` 不影响连接 | Go 无原型链，语言层面不适用；所有报文使用独立 Go map 解码 |
| 50 | 保留事件拒绝 | `TestOfficialServerRejectsReservedEvent`、`TestOfficialSocketRejectsReservedEvent` |
| 51 | Namespace 断开后忽略迟到二进制包 | `TestOfficialSocketIgnoresBinaryPacketAfterNamespaceDisconnect` |
| 52–53 | middleware 失败清理 Room；断开后禁止重新 Join | `TestOfficialSocketRoomsAreCleanedAfterMiddlewareFailure`、`TestOfficialSocketCannotJoinRoomsAfterDisconnection` |
| 54–56 | `onAny` 调用、prepend 顺序、off/clear | `TestOfficialSocketIncomingAnyListenerLifecycle` |
| 57–61 | `onAnyOutgoing` 调用、广播、二进制、prepend、off/clear | `TestOfficialSocketOutgoingAnyListenerLifecycle`、`checkAnyListeners` |
| 62 | `disconnect(true)` 断开同一 Engine.IO 连接上的全部 Namespace | `TestOfficialTransportDisconnectClosesAllNamespaces` |

### `namespaces.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 默认 Namespace 可通过 `sockets` 访问 | `TestOfficialNamespaceAliasesAndNormalization` |
| 2 | Server 的 Namespace/Broadcast 别名 API | `TestOfficialServerNamespaceAliases` |
| 3 | 广播操作器保持不可变 | `TestOfficialNamespaceBroadcastOperatorIsImmutable` |
| 4–11 | 自动连接；`connect`/`connection`；多个 Socket/Namespace；空名称与 `/`；多监听器；`Of` 第二参数 | `TestOfficialNamespaceConnectionsAndListenerRegistration`；`TestOfficialJavaScriptClientCompatibilityMatrix` |
| 12 | transport 断开时关闭其全部 Namespace | `TestOfficialTransportDisconnectClosesAllNamespaces` |
| 13 | `disconnecting` 在清空 Room 前触发 | `TestOfficialDisconnectingEventRoomOrder` |
| 14 | 不存在的 Namespace 返回 `Invalid namespace` | `TestOfficialInvalidNamespaceConnectError` |
| 15–18 | 同 Namespace 不复用独立连接；Namespace 与 Room 的 `AllSockets` 隔离 | `TestOfficialNamespaceAllSocketsFilters` |
| 19–20 | 普通包占用 transport 时丢弃 volatile；空闲 transport 发送 volatile | `TestOfficialNamespaceVolatilePollingBehavior` |
| 21–22 | 默认开启压缩及逐消息关闭压缩 | `TestOfficialNamespaceCompressionFlags` |
| 23 | 保留事件拒绝 | `TestOfficialServerRejectsReservedEvent` |
| 24 | 未发送 Namespace CONNECT 的 Client 超时关闭 | `TestOfficialConnectTimeoutClosesClientWithoutNamespace` |
| 25–27 | 按默认 Namespace Socket、自定义 Namespace Socket、Room 排除广播 | `TestOfficialNamespaceExclusionBroadcasts` |
| 28 | 静态 Namespace 触发一次 `new_namespace` | `TestOfficialNewNamespaceEvent` |
| 29 | 开启动态清理时不清理静态 Namespace | `TestOfficialStaticNamespaceIsNotCleanedUp` |
| 30–31 | 正则动态 Namespace 真实连接、中间件、父级广播及 Room 广播 | `TestOfficialRegexDynamicNamespaceConnectionAndRoomBroadcast` |
| 32 | 函数匹配器允许动态 Namespace | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkDynamicNamespaces` |
| 33 | 无匹配动态 Namespace 时拒绝连接 | `TestOfficialInvalidNamespaceConnectError`、`checkDynamicNamespaces` |
| 34–35 | 动态 `new_namespace`；并发匹配只创建一个子 Namespace | `TestOfficialDynamicNamespaceConcurrentCreationEmitsOnce` |
| 36–38 | 启用清理后删除并可重建；关闭清理时保留空 Namespace | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkDynamicNamespaceCleanup` |
| 39–40 | middleware 拒绝时按清理开关保留或删除动态 Namespace | `TestOfficialDynamicNamespaceCleanupAfterMiddlewareRejection` |
| 41 | middleware 拒绝一个 Client 时保留仍有 Socket 的动态 Namespace | `TestOfficialDynamicNamespaceIsKeptWhenAnotherSocketRemains` |
| 42 | 手动创建的匹配子 Namespace 挂接到正则父 Namespace | `TestOfficialManualDynamicChildAttachesToRegexParent` |

### `messaging-many.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–2 | 仅向目标 Namespace 广播普通及二进制数据 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkNamespaceBroadcastIsolation` |
| 3 | Socket 广播排除发送者和其他 Namespace | `checkRoomBroadcastSemantics` |
| 4–5 | Room 广播及多 Room 并集去重 | `checkRoomBroadcastSemantics` |
| 6–7 | Socket/Namespace Room 广播及二进制 Room 广播 | `checkRoomBroadcastSemantics` |
| 8–11 | 跟踪 Room、删除空 Room、忽略未知 Leave、一次加入多个 Room、`leaveAll` 清理 | `TestOfficialMessagingManyRoomLifecycle` |
| 12–13 | 广播时排除指定 Socket 或 Room | `checkRoomBroadcastSemantics`、`TestOfficialNamespaceExclusionBroadcasts` |
| 14 | Socket 广播操作器保持不可变 | `TestOfficialMessagingManySocketBroadcastOperatorIsImmutable` |
| 15–18 | 多客户端广播 ACK 成功及部分响应超时；Go 回调形式覆盖官方 callback/Promise 两种表面 API 的共同语义 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkBroadcastAcknowledgements` |
| 19 | 零接收者广播 ACK 立即返回空响应且无错误 | `checkBroadcastAcknowledgements` |
| 20 | 广播前预计算 WebSocket Engine.IO 帧并实际发送 | `TestOfficialMessagingManyPrecomputesWebSocketFrame` |

### `server-attachment.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–5 | 普通、query、source map、min bundle 与 min source map | `TestOfficialServerAttachmentServesEmbeddedClientBundles` |
| 6–7 | gzip 与 brotli 内容编码 | `TestOfficialServerAttachmentCompression` |
| 8 | 静态客户端响应 CORS | `TestOfficialServerAttachmentCORSAndConditionalRequests` |
| 9–12 | msgpack/ESM bundle 及各自 source map | `TestOfficialServerAttachmentServesEmbeddedClientBundles` |
| 13–14 | 强 ETag 与弱 ETag 命中返回 304 | `TestOfficialServerAttachmentCORSAndConditionalRequests` |
| 15 | `ServeClient(false)` 不服务静态文件 | `TestOfficialServerAttachmentCanDisableClientServing` |
| 16 | 显式 `Attach` 后服务客户端 | `TestOfficialServerAttachmentExplicitAttach` |
| 17 | `Attach` 合并构造选项和挂载选项 | `TestOfficialServerAttachmentMergesOptions` |
| 18–19 | 构造器绑定地址及 `Listen` 绑定地址 | `TestOfficialServerAttachmentConstructorAndListenBind` |

Go 特有的 `ServeHandler` 入口由 `TestOfficialServerAttachmentServeHandler` 验证相同静态资源行为。资源来自锁定的 `socket.io@4.8.3` 发布包并通过 `go:embed` 编入模块，不依赖应用可执行文件附近存在 `client-dist` 目录。

### `middleware.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 依次调用多个默认 Namespace middleware | `TestOfficialNamespaceMiddlewareOrderAndConnectionState` |
| 2–3 | middleware 错误短路并传递 message 与结构化 data | `TestOfficialNamespaceMiddlewareStructuredErrorShortCircuits`；官方矩阵 `checkNamespaceAuth` |
| 4–5 | 同步/长耗时 middleware 完成后才触发 connection | `TestOfficialNamespaceMiddlewareOrderAndConnectionState` |
| 6 | transport 在异步 middleware 完成前关闭时忽略 Socket | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkDisconnectDuringNamespaceMiddleware` |
| 7 | 自定义 Namespace 的多个异步 middleware 保持注册顺序 | `TestOfficialCustomNamespaceMiddlewareIsolation`、`TestOfficialNamespaceMiddlewareOrderAndConnectionState` |
| 8 | 存在 middleware 时握手仍独立、正确完成 | `TestOfficialNamespaceMiddlewareOrderAndConnectionState`；`checkNamespaceAuth` |
| 9 | middleware 只作用于注册它的自定义 Namespace | `TestOfficialCustomNamespaceMiddlewareIsolation` |
| 10 | middleware 期间未连接且不在已连接 Socket 表，完成后原子切换状态 | `TestOfficialNamespaceMiddlewareOrderAndConnectionState` |

### `connection-state-recovery.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 恢复公开 SID、私有 PID、offset 与遗漏的全局/Room/直接事件 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkConnectionStateRecovery` |
| 2 | 恢复 Room、Socket Data，并标记 `Recovered()` | `checkConnectionStateRecovery` |
| 3 | 默认恢复时跳过 Namespace middleware | `checkConnectionStateRecovery` |
| 4 | `SkipMiddlewares(false)` 时恢复仍运行 middleware | `checkConnectionStateRecoveryWithMiddleware` |
| 5 | 未知 PID/offset 安全降级为新会话 | `checkUnknownRecoverySession` |
| 6 | 默认不启用恢复，不签发 PID | `checkRecoveryDisabledByDefault` |
| 7 | 关闭恢复时不调用 Adapter `PersistSession`/`RestoreSession` | `TestOfficialRecoveryDisabledDoesNotCallAdapterSessionMethods` |

### `utility-methods.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–2 | `FetchSockets` 返回全部 Socket 或指定 Room 的 Socket | `TestOfficialUtilityFetchJoinAndLeaveSockets` |
| 3 | 自定义 Adapter 返回 RemoteSocket 的 ID、Handshake、Room 与 Data | `TestOfficialUtilityFetchSocketsWithCustomAdapter` |
| 4–5 | 全部 Socket 或指定 Room Socket 批量加入 Room | `TestOfficialUtilityFetchJoinAndLeaveSockets` |
| 6–7 | 全部 Socket 或指定 Room Socket 批量离开 Room | `TestOfficialUtilityFetchJoinAndLeaveSockets` |
| 8–9 | 断开全部 Socket 或指定 Room Socket | `TestOfficialUtilityDisconnectSockets` |

### `close.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–2 | 关闭附加的 HTTP Server 或由 Socket.IO 创建的 HTTP Server，停止 Socket/Engine 客户端、计时器并释放端口 | `TestOfficialCloseReleasesHTTPPort`、`TestOfficialCloseStopsConnectedSocketsAndEngineClients`；官方矩阵 `checkServerRestart` 验证 v2/v3/v4 客户端收到 `transport close` |
| 3–4 | 底层 HTTP Server 未运行时，callback 与 Promise 两种形式均不抛出未处理异常 | `TestOfficialCloseWithHTTPServerThatIsNotRunning`；Go 无 Promise 重载，以有 callback 返回明确错误、无 callback 不 panic 对应两种调用形式 |
| 5 | 关闭服务端会停止 Socket、Engine Client 和相关生命周期 | `TestOfficialCloseStopsConnectedSocketsAndEngineClients`、`TestOfficialCloseReleasesHTTPPort` |
| 6 | 收到多个 CONNECT 包时干净关闭连接 | `TestOfficialCloseProtocolViolations/several_CONNECT_packets` |
| 7 | 未连接时收到 EVENT 包会干净关闭连接 | `TestOfficialCloseProtocolViolations/EVENT_packet_before_CONNECT` |
| 8 | 收到非法 Socket.IO 包会干净关闭连接 | `TestOfficialCloseProtocolViolations/invalid_Socket.IO_packet` |

服务端主动断开单个 Namespace 的补充互操作断言位于官方矩阵 `checkFeatures`，v2/v3/v4 客户端均必须收到官方原因 `io server disconnect`。

### `socket-middleware.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 多个 Socket middleware 按顺序执行，前一项对事件参数的修改对后一项和最终 handler 可见 | `TestOfficialSocketMiddlewareOrderMutationAndError`；官方矩阵 `checkSocketMiddleware` |
| 2 | middleware 错误会短路后续 middleware 与事件 handler，并通过 Socket 的 `error` 事件传递 | `TestOfficialSocketMiddlewareOrderMutationAndError`；官方矩阵 `checkSocketMiddleware` |

### `socket-timeout.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 客户端不 ACK 时在期限后返回错误 | 官方矩阵 `checkAckLifecycle` 的 `ack-never`；`TestAckTimeoutIgnoresLateResponse` |
| 2 | 0ms 超时即使客户端立即 ACK 也必须超时，且 callback 只运行一次 | 官方矩阵 `checkAckLifecycle` 的 `ack-zero`；`TestZeroAckTimeoutRegistersBeforeTimerAndCallsOnce` |
| 3 | 客户端及时 ACK 时返回全部 ACK 参数且没有错误 | 官方矩阵 `checkAckLifecycle` 的 `ack-fast` |
| 4 | 无 ACK 时 Promise 形式拒绝 | Go `EmitWithAck` 使用 callback 表面 API；其共同的超时与单次完成语义由 `ack-never`、`TestAckTimeoutIgnoresLateResponse` 覆盖 |
| 5 | 及时 ACK 时 Promise 形式成功解析 | Go `EmitWithAck` 使用 callback 表面 API；其共同的成功与参数传递语义由 `ack-fast` 覆盖 |

### `uws.ts` 逐项索引

官方文件验证的是更换 uWebSockets.js 引擎后 Socket.IO 的可观察行为，而不是新增协议。`TestOfficialJavaScriptClientCompatibilityMatrix` 的 `checkDeploymentEngineParity` 在 Go HTTP 引擎上建立四条并发真实连接（自动 Polling→WebSocket、WebSocket-only、Polling-only、自定义 Namespace），并由官方 v2.5.0、v3.1.3 和 v4.8.3 客户端执行下列断言：

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 默认 Namespace 广播到三种传输连接，且不泄漏到自定义 Namespace | `checkDeploymentEngineParity` 的 `deployment-broadcast` |
| 2 | 自定义 Namespace 广播只送达该 Namespace | `deployment-namespace-broadcast` 及三个默认 Namespace 泄漏计数器 |
| 3 | 正则动态 Namespace 创建并广播 | `/dynamic-101` 的 `trigger-dynamic-broadcast` 真实互操作 |
| 4 | 二进制广播到自动升级、WebSocket-only 和 Polling-only 客户端 | `Buffer.from([1,2,3])` 的三路逐字节断言 |
| 5 | 空闲连接上的 volatile 二进制广播 | `deployment-volatile-binary` 的三路逐字节断言；另有 Polling/WS 丢弃语义回归 |
| 6 | 只向指定 Room 中的 WebSocket 与 Polling 客户端广播 | `deployment-room-1`，并断言 Room 外客户端未收到 |
| 7 | 多个 Room 的并集广播 | `deployment-room-a`、`deployment-room-b` 两路真实接收 |
| 8 | 排除指定 Room 后向其余默认 Namespace 客户端广播 | `deployment-excluded-room`，并断言被排除客户端未收到 |
| 9 | Namespace middleware 中加入的 Room 可立即用于广播 | 服务端 middleware 加入 `deployment-middleware-room`，三路真实接收 |
| 10 | Socket 离开 Room 后不再接收该 Room 广播 | `deployment-leave-room`，断言离开者为零次、其余两路各接收一次 |
| 11 | Polling→WebSocket 升级窗口附近由服务端主动断开不会崩溃 | 自动升级连接触发 `request-server-disconnect` 并等待官方客户端 `disconnect`；底层升级关闭竞态另有 Engine.IO 回归 |
| 12 | 提供官方浏览器客户端静态文件 | 官方 Node HTTP 客户端验证 200、Content-Type、ETag、无 X-SourceMap 且正文包含 `engine.io`；`TestOfficialServerAttachmentServesEmbeddedClientBundles` 进一步验证完整资源集合 |

### `v2-compatibility.ts` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | `allowEIO3: true` 时官方 v2 客户端成功连接，客户端与服务端公开 Socket ID 一致 | `TestOfficialJavaScriptClientCompatibilityMatrix` 的严格/兼容服务器与 `checkFeatures/server-socket-id` |
| 2 | v2 客户端连接 Namespace 时设置的 query 进入服务端 Handshake auth | 官方矩阵 `checkNamespaceAuth` 使用 v2 query 完成允许和拒绝两条真实连接 |
| 3 | 默认关闭 `allowEIO3` 时拒绝 v2/EIO3 客户端 | 官方矩阵 `strict EIO policy` |

## Engine.IO 服务端

| 官方测试文件 | 用例数 | 当前证据 | 状态 | 下一项 |
| --- | ---: | --- | --- | --- |
| `engine.io.js` | 13 | 13 项均已建立下方索引；入口、选项、Listen/Attach、路径与原处理器保留由 Go 回归覆盖，官方 Engine.IO v3/v4/v6 客户端独立验证 EIO 3/4、Polling、WebSocket、升级、文本/二进制与心跳 | 真实互操作 + Go 等价回归 | — |
| `middlewares.js` | 12 | 12 项均已建立下方索引；真实 Polling/WebSocket 握手覆盖响应头、顺序、主动结束、错误短路、安全头、会话 Cookie，并验证 middleware 不绕过 Polling 最大载荷限制 | Go 等价回归 | — |
| `parser.js` | 1 | 1 项已建立下方索引；逐字节复现官方 EIO 3 文本与二进制混合 payload，并反向解码验证 Unicode 与二进制内容 | Go 等价回归 | — |
| `server.js` | 153 | 153 项全部建立下方逐项索引；TLS、drain、回调、升级流量、压缩、请求/响应头、CORS、自定义 WebSocket engine、远端地址及 EIO 4 Polling 二进制 Content-Type 拒绝均有精确证据 | 完成 | — |
| `webtransport.mjs` | 18 | 18 项全部使用真实 HTTP/3 + QUIC session 逐项覆盖：直连、升级、超时、关闭、心跳、文本/二进制、回调、1 MB 大包、未知/原型链 SID 及 middleware 边界 | 完成 | — |

Engine.IO 6.6.9 官方服务端运行时分母共197个源测试行。`engine.io.js` 13项、`middlewares.js` 12项、`parser.js` 1项、`server.js` 153项和 `webtransport.mjs` 18项均已分类并建立逐项映射；可适用行为具有自动化证据，上游 `it.skip` 与Node/Go宿主边界在下表单列。这里的197/197表示映射闭合，不表示197个上游JavaScript测试被原样执行。

### `engine.io.js` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 暴露 Engine.IO 协议号 | `TestOfficialEngineEntryPointsAndOptions` 验证 `Protocol == 4` |
| 2 | 服务端与客户端包版本相同 | 官方本项自身为 `it.skip`；Go 服务端与客户端共享仓库版本常量，协议互操作由独立客户端矩阵验证 |
| 3 | 无参数构造 Server 并包含 WebSocket engine | `TestOfficialEngineEntryPointsAndOptions` |
| 4 | 构造选项传入 Server | `TestOfficialEngineEntryPointsAndOptions` |
| 5 | `Listen` 创建 HTTP Server，非 Engine.IO 路径返回 501 | `TestOfficialEngineListenUses501Fallback` |
| 6–7 | `New(httpServer)` 与 `Attach(httpServer)` 均返回可用 Engine.IO Server | `TestOfficialEngineEntryPointsAndOptions` |
| 8 | Attach 后 Engine.IO 路径由 Engine 处理，未知 transport 返回官方 400/code 0/message | `TestOfficialEngineAttachHandlesItsPathAndPreservesFallback` |
| 9、11 | 未处理的非 Engine.IO Upgrade 会关闭；Node 的延迟销毁机制在 Go `net/http` 中由确定的 HTTP 错误响应替代 | `TestOfficialEngineAttachRejectsUnhandledUpgrade`；默认 AttachOptions 已与官方对齐为 destroy=true、timeout=1s |
| 10 | `destroyUpgrade:false` 保留未处理的 Node `upgrade` 事件 Socket | Go 没有 Node EventEmitter 式“无人处理的 upgrade Socket”；现有非 Engine.IO HTTP/WebSocket handler 始终保留，不会被 Attach 接管 |
| 12 | 已由应用处理的其他 Upgrade 在超时后仍保持可用 | `TestOfficialEngineAttachPreservesHandledUpgrade` 在超时边界后完成 WebSocket 回显 |
| 13 | Attach 保留原有 request handler | `TestOfficialEngineAttachHandlesItsPathAndPreservesFallback` 验证 Engine 路径隔离及原 fallback 的调用次数 |

`TestOfficialEngineJavaScriptClientCompatibilityMatrix` 额外锁定 `engine.io-client@3.5.6`、`4.1.4`、`6.6.6`：三代客户端分别验证 EIO 3/4 的 Polling、直接 WebSocket、Polling→WebSocket 升级、文本/二进制回显和多轮心跳，并验证严格服务器默认拒绝 EIO 3。

### `middlewares.js` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–2 | middleware 为 Polling 和 WebSocket 握手添加响应头 | `TestOfficialEngineMiddlewareAppliesToPollingAndWebSocket` |
| 3 | 所有 middleware 按注册顺序执行 | `TestOfficialEngineMiddlewareRunsAllFunctionsInOrder` |
| 4–5 | middleware 可主动以 503 结束 Polling/WebSocket 请求，且不得建立 Engine.IO 连接 | `TestOfficialEngineMiddlewareCanEndPollingAndWebSocketRequests` |
| 6–7 | Helmet 类安全 middleware 的响应头在 Polling/WebSocket 握手上保留 | `TestOfficialEngineMiddlewareSupportsSecurityAndSessionHeaders` |
| 8–9 | Session 类 middleware 的 `Set-Cookie` 在 Polling/WebSocket 握手上保留 | `TestOfficialEngineMiddlewareSupportsSecurityAndSessionHeaders` |
| 10–11 | middleware 返回错误时 Polling/WebSocket 均以 400 拒绝，且不得建立连接 | `TestOfficialEngineMiddlewareErrorsRejectPollingAndWebSocket` |
| 12 | 存在 middleware 时，超出 `maxHttpBufferSize` 的 Polling 数据仍不会到达 message listener，并以 transport error 关闭会话 | `TestServerPollingMaxPayloadRejectsContentLengthAndChunkedBodies` |

### `parser.js` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | EIO 3 在支持二进制时，将 Unicode 文本 `€€€€` 与二进制 `[1,2,3]` 编为官方相同的混合 payload，并可无损解码 | `TestOfficialV3ParserEncodesMixedPayload`（验证完整字节序列及反向解码） |

### `server.js` 第 1–24 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1–4 | 拒绝未知 transport、`constructor`、未知 SID 与被 `AllowRequest` 拒绝的 Polling，响应及 `connection_error` 的 code/message/context 与官方一致 | `TestServerVerificationMatchesOfficialErrors`；`TestServerVerificationAllowRequestRejection` |
| 5 | 被 `AllowRequest` 拒绝的直接 WebSocket 握手失败，不注册客户端，并发出 code 4 及拒绝消息 | `TestOfficialServerWebSocketVerificationErrors/AllowRequest_rejection` |
| 6–7 | 直接 WebSocket 和 Polling→WebSocket 升级阶段收到非法帧时不崩溃；现有 Polling 会话保持，服务仍接受新握手 | `TestOfficialServerSurvivesInvalidWebSocketHandshakeData` |
| 8 | 首次 Polling→WebSocket 升级成功后拒绝第二条升级连接，原 WebSocket 仍可双向收发 | `TestOfficialServerPreventsSecondWebSocketUpgrade` |
| 9–10 | Polling 与 WebSocket 均拒绝 `__proto__` transport，返回 code 0/`Transport unknown` 及对应事件上下文 | `TestServerVerificationMatchesOfficialErrors/prototype_transport`；`TestOfficialServerWebSocketVerificationErrors/prototype_transport` |
| 11 | 启用 Cookie 时使用 SID、默认名 `io`、Path `/`、HttpOnly 与 SameSite Lax | `TestServerPollingCookieAndHeaderEventsMatchOfficialBehavior` |
| 12–17 | 自定义名称、路径、禁用 Path、HttpOnly true/false 与 SameSite Strict 均生成官方相同 Cookie | `TestServerPollingCookieVariantsMatchOfficialBehavior` |
| 18 | JavaScript 的非布尔 `httpOnly` 会按 truthy 处理；Go 配置项为静态 `bool`，非法类型在编译期不可表示，默认 true 的线缆结果由 Cookie 测试覆盖 | Go 类型系统等价约束；`TestServerPollingCookieAndHeaderEventsMatchOfficialBehavior` |
| 19 | 默认不配置 Cookie 时不发送 `Set-Cookie` | `TestServerPollingCookieVariantsMatchOfficialBehavior/disabled_by_default` |
| 20 | 新客户端在 `connection` 事件前已按 SID 写入 clients，clientsCount 从 0 原子变为 1 | `TestOfficialServerRegistersNewClientBeforeConnectionEvent` |
| 21 | 自定义同步 ID 同时进入 OPEN、clients 表和服务端 Socket | `TestServerCustomAndRejectedGeneratedIDsMatchOfficialBehavior/custom_ID_polling`、`custom_ID_WebSocket` |
| 22 | 握手等待延迟完成的 ID 生成器，再用结果注册和返回 OPEN；对应官方 Promise ID 语义 | `TestOfficialServerWaitsForGeneratedID` |
| 23–24 | Polling 与 WebSocket 的 ID 生成错误均返回 code 3/`ID_GENERATION_ERROR`、触发事件且不残留客户端 | `TestServerCustomAndRejectedGeneratedIDsMatchOfficialBehavior/rejected_polling`、`rejected_websocket` |

### `server.js` 第 25–42 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 25–26 | OPEN 握手包含 SID、升级、心跳和最大载荷；自定义 `pingTimeout`/`pingInterval` 以毫秒写入 | `TestOfficialServerHandshakeDataAndUpgradeSuggestions` |
| 27–29 | 握手触发携带 Engine Socket 的 `connection`；默认 Polling 与直接 WebSocket 均使用正确 transport | `TestOfficialServerHandshakeDataAndUpgradeSuggestions`；`TestOfficialEngineJavaScriptClientCompatibilityMatrix` |
| 30–33 | 直接 WebSocket、仅 Polling、禁用升级及默认配置分别给出正确升级候选 | `TestOfficialServerHandshakeDataAndUpgradeSuggestions` |
| 34 | 已有 Polling SID 误以 WebSocket transport 发起普通 HTTP 请求时返回 code 3/`TRANSPORT_MISMATCH`，原会话仍可继续收发 | `TestOfficialServerRejectsTransportMismatchWithoutClosingSession` |
| 35–36 | 任意 URI query 数据在连接请求中保持可见，`EIO` 仍为字符串值 | `TestOfficialServerHandshakePreservesArbitraryQueryData`；官方客户端矩阵 |
| 37 | 未执行 WebSocket Upgrade 的 WebSocket transport 握手返回 code 3/`TRANSPORT_HANDSHAKE_ERROR` | `TestOfficialServerRejectsWebSocketTransportWithoutUpgrade` |
| 38 | 非法 Origin 返回 code 3/`INVALID_ORIGIN`，事件保留原值且请求头被清理 | `TestServerVerificationRejectsInvalidOrigin` |
| 39 | 非 GET 握手（官方 OPTIONS 用例）返回 code 2/`Bad handshake method`，事件包含 method | `TestServerVerificationMatchesOfficialErrors/bad_handshake_method` |
| 40 | 默认拒绝 EIO 3，HTTP 响应与事件均返回 code 5/`Unsupported protocol version`，context 中 protocol 为数字 3 | `TestServerVerificationMatchesOfficialErrors/unsupported_protocol`；`TestVerifyStrictEIO`；官方客户端矩阵严格模式 |
| 41 | `initialPacket` 与首个 Polling 握手 payload 拼接，且可安全复用于后续连接 | `TestOfficialServerInitialPacketIsSentOnEveryHandshake` |
| 42 | `addTrailingSlash:false` 同时接受 `/engine.io` 与 `/engine.io/foo/bar/` | `TestOfficialEngineAttachWithoutTrailingSlashUsesPrefixRouting` |

### `server.js` 第 43–64 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 43 | 服务端 Socket 的 writeBuffer 在 close listener 中仍可读，事件返回后再清空；未发送回调不得执行 | `TestServerDiscardClearsPendingCallbacksAfterCloseEvent` |
| 44 | Go Engine.IO 客户端 writeBuffer 在 close listener 中仍含待发送包，事件返回后清空 | `TestOfficialClientWriteBufferVisibleDuringClose` |
| 45–46 | 客户端不发送 PONG 时服务端以 `ping timeout` 关闭；没有挂起 Polling 请求时也会清理 Socket 和 clients 表 | `TestServerPingTimeoutWithoutOutstandingPoll`（EIO 3、EIO 4 Polling 与 EIO 4 WebSocket） |
| 47–48 | 客户端超时使用 `pingInterval + pingTimeout`；官方 v3/v4/v6 客户端与 Go 服务端均报告 `ping timeout` | `TestOfficialClientPingTimeoutUsesIntervalPlusTimeout`；官方 Engine.IO 客户端矩阵 `checkPingTimeoutOnBothEnds` |
| 49–50 | 服务端分别关闭 Polling/WebSocket 时，服务端原因为 `forced close`，官方 v3/v4/v6 客户端原因为 `transport close` | 官方 Engine.IO 客户端矩阵 `checkServerInitiatedClose` 及 Go 侧 reason channel |
| 51 | HTTP Server 在同一端口关闭并重新监听后，官方客户端重新连接且随后仍能收发 | Socket.IO 官方客户端矩阵 `checkServerRestart`（底层 Engine.IO WebSocket 生命周期） |
| 52–53 | 官方 v3/v4/v6 客户端分别关闭 Polling/WebSocket 时，客户端原因为 `forced close`，Go 服务端原因为 `transport close` | 官方 Engine.IO 客户端矩阵 `checkClientInitiatedClose` 及 Go 侧 reason channel |
| 54 | Polling 客户端处理首包时关闭后不会继续处理同一发送序列中的后续包；双方关闭原因保持官方值 | 官方 Engine.IO 客户端矩阵 `checkClientCloseWhileProcessingPayload` |
| 55 | 客户端在 Polling→WebSocket 升级期间关闭时安全结束且无迟到异常；候选 transport 随 Socket 关闭 | 官方 Engine.IO 客户端矩阵 `checkClientCloseDuringUpgrade`；`TestServerClosesUpgradingTransportWhenSocketCloses` |
| 56 | 缺少必要 WebSocket 握手头的 Upgrade 返回 400/`UPGRADE_FAILURE`，不注册客户端且服务可继续握手 | `TestOfficialServerRejectsFailedWebSocketUpgrade` |
| 57 | 挂起 Polling 请求的底层连接被取消时，以 `transport error` 和 `poll connection closed prematurely` 关闭 | `TestServerPollingRequestCancellationClosesTransport` |
| 58 | 每次请求携带 `Connection: close` 只关闭 HTTP 连接，不中止 Engine.IO 会话，后续消息仍可往返 | `TestServerConnectionCloseHeaderDoesNotAbortPollingSession` |
| 59–60 | 初次握手及后续心跳后均不会仅按 `pingTimeout` 提前关闭，正常官方客户端可跨多轮心跳保持连接 | `TestOfficialClientPingTimeoutUsesIntervalPlusTimeout`；官方矩阵 `checkMessagesAndHeartbeat` |
| 61 | 心跳期限内先发生 transport close 时客户端只报告 `transport close`，原心跳计时器被取消且不重复 close | `TestOfficialClientTransportCloseWinsBeforePingTimeout`；官方矩阵服务端主动关闭场景 |
| 62–63 | EIO 3 与 EIO 4 客户端均在完整的 `pingInterval + pingTimeout` 后报告 `ping timeout` | `TestOfficialClientPingTimeoutUsesIntervalPlusTimeout`；官方 v3/v4/v6 客户端矩阵 `checkPingTimeoutOnBothEnds` |
| 64 | 服务端关闭时会中止仍在读取的 Polling 数据请求，并结束 Socket/transport | `TestServerCloseAbortsInProgressPollingDataRequest` |

### `server.js` 第 65–82 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 65–66 | WebSocket/Polling 在 OPEN 前发生 transport error 时，Go 客户端不触发 open，以 `transport error` 关闭 Socket 与 transport | `TestOfficialClientTransportErrorBeforeOpen` |
| 67–68 | WebSocket/Polling 仍处于 opening 时调用客户端 `Close()`，只以 `forced close` 结束且双方状态均为 closed | `TestOfficialClientForcedCloseBeforeOpen` |
| 69–70 | EIO 4 WebSocket 和活跃 Polling 在无 PONG 后同时关闭 Socket 与底层 transport；无挂起 Poll 时仍清理 Socket | `TestServerPingTimeoutWithoutOutstandingPoll`；`TestOfficialServerPingTimeoutClosesActivePollingTransport` |
| 71–72 | WebSocket/Polling 收到非法 Engine.IO packet 时不触发 message，以 `parse error` 关闭 Socket 并关闭 transport | `TestServerClosesTransportOnParseError` |
| 73 | Socket 在升级过程中关闭时，候选 WebSocket transport 同步关闭 | `TestServerClosesUpgradingTransportWhenSocketCloses` |
| 74 | 未完成 probe 的候选 transport 在 `UpgradeTimeout` 后关闭，原 Polling Socket 保持 open 且清除 upgrading 状态 | `TestServerClosesUpgradingTransportOnTimeout` |
| 75 | 官方 v3/v4/v6 客户端完成 Polling→WebSocket 升级后跨多轮心跳仍保持 open | 官方 Engine.IO 客户端矩阵 `checkTransport` 的升级存活等待 |
| 76 | JavaScript `Object.prototype` 污染不适用于 Go；transport 和 client 注册表使用强类型 map，不存在原型链继承 | Go 语言层面不适用；真实客户端矩阵覆盖正常升级及关闭 |
| 77–78 | `Server.Close()` 对 open 或已进入 closing 的 Socket 都丢弃待发送包，不执行发送回调；close listener 后清空缓冲 | `TestOfficialServerCloseDiscardsPendingPackets` |
| 79 | 单个 Socket graceful close 会先送达待发送消息并执行回调，再触发 close | `TestServerGracefulCloseFlushesPendingPacketAndCallback` |
| 80 | `Server.Close()` 关闭普通 Socket/transport、清空 clients，并取消心跳，关闭后不再生成 PING | `TestOfficialServerCloseDiscardsPendingPackets` |
| 81 | `Server.Close()` 关闭已升级 WebSocket、清空 clients 和心跳计时器 | `TestOfficialServerCloseStopsUpgradedSocketAndTimers` |
| 82 | `Server.Close()` 在升级中同时关闭原 Socket 和候选 transport、清除 upgrading 状态和 clients | `TestOfficialServerCloseStopsUpgradingSocketAndTimers` |

### `server.js` 第 83–102 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 83–84 | Polling 客户端依次收到服务端发送的单条及多条文本消息，关闭前不丢包、不乱序 | 官方 Engine.IO 客户端矩阵 `checkOrderedMessagesBeforeClose`（v3/v4/v6） |
| 85 | Polling 请求体超过 `maxHttpBufferSize` 时拒绝消息并关闭 transport | `TestServerPollingMaxPayloadRejectsContentLengthAndChunkedBodies` |
| 86 | WebSocket 消息超过 `maxHttpBufferSize` 时拒绝消息并关闭 transport | `TestServerWebSocketRejectsPayloadAboveMaximum` |
| 87 | 小于 `maxHttpBufferSize` 的 Polling 消息正常到达 | `TestServerPollingAcceptsPayloadBelowMaximum` |
| 88–90 | WebSocket 客户端依次收到单条、多条延迟发送及连续发送的消息 | 官方矩阵 `checkOrderedMessagesBeforeClose`（v3/v4/v6，延迟与立即发送） |
| 91–95 | WebSocket/Polling 接收 TypedArray、切片与 Buffer 的等价二进制线缆数据 `[0,1,2,3,4]` | 官方矩阵 `checkServerToClientMessages`；Go 以 `[]byte` 表达上述 JavaScript 二进制视图的共同线缆语义 |
| 96、99 | WebSocket/Polling 客户端指定 `arraybuffer` 后仍无损接收服务端二进制消息 | 官方矩阵 `checkServerToClientMessages` 的 arraybuffer 模式 |
| 97–98 | Polling POST 请求体无论按多次写入还是 chunked transfer 分段传输，均完整解析为一条消息 | `TestOfficialServerPollingReceivesSplitAndChunkedPayloads` |
| 100 | Socket 发送数据时依次触发 `flush` 与 `drain`，Polling 客户端收到相同 payload | `TestOfficialServerEmitsFlushAndDrainEvents` |
| 101 | 100 条约 64 KiB 的缓冲消息按序送达，期间 PING/PONG 心跳不进入应用消息且连接保持存活 | 官方矩阵 `checkBufferedMessagesInterleaveWithPongs`（v3/v4/v6） |
| 102 | ASCII 与两组中文文本经官方客户端→Go 服务端→官方客户端往返后逐字一致 | 官方矩阵 `checkUnicodeRoundTrip` 及 Go 侧完成信号 |

### `server.js` 第 103–125 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 103 | 双向校验证书的 TLS Polling 连接可完成握手并向服务端发送消息 | `TestOfficialServerTLSClientAuthentication/key_and_certificate_polling` |
| 104 | 服务端请求客户端证书但不强制认证时，无客户端证书的 TLS Polling 仍可收发 | `TestOfficialServerTLSClientAuthentication/CA_without_required_authentication_polling` |
| 105 | 双向校验证书的 TLS WebSocket 连接可完成握手并向服务端发送消息 | `TestOfficialServerTLSClientAuthentication/key_and_certificate_websocket` |
| 106–107 | PFX 与 PEM 只是 Node/Go 载入客户端密钥和证书的不同容器；解码后的 TLS client Certificate 在线缆上相同，Polling/WebSocket 双向认证分别由第 103、105 项覆盖 | `TestOfficialServerTLSClientAuthentication`；Go 使用标准 `tls.Certificate`，不把 Node 专属 PFX 文件解析计作 Engine.IO 协议行为 |
| 108–109 | Go 客户端 Polling/WebSocket 的 `writeBuffer` 在 transport drain 前保持两项，随后按每批 drain 从 2 变为 1、再变为 0 | `TestOfficialClientWriteBufferDrainsAfterTransportWrite`；生产实现使用 `prevBufferLen` 延迟移除已提交包 |
| 110–113 | Go 客户端 Polling/WebSocket 的单包及同批 payload 发送回调按注册顺序执行且每项仅一次 | `TestOfficialClientSendCallbacksFollowFlushOrder`（两种 transport 各覆盖单包和二包同批 flush） |
| 114、116 | 服务端 Polling/WebSocket 发送完成回调与实际送达包一一对应 | `TestServerSendCallbacksExecuteInOrder` |
| 115 | Polling→WebSocket probe 窗口内发送的数据仍经原 Polling transport 到达，回调执行一次且关联 Polling | `TestServerSendCallbackDuringPollingUpgradeWindow` |
| 117 | 连续发送 a、b、c 时，每条消息只到达一次 | 官方矩阵 `checkOrderedMessagesBeforeClose`；`TestServerSendCallbacksExecuteInOrder` 同时验证三条独立回调 |
| 118–119 | WebSocket/Polling 同一批多包发送的四个回调均在对应 transport drain 后按序执行 | `TestServerSendCallbacksExecuteInOrder`（批量队列）；`TestServerGracefulCloseFlushesPendingPacketAndCallback`（回调先于关闭） |
| 120 | Socket 带待发送回调关闭时，清空 `packetsFn`/`sentCallbackFn` 引用且不执行失效回调 | `TestServerDiscardClearsPendingCallbacksAfterCloseEvent` |
| 121 | Polling 包未实际发送便关闭时，其回调不得执行 | `TestServerDiscardClearsPendingCallbacksAfterCloseEvent`；`TestOfficialServerCloseDiscardsPendingPackets` |
| 122 | 未启用 permessage-deflate 时 WebSocket 使用调用方提供的预编码帧 | `TestServerWebSocketPreEncodedContentMatchesOfficialBehavior/uses_pre-encoded_content` |
| 123 | 启用 permessage-deflate 时忽略预编码帧并按原始消息重新编码 | `TestServerWebSocketPreEncodedContentMatchesOfficialBehavior/ignores_pre-encoded_content_with_compression` |
| 124 | 服务端收到 MESSAGE 时先发出可独立读取的 `packet` 事件，随后 `message` 仍收到完整数据 | `TestServerPacketEventsAreObservableWithoutConsumingData` |
| 125 | EIO 4 收到客户端 PONG 时发出 `packet` 事件；对应 PING 的 `packetCreate` 事件也可观测 | `TestServerHeartbeatPacketEventsMatchOfficialBehavior` |

### `server.js` 第 126–153 项逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 126 | 服务端发送 MESSAGE 前发出 `packetCreate`，监听器读取数据不会消耗实际发送内容 | `TestServerPacketEventsAreObservableWithoutConsumingData` |
| 127 | EIO 3 回应客户端 PING 前发出 PONG `packetCreate`；EIO 4 主动 PING 的对应事件同样可观测 | `TestServerV3PongPacketCreateEventMatchesOfficialBehavior`；`TestServerHeartbeatPacketEventsMatchOfficialBehavior` |
| 128 | Polling→WebSocket 升级期间双方各连续发送 50 条消息，升级前后均严格有序、无丢失/重复，且官方客户端观察到 upgrading/upgrade 各一次 | 官方 Engine.IO v3/v4/v6 客户端矩阵 `checkTrafficDuringUpgrade`；Go 侧 `upgradeStressResults` 同时验证服务端收到 50 条并完成升级 |
| 129–134 | Polling 默认 gzip、指定 deflate、自定义阈值、全局禁用、单消息禁用及低于阈值不压缩 | `TestPollingHTTPCompressionMatchesOfficialBehavior`；另覆盖 q=0 与质量权重选择 |
| 135–136 | Go 客户端 `ExtraHeaders` 的自定义头与 Cookie 分别随 WebSocket Upgrade 和 Polling 握手到达服务端 | `TestOfficialClientExtraHeadersReachServer` |
| 137–139 | IE8/IE11 Polling 响应包含 `X-XSS-Protection: 0`，所有 Polling 响应包含 `Cache-Control: no-store` | `TestPollingResponseHeadersMatchOfficialBehavior` |
| 140–141 | Polling 首次握手触发一次 `initial_headers`，每次响应触发 `headers`；自定义头/Cookie 仅按对应事件范围出现 | `TestServerPollingCookieAndHeaderEventsMatchOfficialBehavior` |
| 142 | 直接 WebSocket 握手触发 `initial_headers`，自定义头和 Cookie 出现在 Upgrade 响应 | `TestServerWebSocketHeaderEventsMatchOfficialBehavior` |
| 143–144 | 同一 Polling→WebSocket 连接只触发一次 `initial_headers`，但握手与 Upgrade 分别触发 `headers` | `TestServerUpgradeEmitsHeadersButNotInitialHeaders` |
| 145 | CORS 预检反射允许的 Origin，并返回默认 methods、指定 headers、credentials 和 204 空响应 | `TestOfficialServerCORSResponses/current_origin_preflight` |
| 146 | 实际 CORS 握手仅返回 origin/credentials，不泄漏预检专属 methods/headers | `TestOfficialServerCORSResponses/current_origin_actual_request` |
| 147 | 不在允许列表中的 Origin 不获得 allow-origin/credentials 响应头 | `TestOfficialServerCORSResponses/bad_origin` |
| 148 | 自定义 methods、allowedHeaders、credentials、maxAge 和 success status 完整转发；exposedHeaders 不错误出现在预检响应 | `TestOfficialServerCORSResponses/custom_configuration` |
| 149 | 启用 CORS 后 Polling 握手和 POST 消息仍正常完成，且实际响应持续包含允许头 | `TestOfficialServerWorksWithCORSEnabled` |
| 150 | 官方 eiows 用例自身为 `it.skip`；Go 通过强类型 `WebSocketEngine` 接口加载替代实现，并把缓冲、压缩、Origin 与错误处理选项传入 | `TestOfficialServerUsesConfiguredWebSocketEngine`；`TestWebSocketEngineOptionAssignment` |
| 151–152 | Polling/WebSocket 的 `RemoteAddress()` 均返回可解析的客户端 IP，不包含临时端口 | `TestOfficialServerRemoteAddressIsClientIP`；生产实现统一经 `net.SplitHostPort` 规范化 |
| 153 | EIO 4 Polling POST 使用 `application/octet-stream` 发送二进制 body 时返回 HTTP 400，不把任意字节误作文本 payload | `TestOfficialEngineIO669RejectsBinaryEIO4PollingData` |

### `webtransport.mjs` 逐项索引

| 官方编号 | 行为 | 本项目证据 |
| --- | --- | --- |
| 1 | 真实 HTTP/3 WebTransport session 创建双向流，以空 OPEN 建立直接 Engine.IO 连接并收到完整 OPEN payload | `TestOfficialWebTransportDirectMessagesAndCallbacks` |
| 2 | Polling OPEN 宣告 WebTransport，客户端以 SID 完成 probe/upgrade，升级后继续双向收发 | `TestOfficialWebTransportUpgradeFromPolling`（真实同端口 TCP+UDP） |
| 3 | 已建立 session 但未创建双向流时，在 `upgradeTimeout` 后关闭 | `TestOfficialWebTransportRejectsMissingStreamAndInvalidHandshake/missing_bidirectional_stream` |
| 4 | 双向流发送非法握手帧时关闭 session，不建立 Engine Socket | `TestOfficialWebTransportRejectsMissingStreamAndInvalidHandshake/invalid_handshake` |
| 5 | 连续五轮服务端 PING/客户端 PONG 在线缆上分别为 `2`/`3` | `TestOfficialWebTransportPingPongAndCloseLifecycle/ping_pong` |
| 6 | 客户端不回应 PING 时 Socket 以 `ping timeout` 关闭，底层 session 同步结束 | `TestOfficialWebTransportPingPongAndCloseLifecycle/ping_timeout` |
| 7 | 服务端关闭 Socket 时底层 WebTransport session 结束 | `TestOfficialWebTransportPingPongAndCloseLifecycle/server_close` |
| 8 | 客户端关闭 session 时服务端稳定报告 `transport close`，而非 `transport error` | `TestOfficialWebTransportPingPongAndCloseLifecycle/client_close`；服务端与 Go 客户端均识别 `SessionError` 为关闭 |
| 9–10 | 文本消息客户端→服务端与服务端→客户端均保持 `hello`/`world` 内容和文本帧类型 | `TestOfficialWebTransportDirectMessagesAndCallbacks` |
| 11 | 服务端连续四次发送的完成回调与收到的四帧严格一一对应、按序执行 | `TestOfficialWebTransportDirectMessagesAndCallbacks` |
| 12–13 | 三字节二进制数据双向无损传输并保持二进制帧类型 | `TestOfficialWebTransportDirectMessagesAndCallbacks` |
| 14–15 | 1,000,000 字节二进制数据双向无损传输；大包结束后不产生额外空二进制帧 | `TestOfficialWebTransportDirectMessagesAndCallbacks`；`TestConnLargeWriterDoesNotAppendEmptyFrame` |
| 16–17 | WebTransport Upgrade 使用普通未知 SID 或 `__proto__` SID 时立即关闭 session，既不建立新客户端也不影响现有 Polling 客户端 | `TestOfficialEngineIO669WebTransportRejectsUnknownUpgradeSID` |
| 18 | 注册 HTTP middleware 后拒绝当前官方实现无法安全执行 middleware 的 WebTransport 握手，不发出 connection 且不泄漏 client | `TestOfficialEngineIO669WebTransportRejectsRegisteredMiddleware` |

## 官方客户端

客户端测试与服务端发布包闭包分开统计：

| 官方包 | 官方运行时分母 | 当前结果 | 类型/边界 | 逐项证据 |
| --- | ---: | ---: | --- | --- |
| `engine.io-client@6.6.6` | 108 个有效 `it()`，另有1个上游 `it.skip()` | 108/108 均已分类，所有可移植行为有Go等价证据 | 官方无独立类型测试分母；Blob、ArrayBuffer、Worker、旧IE及Node `autoUnref` 等宿主项明确标为部分等价或不适用 | [`clients/engine/OFFICIAL_TEST_MAPPING.md`](../clients/engine/OFFICIAL_TEST_MAPPING.md) |
| `socket.io-client@4.8.3` | 115 | 115/115已分类或映射 | 类型14/14另计；独立依赖 `@socket.io/component-emitter@3.1.2` 的16/16也表示源行已分类或映射 | [`clients/socket/OFFICIAL_4_8_3_AUDIT.md`](../clients/socket/OFFICIAL_4_8_3_AUDIT.md) |

这些结果表达可观察协议行为及Go平台等价，不表示复制浏览器、Promise、JavaScript对象外形或Node事件循环API。

## Parser 与基础 Adapter

| 官方包 | 官方运行时分母 | 当前结果 | 逐项证据 |
| --- | ---: | ---: | --- |
| `engine.io-parser@5.2.3` | 38 个互异用例（shared 19、Node 10、Browser 9） | 38/38已分类或映射 | [`parsers/engine/OFFICIAL_TEST_MAPPING.md`](../parsers/engine/OFFICIAL_TEST_MAPPING.md)；包括 WebTransport 1/3/9 字节长度头、任意分块、多包、binary bit 与 maxPayload 边界 |
| `socket.io-parser@4.2.7` | 34（parser 17、Buffer 8、ArrayBuffer 6、Blob 3） | 34/34已分类或映射 | [`parsers/socket/OFFICIAL_TEST_MAPPING.md`](../parsers/socket/OFFICIAL_TEST_MAPPING.md)；JavaScript 二进制容器分别映射为 Go `[]byte`/Buffer/`io.Reader` 线缆语义 |
| `socket.io-adapter@2.5.8` | 48（in-memory 21、cluster 27） | 48/48已分类或映射 | [`adapters/adapter/OFFICIAL_TEST_MAPPING.md`](../adapters/adapter/OFFICIAL_TEST_MAPPING.md)；Go 实际枚举21个内存子测试、26个集群子测试及1个独立 publish-failure 回归 |

这些分母来自各锁定官方 tag 的实际 `it()` 枚举；模块测试必须逐项列名并运行，不能只在汇总 JSON 中输出一个覆盖数字。

## Admin UI 协议

`@socket.io/admin-ui@0.5.1` 发布包没有携带测试文件，但其 `gitHead` 精确指向官方提交 `232d87af04777b108725bd70c248b5e929a5057d`。从该提交的 `test/index.ts` 与 `test/events.ts` 枚举出21个唯一测试声明：20项适用于Go，其中18项由锁定的官方 `socket.io-client@4.8.3` 通过真实EIO 4/WebSocket逐项执行，另外2项分别由内存Store和真实Redis执行；Node专属 `io.listen()` 1项明确标为宿主API不适用。完整逐项证据见 [`instrumentation/OFFICIAL_TEST_MAPPING.md`](../instrumentation/OFFICIAL_TEST_MAPPING.md)。

运行时矩阵验证：

- `session`、`config`、`server_stats`、`all_sockets`；
- `socket_connected`、`socket_updated`、`socket_disconnected` 与 Room 生命周期；
- `event_received`、`event_sent` 及 ACK 剥离；
- `emit`、`join`、`leave`、`_disconnect` 管理命令；
- bcrypt Basic Auth、16字符 session ID 及仅凭 `sessionId` 的重新连接；
- `rawConnection`/`rawDisconnection` 与 packets/bytes 双向聚合统计。

`RedisStore` 同步实现官方默认键 `socket.io-admin#<sessionId>`、86400秒TTL及可配置前缀/TTL，并使用单条带过期时间的Redis `SET`。协议字段和事件顺序以官方0.5.1源码与发布包 `dist/index.js` 为基准；Go允许额外的自定义认证中间件，属于扩展而非官方表面API。

## 非云官方 Adapter

以下统计只覆盖用户指定的非云 Adapter，不包含 Google Cloud、AWS 或 Azure。原始125项必须按“可适用行为、上游主动跳过、宿主不适用”分别报告：

| 官方包 | 原始项 | Go可适用且有自动化证据 | 上游 `skip` | 宿主N/A | 逐项证据 |
| --- | ---: | ---: | ---: | ---: | --- |
| `@socket.io/redis-adapter@8.3.0` | 32 | 30 | 1 | 1 | [`adapters/redis/OFFICIAL_TEST_MAPPING.md`](../adapters/redis/OFFICIAL_TEST_MAPPING.md) |
| `@socket.io/redis-streams-adapter@0.3.1` | 30 | 29 | 1 | 0 | [`adapters/redis/OFFICIAL_TEST_MAPPING.md`](../adapters/redis/OFFICIAL_TEST_MAPPING.md) |
| `@socket.io/mongo-adapter@0.4.0` | 36 | 36 | 0 | 0 | [`adapters/mongo/OFFICIAL_TEST_MAPPING.md`](../adapters/mongo/OFFICIAL_TEST_MAPPING.md) |
| `@socket.io/postgres-adapter@0.5.0` | 27 | 25 | 1 | 1 | [`adapters/postgres/OFFICIAL_TEST_MAPPING.md`](../adapters/postgres/OFFICIAL_TEST_MAPPING.md) |
| **合计** | **125** | **120** | **3** | **2** | — |

Redis 与 Redis Streams 的可适用项包含锁定官方 Node Adapter 的真实双向矩阵。MongoDB 和 PostgreSQL 的共享 Cluster Adapter 行为由同名进程内回归逐项断言，数据库线协议另由真实 Node↔Go 综合矩阵覆盖；这不等于125个上游 TypeScript 测试被原样执行，也不能写成“125/125测试通过”。

## Cluster Adapter 18项部署等价映射

基准为官方 `@socket.io/cluster-adapter@0.3.0`，源码位于同一官方提交 `9978574e4f1d4e21593497f94c40053cd0fff359`。Go 不复制 Node `cluster` IPC API，而由通用 `ClusterAdapterWithHeartbeat` 和 Unix Domain Socket Adapter 承担同机进程通信。`TestOfficialClusterDeploymentEquivalence` 启动三个真实 Go 子进程，并以官方 `socket.io-client@4.8.3` 验证：

| 官方编号 | 行为 | 本项目证据 |
|---:|---|---|
| 1–5 | 全局、Namespace、Room、Except 与 Local 广播 | 三进程矩阵的文本广播、`/custom` 隔离、Room、排除与同节点限定 |
| 6–9 | 多客户端 ACK、二进制 ACK、零接收者与部分超时 | 六个客户端的完整 ACK 集、Buffer ACK、空 Room 零响应及一端不回应的部分结果/超时 |
| 10–13 | 全部或按条件 `socketsJoin`、`socketsLeave` | 全集远程 Join/Leave，以及以 source Room 筛选后的精确 Room 状态查询 |
| 14 | 跨节点断开 Socket | 目标 Room 断开及最终全集断开，客户端观察官方断开路径 |
| 15 | `fetchSockets` 返回所有进程实例 | 六个客户端分布在三个进程时集群查询返回6 |
| 16–18 | 无 ACK 的 `serverSideEmit`、全节点 ACK 和节点不回应超时 | 仅其他节点本地观察事件、两个远端节点响应、指定节点不响应时返回部分结果与超时 |

同一测试随后强制终止一个 Worker，等待心跳成员收敛，再加入新 Worker；存活节点和新旧重叠节点均继续通过 `fetchSockets`、`serverSideEmitWithAck` 和广播。

## Cluster Engine 18项实现与验证结果

基准为官方 `@socket.io/cluster-engine@0.1.0`。Go `engine.ClusterServer` 已实现跨监听器的读写锁、Polling 包转发、关闭通知、Upgrade 接管和升级前缓冲；`MemoryClusterBus` 用于同一Go进程内的多监听器，以每订阅者异步队列交付，对有序 `Publish` 调用保持FIFO，且不在 `Publish` 调用栈内重入listener；`adapters/redis/enginebus` 则按官方 `_eio` MessagePack envelope 及 `<prefix>#`、`<prefix>#<recipient>#` Channel 传输到其他进程或主机。

| 官方编号 | 官方测试重点 | 本项目证据与当前状态 |
|---:|---|---|
| 1–12 | 远端读、延迟读、写、多包写、同/异实例 read lock、两端关闭、十轮 PING/PONG、非法 SID、远端 Upgrade 及升级前缓冲 | `TestOfficialClusterEngine010InMemory` 逐项使用多个 `ClusterServer` 和 `MemoryClusterBus` 将同一会话请求发送到非 owner 监听器；12项已通过 |
| 13–14 | Node Cluster 模式的心跳与二进制 | `TestOfficialClusterEngine010NodeCluster` 在三个Go `ClusterServer` 间对每次请求做无粘性 round-robin，验证十轮心跳和二进制往返；2项部署行为等价已通过，但不冒充Node OS worker/IPC API |
| 15–18 | `redis` 与 `ioredis` 两种 Redis Engine 的心跳和二进制 | `TestOfficialClusterEngine010Redis` 覆盖Go三实例线路；`TestOfficialNodeClusterEngine010RedisInterop` 启动真实官方Node `RedisEngine`，分别以Node和Go作为会话owner验证双向请求；两组在真实Redis上以 `-race -count=10` 复验通过，4项闭合 |

Redis 线路的结论是“Go `ClusterServer` 可以直接与官方Node `RedisEngine` 交换0.1.0报文”，不是“复制了官方Node进程拓扑”。Go不提供 `setupPrimary()`、`NodeClusterEngine`、`setupPrimaryWithRedis()`、primary/worker IPC relay、进程句柄传递、类继承或worker映射；13–14项属于Go部署行为等价，15–18项属于直接 `RedisEngine` 线协议互操作。

`ClusterServer` 默认生成20字符base64url SID。自定义 `SetGenerateId` 若要跨Go实例或与Node混部，也必须继续返回20字符并满足本项目 `utils.IsValidSid` 安全字符集，建议使用base64url；其他长度只会保留本地处理，不能作为跨运行时路由承诺。官方Cluster Engine对未知SID只检查长度20，因此包含特殊符号的20字符SID可能被官方Node路由、却会被Go以400拒绝，这是刻意的安全加严和已知差异。

官方 `engine.io@6.6.9` 的 `BaseServer.onWebTransportSession` 不经过普通请求的 `verify`，`@socket.io/cluster-engine@0.1.0` 也没有覆盖该入口；远端SID不在当前Node本地clients时会直接关闭。Go当前同样不提供跨节点WebTransport接管。内部可锁定transport枚举出现 `webtransport` 不代表存在可用的跨节点入口，这是双方共同边界，不计为Go独有缺口。

官方Cluster Engine 0.1.0的快速接管实现以固定协议版本4构造新Engine.IO Socket，Go也保持这一行为。因此本项目不把普通连接的EIO 3/Socket.IO v2兼容矩阵解释为“跨节点快速接管支持EIO 3”；这是官方与Go共有的0.1.0边界。

官方 `@socket.io/sticky@2.0.1` 的10项顶层测试行为——least-connection、round-robin、random、WebSocket-only、Polling-only、CORS、无 Worker 时 Polling/WebSocket 503、跨 TCP 连接 SID 亲和，以及10 KB/1 MB（Content-Length/chunked）上传——由 `sticky/router_test.go` 与官方客户端矩阵覆盖。`sticky.Router` 现在是可选的owner-affinity部署替代，不是 `ClusterServer` 包转发实现，也不计入Cluster Engine的18项分母。

## 已锁定的外部互操作矩阵

- Socket.IO Client 4.8.3 的 115 项运行时测试及 14 项类型测试另见 `clients/socket/OFFICIAL_4_8_3_AUDIT.md`；其中类型层的独立 14/14 编译契约见 `typed/OFFICIAL_CLIENT_TYPES_MAPPING.md`。

- `socket.io-client@2.5.0`、`3.1.3`、`4.8.3`；
- `engine.io-client@3.5.6`、`4.1.4`、`6.6.6`；
- `@socket.io/redis-adapter@8.2.1`、`8.3.0`；
- `@socket.io/redis-streams-adapter@0.3.0`、`0.3.1`；
- Redis Pub/Sub、Sharded Pub/Sub、Streams 的 Node/Go 双向广播、ACK、集群操作、故障恢复和滚动升级。
- `@socket.io/mongo-adapter@0.4.0`（官方 tag 提交 `eff10ab63ecfd5753d7985f46fd4278058f8570b`）：Node/Go 双向广播、二进制、Room、`fetchSockets`、`serverSideEmitWithAck`、远程 Join、两方向会话恢复、节点退出及滚动重启；矩阵同时防止官方 Mongo 消息类型 13（SESSION）被误判为新版通用协议的 ADAPTER_CLOSE；
- `@socket.io/postgres-adapter@0.5.0`（官方 tag 提交 `a9300c13376d1e953b3beaba5bf150eca2f365fb`）：Node/Go 双向 JSON 通知、二进制与 20 KB MessagePack attachment、Room、`fetchSockets`、`serverSideEmitWithAck`、远程 Join、节点退出及滚动重启；官方实现明确不支持连接恢复，因此恢复不作为跨实现声明。
- 四个非云 external emitter 的官方测试总分母为53，真实双向结果53/53、失败0：[`redis-emitter@5.1.0` 17项与 `redis-streams-emitter@0.1.1` 12项](../adapters/redis/testdata/interop/EMITTER_TEST_MAPPING.md)、[`postgres-emitter@0.1.1` 12项](../adapters/postgres/testdata/official-interop/EMITTER_TEST_MAPPING.md)、[`mongo-emitter@0.2.0` 12项](../adapters/mongo/testdata/official-interop/EMITTER_TEST_MAPPING.md)。
- `@socket.io/cluster-adapter@0.3.0`：三个Go子进程和官方客户端完成18项部署等价行为、强制退出收敛与滚动加入；`@socket.io/sticky@2.0.1` 的10项行为已映射。`@socket.io/cluster-engine@0.1.0` 已完成18/18分类与映射，Redis 15–18项包含官方Node `RedisEngine` 双向直接互操作及高频 `-race` 门禁。

## 更新规则

每次将一项标记为“真实互操作”前，必须记录锁定的官方包版本，并由 CI 中默认关闭、显式环境变量开启的端到端测试提供证据。仅有 API 名称相同或单元测试通过，不足以宣称官方行为完全兼容。
