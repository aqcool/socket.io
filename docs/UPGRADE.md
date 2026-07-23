# 升级指南

## 目录

- [v3 新特性](#v3-新特性)
- [从 v1/v2 升级到 v3](#从-v1v2-升级到-v3)
  - [预计升级时间](#预计升级时间30--60-分钟)
  - [高影响改动](#高影响改动)
  - [中等影响改动](#中等影响改动)
  - [低影响改动](#低影响改动)
  - [更新依赖](#更新依赖)
  - [导入路径更新](#导入路径更新)
  - [破坏性改动](#破坏性改动)
  - [快速开始示例](#快速开始示例)
  - [测试升级结果](#测试升级结果)
  - [常见问题](#常见问题)
  - [获取帮助](#获取帮助)
- [发布说明](#发布说明)
  - [v3.0.0](#v300)
  - [v3.0.0-rc.14](#v300-rc14)
  - [v3.0.0-rc.13](#v300-rc13)
  - [v3.0.0-rc.12](#v300-rc12)
  - [v3.0.0-rc.8](#v300-rc8)
  - [v3.0.0-rc.4](#v300-rc4)
  - [v3.0.0-rc.2](#v300-rc2)
  - [v3.0.0-beta.1](#v300-beta1)
  - [v3.0.0-alpha.0 ~ alpha.4](#v300-alpha0--alpha4)

---

## v3 新特性

Socket.IO Go **v3.0.0** 是一个大版本更新，主要改进如下：

| 特性 | 说明 |
|---------|-------------|
| **单体仓库整合** | 原先独立的多个仓库已合并为一个单体仓库，并划分为带版本的子模块 |
| **统一版本管理** | 所有模块共享 `pkg/version` 中的版本定义，保证整个生态版本一致 |
| **协议对齐** | 对齐 Socket.IO v4+ 协议，提高与 JavaScript 生态的兼容性 |
| **线程安全** | 全面修复并发问题，包括原子 Socket 标志、互斥保护的中间件、写时复制及 goroutine 泄漏防护 |
| **类型安全** | 使用泛型 `types.Atomic[T]` 替代 `atomic.Value`，提供 `types.Optional[T]` 和强类型 `Handshake` 字段 |
| **新增工具包** | `pkg/slices` 提供安全切片操作，`pkg/queue` 保证消息顺序，`pkg/request` 提供 HTTP 客户端 |
| **Redis Cluster 支持** | 支持分片广播、修复 CROSSSLOT 错误并提供动态频道订阅管理 |
| **DoS 防护** | 限制 Polling 传输的 HTTP 请求体大小，并支持配置附件数量上限 |
| **Go 1.26.0** | 最低 Go 版本更新为 1.26.0 |

### 模块架构

```
github.com/aqcool/socket.io/
├── v3                          # Root: shared types, interfaces
├── parsers/
│   ├── engine/v3               # Engine.IO protocol parser
│   └── socket/v3               # Socket.IO protocol parser
├── servers/
│   ├── engine/v3               # Engine.IO server
│   └── socket/v3               # Socket.IO server
├── clients/
│   ├── engine/v3               # Engine.IO client
│   └── socket/v3               # Socket.IO client
└── adapters/
    ├── adapter/v3              # Base adapter interface
    └── redis/v3                # Redis adapter (+ emitter)
```

---

## 从 v1/v2 升级到 v3

### 预计升级时间：30～60 分钟

建议完整阅读本指南以了解全部改动。升级过程会整合依赖并更新导入路径，以对齐 Socket.IO v4+ 协议，同时带来性能提升和 API 更新。

### 高影响改动

<details>
<summary>依赖结构整合</summary>

所有 Socket.IO 相关包现已整合到 `github.com/aqcool/socket.io/` 主仓库，并按子模块进行版本管理。

**影响概率：非常高**

此改动会影响应用中的所有相关导入，必须全部更新为新的 v3 路径。
</details>

<details>
<summary>协议兼容性更新</summary>

Socket.IO v3 对齐 Socket.IO v4+ 协议，因此所有客户端连接都会受到兼容性变化的影响。

**影响概率：非常高**

客户端 Socket.IO 库必须升级到 4.x 或更高版本。使用旧版本（v2.x 或 v3.x）的客户端无法连接 v3 服务端。

```bash
# Update your frontend dependency
npm install socket.io-client@^4.0.0
```

**必须执行：**

- 与前端团队协调升级客户端库
- 升级后测试所有客户端连接
- 如需灰度发布，请制定向后兼容策略

</details>

<details>
<summary>导入路径重构</summary>

所有 Socket.IO 导入路径都需要更新为新的 v3 结构，涉及 8 类主要包。

**影响概率：非常高**

需要系统性更新整个代码库中的包导入，包括：

- Engine.IO 解析器（`parsers/engine/v3`）
- Socket.IO 解析器（`parsers/socket/v3`）
- Engine.IO 服务端（`servers/engine/v3`）
- Socket.IO 服务端（`servers/socket/v3`）
- Redis 适配器（`adapters/redis/v3`）
- Valkey 适配器（`adapters/valkey/v3`，v3 新增）
- Engine.IO 客户端（`clients/engine/v3`）
- Socket.IO 客户端（`clients/socket/v3`）
- 公共类型和工具（`v3/pkg`）

完整映射请参阅[导入路径更新](#导入路径更新)。
</details>

<details>
<summary>Redis 适配器类型变更</summary>

Redis 适配器已用 `types.Atomic[string]` 替换 `types.String`，以增强类型安全。

**影响概率：高（使用 Redis 适配器时）**

```go
// Before
import "github.com/zishang520/socket.io-go-redis/types"
var s types.String

// After
import "github.com/aqcool/socket.io/v3/pkg/types"
var s types.Atomic[string]
```

</details>

### 中等影响改动

<details>
<summary>Socket 握手类型更新</summary>

`socket.Handshake` 结构现使用更明确的强类型字段：

**影响概率：中**

```go
// Before
type Handshake struct {
    Headers map[string][]string
    Query   map[string][]string
    Auth    any
}

// After
type Handshake struct {
    Headers types.IncomingHttpHeaders  // provides Header() method
    Query   types.ParsedUrlQuery       // provides Query() method
    Auth    map[string]any
}
```

必须更新访问方式：

```go
// Before
headers := socket.Handshake().Headers
userAgent := headers["user-agent"][0]

// After
headers := socket.Handshake().Headers.Header()
userAgent := headers.Get("User-Agent")
```

</details>

<details>
<summary>HttpContext API 重构</summary>

`*types.HttpContext` 的部分方法和属性已重命名，或由属性重构为方法。

**影响概率：中**

```go
// Before
func example(ctx *types.HttpContext) {
    headers := ctx.ResponseHeaders
    host := ctx.GetHost()
    method := ctx.GetMethod()
    values := ctx.Gets("foo")
    value := ctx.Get("bar")
    path := ctx.GetPathInfo()
}

// After
func example(ctx *types.HttpContext) {
    headers := ctx.ResponseHeaders()
    host := ctx.Host()
    method := ctx.Method()
    values, _ := ctx.Query().Gets("foo")
    value, _ := ctx.Query().Get("bar")
    path := ctx.PathInfo()
}
```

**变更摘要：**

- `ResponseHeaders` → `ResponseHeaders()`（属性改为方法）
- `GetHost()` → `Host()`
- `GetMethod()` → `Method()`
- `Gets(key)` → `Query().Gets(key)`
- `Get(key)` → `Query().Get(key)`
- `GetPathInfo()` → `PathInfo()`

**新增或更新的方法：**

| 方法 | 说明 |
|--------|-------------|
| `Path()` | 返回去除首尾斜杠的规范化路径 |
| `UserAgent()` | 返回 User-Agent 请求头值 |
| `Secure()` | TLS 连接时返回 `true` |
| `SetStatusCode(code)` | 现在返回用于校验的 `error` |
| `IsDone()` | 检查响应是否已写入 |
| `Done()` | 返回 `<-chan struct{}`，不再返回 `<-chan Void` |

</details>

<details>
<summary>配置项 GetRaw* 方法变更</summary>

所有 `GetRaw*` 方法现在返回 `types.Optional[T]` 而非指针类型，以提升空值安全性：

**影响概率：中**

```go
// Before
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", *duration)
    }
}

// After
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", duration.Get())
    }
}
```

</details>

<details>
<summary>ParameterBag 包迁移</summary>

`ParameterBag` 已从 `utils` 包迁移到 `types` 包。

**影响概率：中**

```go
// Before
import "github.com/aqcool/socket.io/v3/pkg/utils"

func example() {
    var bag *utils.ParameterBag
    bag = utils.NewParameterBag(nil)
}

// After
import "github.com/aqcool/socket.io/v3/pkg/types"

func example() {
    var bag *types.ParameterBag
    bag = types.NewParameterBag(nil)
}
```

</details>

<details>
<summary>适配器工具函数重组</summary>

`SliceMap`、`Tap` 等工具函数已从 `adapter` 包迁移到专用的 `pkg` 子包。

**影响概率：中**

```go
// Before
import "github.com/aqcool/socket.io/adapters/adapter/v3"

func example() {
    adapter.SliceMap(/**/)
    adapter.Tap(/**/)
}

// After
import (
    "github.com/aqcool/socket.io/v3/pkg/slices"
    "github.com/aqcool/socket.io/v3/pkg/utils"
)

func example() {
    slices.Map(/**/)
    utils.Tap(/**/)
}
```

**变更摘要：**

- `adapter.SliceMap` → `slices.Map`（迁移至 `pkg/slices`）
- `adapter.Tap` → `utils.Tap`（迁移至 `pkg/utils`）

**`pkg/slices` 中的新增函数：**

新的 `pkg/slices` 包提供以下工具函数：

| 函数 | 说明 |
|----------|-------------|
| `Get(s, idx)` | 检查边界后安全获取元素 |
| `GetAny[O](vals, idx)` | 从 `[]any` 获取元素并进行类型断言 |
| `TryGet(s, idx)` | 越界时返回零值 |
| `TryGetAny[O](vals, idx)` | 从 `[]any` 进行类型断言，失败时返回零值 |
| `GetWithDefault(s, idx, def)` | 越界时返回默认值 |
| `GetPtr(s, idx)` | 返回元素指针，越界时返回 nil |
| `Slice(s, start)` | 检查边界后安全截取子切片 |
| `First(s)` / `Last(s)` | 安全获取首个或末尾元素 |
| `Filter(s, predicate)` | 按条件筛选元素 |
| `Map(vals, transform)` | 转换每个元素 |
| `Reduce(vals, initial, reducer)` | 将元素归约为单个值 |
| `IsEmpty(s)` | 检查切片是否为 nil 或空 |
| `IsValidIndex(s, idx)` | 检查索引是否有效 |

</details>

<details>
<summary>ExtendedError 类型整合</summary>

`clients/socket` 和 `servers/socket` 中各自的 `ExtendedError` 已整合为 `pkg/types` 中的共享实现，消除了重复代码，并为整个代码库提供统一的错误类型。

**影响概率：中**

```go
// Before (client-side)
import "github.com/zishang520/socket.io-client-go/socket"

err := socket.NewExtendedError("connection failed", nil)

// Before (server-side)
import "github.com/zishang520/socket.io/v2/socket"

err := socket.NewExtendedError("middleware error", map[string]any{"code": 401})
data := err.Data()  // Note: server-side had Data() method

// After (unified)
import "github.com/aqcool/socket.io/v3/pkg/types"

err := types.NewExtendedError("error message", map[string]any{"code": 401})
data := err.Data  // Now uses direct field access
```

**主要变更：**

- `clients/socket.ExtendedError` → `types.ExtendedError`
- `servers/socket.ExtendedError` → `types.ExtendedError`（保留类型别名以向后兼容）
- 服务端的 `Data()` 方法改为 `Data` 字段
- 客户端和服务端现在共享同一个 `ExtendedError` 实现

**注意：** 为保持向后兼容，服务端 `socket` 包保留了 `ExtendedError` 类型别名和 `NewExtendedError` 包装函数，因此现有服务端代码可能无需修改；客户端代码则必须更新导入。
</details>

<details>
<summary>Redis SubscriptionMode 类型迁移</summary>

`SubscriptionMode` 已从 `adapters/redis/adapter` 包迁移到根 `adapters/redis` 包，以便适配器和发射器共享。

**影响概率：中（使用 Redis 分片适配器时）**

```go
// Before
import "github.com/aqcool/socket.io/adapters/redis/v3/adapter"

opts := adapter.NewShardedRedisAdapterOptions()
opts.SetSubscriptionMode(adapter.DynamicSubscriptionMode)

// After
import (
    "github.com/aqcool/socket.io/adapters/redis/v3"
    "github.com/aqcool/socket.io/adapters/redis/v3/adapter"
)

opts := adapter.NewShardedRedisAdapterOptions()
opts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
```

**主要变更：**

| 之前 | 之后 |
|--------|-------|
| `adapter.SubscriptionMode` | `redis.SubscriptionMode` |
| `adapter.StaticSubscriptionMode` | `redis.StaticSubscriptionMode` |
| `adapter.DynamicSubscriptionMode` | `redis.DynamicSubscriptionMode` |
| `adapter.DynamicPrivateSubscriptionMode` | `redis.DynamicPrivateSubscriptionMode` |

**新增内容：**

- `redis.DefaultSubscriptionMode`——默认模式常量
- `redis.PrivateRoomIdLength`——用于识别私有房间的长度常量
- `redis.ShouldUseDynamicChannel(mode, room)`——共享辅助函数

**发射器选项扩展：**

`EmitterOptions` 现在支持分片 Pub/Sub 配置：

```go
emitterOpts := emitter.NewEmitterOptions()
emitterOpts.SetSharded(true)
emitterOpts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
```
</details>

### 低影响改动

<details>
<summary>调试日志改进</summary>

调试日志已更新，使所有包的输出格式更加一致。

**影响概率：低**

无需修改代码，但日志输出格式可能略有变化。
</details>

<details>
<summary>内部类型重组</summary>

为提升可维护性，部分内部类型已重新组织。这些改动不会影响公开 API，但可能影响依赖内部类型的代码。

**影响概率：低**

如果代码导入了内部包，请在升级后检查相关导入。
</details>

## 更新依赖

更新 `go.mod`，引入 Socket.IO v3 包：

```bash
go get github.com/aqcool/socket.io/v3@latest
go get github.com/aqcool/socket.io/parsers/engine/v3@latest
go get github.com/aqcool/socket.io/parsers/socket/v3@latest
go get github.com/aqcool/socket.io/servers/engine/v3@latest
go get github.com/aqcool/socket.io/servers/socket/v3@latest
go get github.com/aqcool/socket.io/adapters/adapter/v3@latest
go get github.com/aqcool/socket.io/adapters/redis/v3@latest
go get github.com/aqcool/socket.io/clients/engine/v3@latest
go get github.com/aqcool/socket.io/clients/socket/v3@latest
```

更新后清理依赖：

```bash
go mod tidy
```

`go.mod` 示例：

```go
require (
    github.com/aqcool/socket.io/v3 v3.0.0
    github.com/aqcool/socket.io/parsers/engine/v3 v3.0.0
    github.com/aqcool/socket.io/parsers/socket/v3 v3.0.0
    github.com/aqcool/socket.io/servers/engine/v3 v3.0.0
    github.com/aqcool/socket.io/servers/socket/v3 v3.0.0
    github.com/aqcool/socket.io/adapters/adapter/v3 v3.0.0
    github.com/aqcool/socket.io/adapters/redis/v3 v3.0.0
    github.com/aqcool/socket.io/clients/engine/v3 v3.0.0
    github.com/aqcool/socket.io/clients/socket/v3 v3.0.0
)
```

---

## 导入路径更新

请参考以下表格更新应用中的所有 Socket.IO 导入路径：

### Engine.IO 解析器

| v1/v2 导入 | v3 导入 |
|--------------|-----------|
| `github.com/zishang520/engine.io-go-parser/packet` | `github.com/aqcool/socket.io/parsers/engine/v3/packet` |
| `github.com/zishang520/engine.io-go-parser/parser` | `github.com/aqcool/socket.io/parsers/engine/v3/parser` |
| `github.com/zishang520/engine.io-go-parser/types` | `github.com/aqcool/socket.io/v3/pkg/types` |
| `github.com/zishang520/engine.io-go-parser/utils` | `github.com/aqcool/socket.io/v3/pkg/utils` |

### Socket.IO 解析器

| v1/v2 导入 | v3 导入 |
|--------------|-----------|
| `github.com/zishang520/socket.io-go-parser/parser` | `github.com/aqcool/socket.io/parsers/socket/v3/parser` |
| `github.com/zishang520/socket.io-go-parser/v2/parser` | `github.com/aqcool/socket.io/parsers/socket/v3/parser` |

### Engine.IO 服务端

| v1/v2 导入 | v3 导入 |
|--------------|-----------|
| `github.com/zishang520/engine.io/config` | `github.com/aqcool/socket.io/servers/engine/v3/config` |
| `github.com/zishang520/engine.io/v2/config` | `github.com/aqcool/socket.io/servers/engine/v3/config` |
| `github.com/zishang520/engine.io/engine` | `github.com/aqcool/socket.io/servers/engine/v3` |
| `github.com/zishang520/engine.io/v2/engine` | `github.com/aqcool/socket.io/servers/engine/v3` |
| `github.com/zishang520/engine.io/errors` | `github.com/aqcool/socket.io/servers/engine/v3/errors` |
| `github.com/zishang520/engine.io/v2/errors` | `github.com/aqcool/socket.io/servers/engine/v3/errors` |
| `github.com/zishang520/engine.io/events` | `github.com/aqcool/socket.io/v3/pkg/events` |
| `github.com/zishang520/engine.io/v2/events` | `github.com/aqcool/socket.io/v3/pkg/events` |
| `github.com/zishang520/engine.io/log` | `github.com/aqcool/socket.io/v3/pkg/log` |
| `github.com/zishang520/engine.io/v2/log` | `github.com/aqcool/socket.io/v3/pkg/log` |
| `github.com/zishang520/engine.io/transports` | `github.com/aqcool/socket.io/servers/engine/v3/transports` |
| `github.com/zishang520/engine.io/v2/transports` | `github.com/aqcool/socket.io/servers/engine/v3/transports` |
| `github.com/zishang520/engine.io/types` | `github.com/aqcool/socket.io/v3/pkg/types` |
| `github.com/zishang520/engine.io/v2/types` | `github.com/aqcool/socket.io/v3/pkg/types` |
| `github.com/zishang520/engine.io/utils` | `github.com/aqcool/socket.io/v3/pkg/utils` |
| `github.com/zishang520/engine.io/v2/utils` | `github.com/aqcool/socket.io/v3/pkg/utils` |
| `github.com/zishang520/engine.io/v2/webtransport` | `github.com/aqcool/socket.io/v3/pkg/webtransport` |

### Socket.IO 服务端

| v1/v2 导入 | v3 导入 |
|--------------|-----------|
| `github.com/zishang520/socket.io/socket` | `github.com/aqcool/socket.io/servers/socket/v3` |
| `github.com/zishang520/socket.io/v2/socket` | `github.com/aqcool/socket.io/servers/socket/v3` |
| `github.com/zishang520/socket.io/v2/adapter` | `github.com/aqcool/socket.io/adapters/adapter/v3` |

### Redis 适配器

| v1 导入 | v3 导入 |
|-----------|-----------|
| `github.com/zishang520/socket.io-go-redis/adapter` | `github.com/aqcool/socket.io/adapters/redis/v3/adapter` |
| `github.com/zishang520/socket.io-go-redis/emitter` | `github.com/aqcool/socket.io/adapters/redis/v3/emitter` |
| `github.com/zishang520/socket.io-go-redis/types` | `github.com/aqcool/socket.io/adapters/redis/v3` |

### Redis 适配器内部迁移（v3）

| 之前（adapter 子包） | 之后（redis 根包） |
|-----------------------------|----------------------------|
| `adapter.SubscriptionMode` | `redis.SubscriptionMode` |
| `adapter.StaticSubscriptionMode` | `redis.StaticSubscriptionMode` |
| `adapter.DynamicSubscriptionMode` | `redis.DynamicSubscriptionMode` |
| `adapter.DynamicPrivateSubscriptionMode` | `redis.DynamicPrivateSubscriptionMode` |

### Valkey 适配器（v3 新增）

Valkey 适配器是 v3 新增的独立模块，功能与 `adapters/redis` 模块一致，但使用 [`valkey-go`](https://github.com/valkey-io/valkey-go) 客户端。

```bash
go get github.com/aqcool/socket.io/adapters/valkey/v3@latest
```

| 包 | 导入路径 |
|---------|-------------|
| 根类型和客户端 | `github.com/aqcool/socket.io/adapters/valkey/v3` |
| 经典、分片及 Streams 适配器 | `github.com/aqcool/socket.io/adapters/valkey/v3/adapter` |
| 发射器 | `github.com/aqcool/socket.io/adapters/valkey/v3/emitter` |

**`go.mod` 示例：**

```go
require (
    github.com/aqcool/socket.io/adapters/valkey/v3 v3.x.y
)
```

**用法：**

```go
import (
    "context"
    vk "github.com/valkey-io/valkey-go"
    valkey "github.com/aqcool/socket.io/adapters/valkey/v3"
    vkadapter "github.com/aqcool/socket.io/adapters/valkey/v3/adapter"
)

client, _ := vk.NewClient(vk.ClientOption{InitAddress: []string{"localhost:6379"}})
valkeyClient := valkey.NewValkeyClient(context.Background(), client)
server.SetAdapter(&vkadapter.ValkeyAdapterBuilder{Valkey: valkeyClient})
```

**读写分离**（生产环境推荐）：

```go
pubClient, _ := vk.NewClient(vk.ClientOption{InitAddress: []string{"master:6379"}})
subClient, _ := vk.NewClient(vk.ClientOption{InitAddress: []string{"replica:6380"}})
valkeyClient := valkey.NewValkeyClientWithSub(context.Background(), pubClient, subClient)
server.SetAdapter(&vkadapter.ValkeyAdapterBuilder{Valkey: valkeyClient})
```

### Engine.IO 客户端

| v1 导入 | v3 导入 |
|-----------|-----------|
| `github.com/zishang520/engine.io-client-go/engine` | `github.com/aqcool/socket.io/clients/engine/v3` |
| `github.com/zishang520/engine.io-client-go/request` | `github.com/aqcool/socket.io/v3/pkg/request` |
| `github.com/zishang520/engine.io-client-go/transports` | `github.com/aqcool/socket.io/clients/engine/v3/transports` |

### Socket.IO 客户端

| v1 导入 | v3 导入 |
|-----------|-----------|
| `github.com/zishang520/socket.io-client-go/socket` | `github.com/aqcool/socket.io/clients/socket/v3` |
| `github.com/zishang520/socket.io-client-go/utils` | `github.com/aqcool/socket.io/v3/pkg/utils` |

### 错误类型（v3 新增）

| 旧导入 | v3 导入 |
|------------|-----------|
| `clients/socket.ExtendedError` | `github.com/aqcool/socket.io/v3/pkg/types.ExtendedError` |
| `servers/socket.ExtendedError` | `github.com/aqcool/socket.io/v3/pkg/types.ExtendedError` |

> **提示：** 使用 `grep -r "github.com/zishang520" .` 查找所有旧导入，再通过查找替换统一更新。

---

## 破坏性改动

### 协议兼容性

Socket.IO v3 对齐 Socket.IO v4+ 协议。请确保客户端 Socket.IO 库已升级到 4.x 或更高版本。

```bash
npm install socket.io-client@^4.0.0
```

### Redis 适配器类型更新

如果使用 Redis 适配器，必须将所有 `types.String` 替换为 `types.Atomic[string]`：

```go
// Before
import "github.com/zishang520/socket.io-go-redis/types"

func example() {
    var roomName types.String
    roomName.Store("lobby")
    value := roomName.Load()
}

// After
import "github.com/aqcool/socket.io/v3/pkg/types"

func example() {
    var roomName types.Atomic[string]
    roomName.Store("lobby")
    value := roomName.Load()
}
```

### Socket 握手访问方式

更新访问握手请求头和查询参数的代码：

```go
// Before
func handleConnection(socket *socket.Socket) {
    headers := socket.Handshake().Headers
    userAgent := headers["user-agent"][0]

    query := socket.Handshake().Query
    token := query["token"][0]
}

// After
func handleConnection(socket *socket.Socket) {
    headers := socket.Handshake().Headers.Header()
    userAgent := headers.Get("User-Agent")

    query := socket.Handshake().Query.Query()
    token := query.Get("token")
}
```

### 配置方法返回值

更新使用 `GetRaw*` 配置方法的代码：

```go
// Before
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", *duration)
    }
}

// After
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", duration.Get())
    }
}
```

### ParameterBag 包迁移

将 `*utils.ParameterBag` 更新为 `*types.ParameterBag`：

```go
// Before
import "github.com/aqcool/socket.io/v3/pkg/utils"
var bag *utils.ParameterBag = utils.NewParameterBag(nil)

// After
import "github.com/aqcool/socket.io/v3/pkg/types"
var bag *types.ParameterBag = types.NewParameterBag(nil)
```

### 传输升级方法

传输升级方法现在返回 `[]string`，不再返回 `*types.Set[string]`：

```go
// Before
upgrades := transport.Upgrades() // *types.Set[string]

// After
upgrades := transport.Upgrades() // []string
```

### HttpContext API 迁移

| 之前 | 之后 |
|--------|-------|
| `ctx.ResponseHeaders` | `ctx.ResponseHeaders()` |
| `ctx.GetHost()` | `ctx.Host()` |
| `ctx.GetMethod()` | `ctx.Method()` |
| `ctx.Gets("key")` | `ctx.Query().Gets("key")` |
| `ctx.Get("key")` | `ctx.Query().Get("key")` |
| `ctx.GetPathInfo()` | `ctx.PathInfo()` |

### 工具函数迁移

| 之前 | 之后 |
|--------|-------|
| `adapter.SliceMap(...)` | `slices.Map(...)` |
| `adapter.Tap(...)` | `utils.Tap(...)` |

### ExtendedError API 迁移

服务端的 `Data()` 方法现已改为字段：

```go
// Before
data := err.Data()

// After  
data := err.Data
```

### Redis SubscriptionMode 迁移

如果使用 Redis 分片适配器，请更新 `SubscriptionMode` 导入：

```go
// Before
import "github.com/aqcool/socket.io/adapters/redis/v3/adapter"

opts.SetSubscriptionMode(adapter.DynamicSubscriptionMode)

// After
import "github.com/aqcool/socket.io/adapters/redis/v3"

opts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
```

---

## 快速开始示例

以下是升级到 v3 后的最小服务端示例：

```go
package main

import (
	"fmt"
	"net/http"

	server "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
	io := server.NewServer(nil, nil)

	io.On("connection", func(args ...any) {
		socket := args[0].(*server.Socket)
		fmt.Printf("connected: %s\n", socket.Id())

		socket.On("message", func(args ...any) {
			fmt.Printf("received: %v\n", args)
			socket.Emit("message", args...)
		})

		socket.On("disconnect", func(args ...any) {
			fmt.Printf("disconnected: %s\n", socket.Id())
		})
	})

	http.Handle("/socket.io/", io.ServeHandler(nil))
	fmt.Println("server listening on :3000")
	http.ListenAndServe(":3000", nil)
}
```

---

## 测试升级结果

完成升级后，请全面测试应用：

### 1. 运行测试套件

```bash
go test ./...
```

### 2. 测试核心功能

- 客户端连接和断开
- 事件发送和接收
- 命名空间与房间操作
- Redis 适配器广播（如适用）

### 3. 启用调试日志

设置 `DEBUG` 环境变量以启用详细日志：

```bash
# Linux / macOS
DEBUG=socket.io:* go run main.go

# Windows PowerShell
$env:DEBUG="socket.io:*"; go run main.go
```

### 4. 验证客户端兼容性

确保前端使用 Socket.IO 客户端 v4.x 或更高版本。

```bash
npm install socket.io-client@^4.0.0
```

### 5. 运行基准测试（可选）

`examples/benchmark` 模块提供内置基准测试，可用于验证性能：

```bash
cd examples/benchmark
go run main.go
```

---

## 常见问题

### 导入解析错误

```bash
go mod tidy
go clean -modcache
go mod download
```

### 连接协议不匹配

```bash
npm install socket.io-client@^4.0.0
```

### ExtendedError API 变更

如果调用 `ExtendedError` 的 `Data()` 方法时报错：

```go
// Before (server-side)
data := err.Data()

// After
data := err.Data
```

---

## 获取帮助

- [GitHub Issues](https://github.com/aqcool/socket.io/issues)——用于报告已确认的缺陷或提出功能请求
- [GitHub Discussions](https://github.com/aqcool/socket.io/discussions/new?category=q-a)——用于一般问题与使用帮助
- [Go 包文档](https://pkg.go.dev/github.com/aqcool/socket.io/v3)——API 参考
- [Socket.IO 协议文档](https://socket.io/docs/v4/)——协议规范
- [Socket.IO Go 仓库](https://github.com/aqcool/socket.io)——源代码和示例

---

## 发布说明

### v3.0.0

> 发布于 2026-04-13

这是 Socket.IO Go v3 的**首个稳定版本**，包含 alpha、beta 和 RC 阶段的全部改动。

#### 相比 v2 的主要变化

- **单体仓库整合**：将 6 个独立仓库合并为一个单体仓库和 9 个带版本的 Go 子模块
- **统一版本**：所有模块共享 `pkg/version/version.go` 中的单一版本源
- **最低 Go 1.26.0**：使用新版 Go 特性
- **协议对齐**：兼容 Socket.IO v4+ JavaScript 客户端
- **线程安全改造**：原子 Socket 标志（写时复制）、互斥保护中间件、使用 `sync.OnceValue` 延迟初始化，并通过 `runtime.SetFinalizer` 防止 goroutine 泄漏
- **类型安全改进**：泛型 `types.Atomic[T]`、保证空值安全的 `types.Optional[T]`，以及强类型 `Handshake` 字段
- **新增包**：`pkg/slices`（安全切片操作）、`pkg/queue`（保证消息顺序的串行任务队列）、`pkg/request`（HTTP 客户端）
- **Redis Cluster 支持**：分片广播、CROSSSLOT 错误修复、动态频道订阅及会话恢复分页
- **安全加固**：Polling 请求体大小限制（DoS 防护）、可配置附件数量上限（默认 10）及不可变数据包编码
- **代码质量**：集成 golangci-lint、修复 `errcheck` 问题、以具名常量替代魔法数字并统一调试日志

#### 迁移

从 v1/v2 迁移的完整说明请参阅[从 v1/v2 升级到 v3](#从-v1v2-升级到-v3)。

#### 完整变更日志

各版本的详细改动请参阅下方 RC、beta 和 alpha 发布说明。

---

### v3.0.0-rc.14

> 基于提交 [`cc50fc2`](https://github.com/zishang520/socket.io/commit/cc50fc2) 发布

#### 破坏性改动与行为更新

<details>
<summary>解析器：从公开 API 移除 ERROR_PACKET</summary>

**影响概率：低（仅限直接引用 ERROR_PACKET 的情况）**

为防止数据竞争，公开 API 已移除可变的共享 `ERROR_PACKET` 单例，改用内部 `newErrorPacket()` 工厂函数，每次创建新实例，避免 goroutine 之间共享可变状态。

```go
// Before (no longer works)
import "github.com/aqcool/socket.io/parsers/engine/v3/parser"
var errPkt = parser.ERROR_PACKET

// After (use alternatives)
// If you need error packet creation, use the public parser APIs
// that internally create error packets as needed
```

**影响：** `ERROR_PACKET` 原本属于内部常量，多数应用不受影响。如果曾直接使用它，请改用公开的解析器 API。
</details>

<details>
<summary>Socket 数据包编码器：Encode() 不再修改输入</summary>

**影响概率：低**

Socket.IO 数据包编码器的 `Encode()` 方法现在会先复制数据包再修改，避免对调用方的数据包对象产生意外副作用。

```go
// Before - Encode() modified the input packet's Type field
import "github.com/aqcool/socket.io/parsers/socket/v3/parser"

pkt := &packet.Packet{Type: parser.EVENT, Data: binaryData}
encoded := encoder.Encode(pkt)
// pkt.Type would now be BINARY_EVENT (mutated!)

// After - Input packet is not modified
pkt := &packet.Packet{Type: parser.EVENT, Data: binaryData}
encoded := encoder.Encode(pkt)
// pkt.Type remains EVENT (not mutated)
```

**影响：** 此行为修复使代码更可预测。如果代码依赖 `Encode()` 修改输入数据包的副作用，需要改为以不可变方式处理数据包。
</details>

<details>
<summary>Socket.IO 解析器：可配置的附件数量上限</summary>

**影响概率：低**

附件上限由硬编码的 1000 调整为每个解码器实例可配置、默认 10 个（与上游 Node.js 实现一致），现通过 `DecoderOptions` 控制，不再使用包级常量。

```go
import "github.com/aqcool/socket.io/parsers/socket/v3/parser"

// Default - limited to 10 attachments per packet
decoder := parser.NewDecoder()

// Custom limit for applications that need more attachments
decoder := parser.NewDecoder(&parser.DecoderOptions{
    MaxAttachments: 50,
})
```

超过上限的数据包会以 `parser.ErrTooManyAttachments` 错误拒绝。

**影响：** 单个数据包发送超过 10 个附件时将被拒绝。遇到此错误时，请将大型载荷拆分为多个数据包，或配置更高上限。
</details>

<details>
<summary>Engine.IO Polling：HTTP 请求体大小限制</summary>

**影响概率：中（仅限通过 Polling 发送超大载荷的情况）**

Polling 传输现在会在读取请求体时强制执行 `MaxHttpBufferSize` 限制，以防止内存无限增长（DoS 防护）。

```go
// Before - No limit on body size
// Large payloads could cause excessive memory usage

// After - Limited by MaxHttpBufferSize (default 1 MB)
// Large payloads exceeding the limit are truncated/rejected
```

**影响：** 通过 Polling 发送大于 `MaxHttpBufferSize`（默认 1 MB）的载荷时，数据将被截断或拒绝。大型消息请使用 WebSocket/WebTransport，或提高限制：

```go
import "github.com/aqcool/socket.io/servers/engine/v3/config"

opts := config.DefaultServerOptions()
opts.SetMaxHttpBufferSize(10 * 1024 * 1024) // 10 MB
```
</details>

#### 缺陷修复

<details>
<summary>WebSocket/WebTransport：发送循环行为</summary>

**影响概率：非常低**

修复发送循环提前返回的问题。此前成功发送一个编码帧后，队列中的剩余数据包会被丢弃。

```go
// Before - Send loop would return after first packet, dropping queue
// Packet 1: sent
// Packet 2, 3, ...: dropped (never sent)

// After - Send loop continues processing all queued packets
// All packets in queue are sent correctly
```

**影响：** 此修复提升了可靠性。此前只会发送队列中的第一个数据包，现在所有数据包都会按预期发送，无需修改代码。
</details>

<details>
<summary>中间件线程安全</summary>

**影响概率：非常低（仅限运行时修改中间件）**

Engine.IO 基础服务端现在使用 `sync.RWMutex` 保护中间件切片，确保并发读写安全。

```go
// Before - Unsafe concurrent middleware modification
go server.Use(middleware1) // Racing writes
go server.Use(middleware2) // Could panic or miss middleware

// After - Thread-safe middleware operations
go server.Use(middleware1) // Safe
go server.Use(middleware2) // Safe
```

**影响：** 线程安全修复，无需修改代码。
</details>

<details>
<summary>Socket 标志：并发修改安全</summary>

**影响概率：非常低**

Socket 标志（Compress、Volatile、Timeout）现在使用带写时复制的 `atomic.Pointer`，以防止竞态。

```go
// Before - Racing flag mutations could cause data races
go socket.Compress(true)
go socket.Volatile()
// Data race condition possible

// After - All flag mutations are thread-safe
go socket.Compress(true)
go socket.Volatile()
// Safe concurrent mutations
```

**影响：** 线程安全修复，无需修改代码。
</details>

<details>
<summary>队列：防止 goroutine 泄漏</summary>

**影响概率：非常低**

任务队列现在使用 `runtime.SetFinalizer()`，防止队列实例被垃圾回收时发生 goroutine 泄漏。

**影响：** 资源泄漏修复。使用长生命周期队列的应用可能会观察到 goroutine 数量下降，无需修改代码。
</details>

<details>
<summary>消息顺序与 OOM 防护</summary>

**影响概率：非常低**

解决 [#116](https://github.com/zishang520/socket.io/issues/116)。新增串行任务队列（`pkg/queue`），可保持消息顺序并防止高并发下发生 OOM。客户端和服务端传输现在都使用此队列发送数据。

**影响：** 可靠性修复，无需修改代码。
</details>

#### 内部改进

- 所有包统一使用 `pkg/log` 输出调试日志
- 整个代码库中的魔法数字替换为具名常量
- 提取客户端常量并修复网络监控泄漏
- 最低 Go 版本更新为 1.26.0

---

### v3.0.0-rc.13

> 基于提交 [`5b988b6`](https://github.com/zishang520/socket.io/commit/5b988b6) 发布

#### 主要变化

- **要求 Go 1.26.0**：最低 Go 版本提升至 1.26.0
- **集成 golangci-lint**：通过 `Makefiles` 将代码检查集成到构建系统
- **改进错误处理**：修复整个代码库中的 `errcheck` 问题，以正确处理或显式 `io.Closer` 模式替代忽略错误

#### Redis 适配器改进

- 增强轮询机制，并为会话恢复添加分页
- 改进 Redis 分片适配器的动态频道订阅管理
- 添加 `MessageType` 校验并改进错误处理

#### 缺陷修复

- 修复 Engine.IO 中竞态导致的空指针解引用（`76a0015`）
- 为 `Buffer` 添加带整数溢出保护的 `Peek` 方法（`ef32276`、`5d3ea31`）

---

### v3.0.0-rc.12

> 基于提交 [`e854211`](https://github.com/zishang520/socket.io/commit/e854211) 发布

#### 主要变化

<details>
<summary>ExtendedError 类型整合</summary>

**影响概率：中**

`clients/socket` 和 `servers/socket` 中各自的 `ExtendedError` 已整合为 `pkg/types` 中的共享实现，消除了重复代码并统一错误类型。

```go
// Before (client-side)
import "github.com/zishang520/socket.io-client-go/socket"

err := socket.NewExtendedError("connection failed", nil)

// Before (server-side)
import "github.com/zishang520/socket.io/v2/socket"

err := socket.NewExtendedError("middleware error", map[string]any{"code": 401})
data := err.Data()  // Note: server-side had Data() method

// After (unified)
import "github.com/aqcool/socket.io/v3/pkg/types"

err := types.NewExtendedError("error message", map[string]any{"code": 401})
data := err.Data  // Now uses direct field access
```

**主要变更：**

- `clients/socket.ExtendedError` → `types.ExtendedError`
- `servers/socket.ExtendedError` → `types.ExtendedError`（保留类型别名以向后兼容）
- 服务端的 `Data()` 方法改为 `Data` 字段
- 客户端和服务端共享同一个 `ExtendedError` 实现

**注意：** 服务端 `socket` 包保留了 `ExtendedError` 类型别名和 `NewExtendedError` 包装函数以向后兼容，因此现有服务端代码可能无需修改；客户端代码必须更新导入。
</details>

<details>
<summary>Redis SubscriptionMode 类型迁移</summary>

**影响概率：中（使用 Redis 分片适配器时）**

`SubscriptionMode` 已从 `adapters/redis/adapter` 迁移到根 `adapters/redis` 包，以便适配器和发射器共享。

```go
// Before
import "github.com/aqcool/socket.io/adapters/redis/v3/adapter"

opts := adapter.NewShardedRedisAdapterOptions()
opts.SetSubscriptionMode(adapter.DynamicSubscriptionMode)

// After
import (
    "github.com/aqcool/socket.io/adapters/redis/v3"
    "github.com/aqcool/socket.io/adapters/redis/v3/adapter"
)

opts := adapter.NewShardedRedisAdapterOptions()
opts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
```

**主要变更：**

| 之前 | 之后 |
|--------|-------|
| `adapter.SubscriptionMode` | `redis.SubscriptionMode` |
| `adapter.StaticSubscriptionMode` | `redis.StaticSubscriptionMode` |
| `adapter.DynamicSubscriptionMode` | `redis.DynamicSubscriptionMode` |
| `adapter.DynamicPrivateSubscriptionMode` | `redis.DynamicPrivateSubscriptionMode` |

**新增内容：**

- `redis.DefaultSubscriptionMode`——默认模式常量
- `redis.PrivateRoomIdLength`——用于识别私有房间的长度常量
- `redis.ShouldUseDynamicChannel(mode, room)`——共享辅助函数

**发射器选项扩展：**

```go
emitterOpts := emitter.NewEmitterOptions()
emitterOpts.SetSharded(true)
emitterOpts.SetSubscriptionMode(redis.DynamicSubscriptionMode)
```
</details>

#### Redis 适配器改进

- 添加分片广播操作器以支持 Redis Cluster（`d83b4db`）
- 修复从空房间获取 Socket 时的超时问题（`d5cfa20`）
- 为每个频道管理独立 PubSub 客户端，修复 Redis Cluster CROSSSLOT 错误（`2629cc1`）
- 改进二进制数据包处理和代码组织

---

### v3.0.0-rc.8

> 基于提交 [`b2f5457`](https://github.com/zishang520/socket.io/commit/b2f5457) 发布

#### 主要变化

<details>
<summary>适配器工具函数重组</summary>

**影响概率：中**

工具函数 `SliceMap` 和 `Tap` 已从 `adapter` 包迁移到专用的 `pkg` 子包。

```go
// Before
import "github.com/aqcool/socket.io/adapters/adapter/v3"

func example() {
    adapter.SliceMap(/**/)
    adapter.Tap(/**/)
}

// After
import (
    "github.com/aqcool/socket.io/v3/pkg/slices"
    "github.com/aqcool/socket.io/v3/pkg/utils"
)

func example() {
    slices.Map(/**/)
    utils.Tap(/**/)
}
```

**变更摘要：**

- `adapter.SliceMap` → `slices.Map`（迁移至 `pkg/slices`）
- `adapter.Tap` → `utils.Tap`（迁移至 `pkg/utils`）

**`pkg/slices` 中的新增函数：**

新的 `pkg/slices` 包提供以下工具函数：

| 函数 | 说明 |
|----------|-------------|
| `Get(s, idx)` | 检查边界后安全获取元素 |
| `GetAny[O](vals, idx)` | 从 `[]any` 获取元素并进行类型断言 |
| `TryGet(s, idx)` | 越界时返回零值 |
| `TryGetAny[O](vals, idx)` | 从 `[]any` 进行类型断言，失败时返回零值 |
| `GetWithDefault(s, idx, def)` | 越界时返回默认值 |
| `GetPtr(s, idx)` | 返回元素指针，越界时返回 nil |
| `Slice(s, start)` | 检查边界后安全截取子切片 |
| `First(s)` / `Last(s)` | 安全获取首个或末尾元素 |
| `Filter(s, predicate)` | 按条件筛选元素 |
| `Map(vals, transform)` | 转换每个元素 |
| `Reduce(vals, initial, reducer)` | 将元素归约为单个值 |
| `IsEmpty(s)` | 检查切片是否为 nil 或空 |
| `IsValidIndex(s, idx)` | 检查索引是否有效 |
</details>

<details>
<summary>HttpContext API 重构</summary>

**影响概率：中**

`*types.HttpContext` 的部分方法和属性已重命名，或由属性重构为方法。所有延迟加载方法现在使用 `sync.OnceValue` 保证线程安全。

```go
// Before
func example(ctx *types.HttpContext) {
    headers := ctx.ResponseHeaders
    host := ctx.GetHost()
    method := ctx.GetMethod()
    values := ctx.Gets("foo")
    value := ctx.Get("bar")
    path := ctx.GetPathInfo()
}

// After
func example(ctx *types.HttpContext) {
    headers := ctx.ResponseHeaders()
    host := ctx.Host()
    method := ctx.Method()
    values, _ := ctx.Query().Gets("foo")
    value, _ := ctx.Query().Get("bar")
    path := ctx.PathInfo()
}
```

**变更摘要：**

- `ResponseHeaders` → `ResponseHeaders()`（属性改为方法）
- `GetHost()` → `Host()`
- `GetMethod()` → `Method()`
- `Gets(key)` → `Query().Gets(key)`
- `Get(key)` → `Query().Get(key)`
- `GetPathInfo()` → `PathInfo()`

**新增或更新的方法：**

| 方法 | 说明 |
|--------|-------------|
| `Path()` | 返回去除首尾斜杠的规范化路径 |
| `UserAgent()` | 返回 User-Agent 请求头值 |
| `Secure()` | TLS 连接时返回 `true` |
| `SetStatusCode(code)` | 现在返回用于校验的 `error` |
| `IsDone()` | 检查响应是否已写入 |
| `Done()` | 返回 `<-chan struct{}`，不再返回 `<-chan Void` |
</details>

<details>
<summary>ParameterBag 包迁移</summary>

**影响概率：中**

`ParameterBag` 已从 `utils` 包迁移到 `types` 包。

```go
// Before
import "github.com/aqcool/socket.io/v3/pkg/utils"

func example() {
    var bag *utils.ParameterBag
    bag = utils.NewParameterBag(nil)
}

// After
import "github.com/aqcool/socket.io/v3/pkg/types"

func example() {
    var bag *types.ParameterBag
    bag = types.NewParameterBag(nil)
}
```
</details>

---

### v3.0.0-rc.4

> 基于提交 [`d7c93b5`](https://github.com/zishang520/socket.io/commit/d7c93b5) 发布

#### 主要变化

<details>
<summary>Socket 握手类型更新</summary>

**影响概率：中**

`socket.Handshake` 结构现在使用更明确的强类型字段：

```go
// Before
type Handshake struct {
    Headers map[string][]string
    Query   map[string][]string
    Auth    any
}

// After
type Handshake struct {
    Headers types.IncomingHttpHeaders  // provides Header() method
    Query   types.ParsedUrlQuery       // provides Query() method
    Auth    map[string]any
}
```

必须更新访问方式：

```go
// Before
headers := socket.Handshake().Headers
userAgent := headers["user-agent"][0]

// After
headers := socket.Handshake().Headers.Header()
userAgent := headers.Get("User-Agent")
```
</details>

<details>
<summary>Auth 参数标准化</summary>

**影响概率：中**

`Handshake` 中的 `Auth` 字段已由 `any` 统一为 `map[string]any`，为身份认证数据提供一致类型。

```go
// Before
auth := socket.Handshake().Auth // type: any
if authMap, ok := auth.(map[string]any); ok {
    token := authMap["token"]
}

// After
auth := socket.Handshake().Auth // type: map[string]any
token := auth["token"]
```
</details>

<details>
<summary>Optional[T] 增强</summary>

**影响概率：低**

`Optional[T]` 接口新增 `IsPresent()` 和 `IsEmpty()` 方法，`Some.Get()` 也可安全处理 nil 接收者。

```go
if duration := config.GetRawMaxDisconnectionDuration(); duration != nil && duration.IsPresent() {
    fmt.Printf("Duration: %d", duration.Get())
}
```
</details>

---

### v3.0.0-rc.2

> 基于提交 [`540c239`](https://github.com/zishang520/socket.io/commit/540c239) 发布

#### 缺陷修复

- 修复客户端向 Socket.IO 解析器发送 nil 载荷时的 panic（`80fe0b9`）

#### 内部改动

- 使用直接属性访问替代 `GetRaw*` 方法调用，提高可读性（`ce8f623`）

---

### v3.0.0-beta.1

> 基于提交 [`01f5eca`](https://github.com/zishang520/socket.io/commit/01f5eca) 发布

#### 主要变化

<details>
<summary>配置项 GetRaw* 方法变更</summary>

**影响概率：中**

所有 `GetRaw*` 方法现在返回 `types.Optional[T]` 而非指针类型，以提升空值安全性：

```go
// Before
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", *duration)
    }
}

// After
func configExample(config ConnectionStateRecoveryInterface) {
    if duration := config.GetRawMaxDisconnectionDuration(); duration != nil {
        fmt.Printf("Duration: %d", duration.Get())
    }
}
```
</details>

#### 缺陷修复

- 修复 `HTTPClient.Close()` 中的 HTTP/2 连接 goroutine 泄漏（`069619b`）
- 合入上游方案，修复定时器 goroutine 泄漏（`ff5d935`）

---

### v3.0.0-alpha.0 ~ alpha.4

> 涵盖 v3 初始重构的 Alpha 版本

#### 主要变化

- **依赖整合**：将原先独立的多个仓库合并为一个包含带版本子模块的单体仓库
- **导入路径重构**：所有包导入路径更新为新的 `github.com/aqcool/socket.io/` 命名空间（参阅[导入路径更新](#导入路径更新)）
- **类型安全的原子类型**：使用泛型 `types.Atomic[T]` 替代 `atomic.Value`（`7389549`）
- **Redis 适配器类型更新**：使用 `types.Atomic[string]` 替换 `types.String`
- **服务端选项重构**：整合服务端选项接口和结构，提高可读性（`a396fef`）
- **传输升级方法**：改为返回 `[]string`，不再返回 `*types.Set[string]`（`f3c4cd8`）
- **版本管理**：新增带版本命令和各模块版本文件的 `cmd/socket.io` 模块

---

## 补充说明

| 建议 | 详情 |
|----------------|---------|
| **先行备份** | 升级前务必备份代码库 |
| **Go 版本** | 确保使用 Go 1.26.0 或更高版本 |
| **分阶段发布** | 建议先升级非关键组件 |
| **客户端协调** | 与前端团队协调 Socket.IO 客户端 v4.x+ 兼容工作 |
| **安全更新** | v3.0.0 包含重要的 DoS 防护和数据竞争修复 |
| **Vendor 目录** | 如果使用 `go mod vendor`，请在更新依赖后再次运行该命令 |
| **IDE 支持** | 更新导入后重启 IDE 或语言服务器，以获得准确的代码补全 |
