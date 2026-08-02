# 服务端认证中间件

`servers/socket/auth` 包提供 JWT/JWK、API Key 和按用户主动断开连接的扩展。它们都是普通 Namespace middleware，可以用于根 Namespace、固定 Namespace 或动态 Namespace。

## JWT 与 JWK

JWT 中间件支持 `HS256/384/512`、`RS256/384/512` 和 `ES256/384/512`，并校验 `exp`、`nbf`、`iss` 和 `aud`。必须显式设置允许的算法，避免算法降级。

```go
import (
    socketauth "github.com/aqcool/socket.io/servers/socket/v3/auth"
)

keys, err := socketauth.NewRemoteJWKSet(socketauth.JWKSetOptions{
    URL: "https://login.example.com/.well-known/jwks.json",
})
if err != nil {
    panic(err)
}

middleware, err := socketauth.JWT(&socketauth.JWTOptions{
    Keys:       keys,
    Algorithms: []string{"RS256"},
    Issuer:     "https://login.example.com/",
    Audience:   "socket-api",
    OnVerified: func(s *socket.Socket, claims socketauth.Claims) error {
        s.SetData(map[string]any{"userId": claims["sub"]})
        return nil
    },
})
if err != nil {
    panic(err)
}
io.Use(middleware)
```

默认从握手的 `auth.token` 读取 Token，其次读取 `Authorization: Bearer ...`。远程 JWK Set 仅接受 HTTPS 地址，默认缓存 5 分钟，并在遇到未知 `kid` 时刷新。

对称密钥或已经由应用加载的公钥可以使用 `StaticKeySource`：

```go
middleware, err := socketauth.JWT(&socketauth.JWTOptions{
    Keys: socketauth.StaticKeySource{
        Keys: map[string]any{"2026-01": rsaPublicKey},
    },
    Algorithms: []string{"RS256"},
})
```

## API Key

```go
middleware, err := socketauth.APIKey(socketauth.APIKeyOptions{
    Validate: func(ctx context.Context, key string) (any, error) {
        return apiKeyStore.Lookup(ctx, key)
    },
    OnAuthenticated: func(s *socket.Socket, principal any) error {
        s.SetData(principal)
        return nil
    },
})
```

默认从 `auth.apiKey` 读取，其次读取 `X-API-Key`。可以通过 `Extractor` 替换提取规则。API Key 应只保存哈希值，并通过 HTTPS/WSS 传输。

## 禁用用户后主动断开

`DisconnectUser` 会遍历全部 Namespace，并使用 `FetchSockets` 获取本机和 Adapter 集群中的 Socket，再通过 `RemoteSocket.Disconnect` 执行断开：

```go
socketauth.DisconnectUser(
    io,
    disabledUserID,
    socketauth.MapUserID("userId"),
    true,
)(func(disconnected int, err error) {
    // 记录审计日志
})
```

应用在禁用用户、撤销账户或处理安全事件后调用此接口。若 `closeConnection` 为 `true`，同一底层 Engine.IO 连接上的其他 Namespace 也会关闭。
