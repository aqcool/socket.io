# 可插拔网络组件

Engine.IO Go 客户端允许每条连接独立注入 HTTP、WebSocket、WebTransport、代理和观测组件。未设置时继续使用项目原有默认实现。

## HTTP polling

```go
transport := &http.Transport{
    Proxy: http.ProxyFromEnvironment,
    TLSClientConfig: tlsConfig,
}
client := &http.Client{
    Transport: transport,
    Timeout:   15 * time.Second,
}

options.SetHTTPClient(client)
```

也可以只注入 `http.RoundTripper`：

```go
options.SetRoundTripper(otelhttp.NewTransport(http.DefaultTransport))
```

同时设置时，显式 `RoundTripper` 会覆盖 `http.Client.Transport`。

## WebSocket 和 WebTransport Dialer

```go
options.SetWebSocketDialer(func(
    ctx context.Context,
    url string,
    headers http.Header,
) (*websocket.Conn, *http.Response, error) {
    return customDialer.DialContext(ctx, url, headers)
})

options.SetWebTransportDialer(func(
    ctx context.Context,
    url string,
    headers http.Header,
) (*http.Response, *webtransport.Session, error) {
    return customWebTransportDialer.Dial(ctx, url, headers)
})
```

## 每连接代理与统一观测

```go
proxy, _ := url.Parse("http://127.0.0.1:8080")
options.SetProxyURL(proxy)

options.SetNetworkObserver(func(event engine.NetworkEvent) {
    // event.Transport: polling/websocket/webtransport
    // event.Operation: GET/POST/dial
    // event.Kind: dial/tls/proxy/transport
    // event.Duration、Success、Err 可直接接入 metrics/tracing
})
```

代理 URL 只属于当前 Socket options，也可以通过 transport-specific options 为不同 transport 设置不同代理。observer 中的代理地址使用 `url.URL.Redacted()`，不会暴露密码。

## 服务端 WebSocket engine

服务端默认使用 Gorilla WebSocket，但可替换 upgrader：

```go
type MyEngine struct{}

func (*MyEngine) Upgrade(
    writer http.ResponseWriter,
    request *http.Request,
    headers http.Header,
    options config.WebSocketUpgradeOptions,
) (types.WebSocketConnection, error) {
    return upgradeWithMyLibrary(writer, request, headers, options)
}

serverOptions.SetWebSocketEngine(&MyEngine{})
```

自定义连接需要实现 `types.WebSocketConnection`。接口只包含 Engine.IO 实际使用的读写、deadline、压缩、地址和关闭操作。
