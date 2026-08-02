# 动态认证

Go Socket.IO 客户端支持为每个 Namespace 配置动态认证 Provider。Provider 会在首次连接和每次底层连接重建后调用，适合获取短期 Token。

```go
socketOptions := socket.DefaultSocketOptions()
socketOptions.SetAuthProvider(func(ctx context.Context) (map[string]any, error) {
	token, err := tokenService.Refresh(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"token": token}, nil
})

client := manager.Socket("/orders", socketOptions)
```

Provider 收到的 `context.Context` 会使用 Manager 的连接超时时间，并在 Socket 关闭或新的认证尝试开始时取消。Provider 返回错误会触发 `connect_error`，不会发送包含无效认证数据的 CONNECT 包。

服务端拒绝认证后，可以更新当前 Namespace 的静态认证数据或 Provider，再调用 `Connect` 重试：

```go
_ = client.On("connect_error", func(...any) {
	client.SetAuthProvider(refreshedTokenProvider)
	client.Connect()
})
```

`SetAuth` 和 `SetAuthProvider` 只修改当前 Socket 所属的 Namespace，不影响同一 Manager 下的其他 Namespace。连接状态恢复使用的 `pid` 和 `offset` 会写入 CONNECT 包副本，不会污染用户提供的认证 Map。
