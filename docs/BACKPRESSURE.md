# 客户端背压与缓冲区限制

每个 Namespace Socket 都可以独立限制断线发送缓冲、连接前接收缓冲和可靠投递 retry queue。未设置或设置为 `0` 时保持原有的无限制行为。

```go
options := socket.DefaultSocketOptions()
options.SetMaxSendBufferPackets(1_000)
options.SetMaxSendBufferBytes(8 << 20)
options.SetMaxReceiveBufferPackets(1_000)
options.SetMaxReceiveBufferBytes(8 << 20)
options.SetMaxRetryQueuePackets(500)
options.SetMaxRetryQueueBytes(4 << 20)
options.SetOverflowStrategy(socket.OverflowDropOldest)

orders := manager.Socket("/orders", options)
```

可用策略：

| 策略 | 行为 |
|------|------|
| `OverflowReject` | 拒绝新包，`Emit` 返回 `ErrBufferOverflow` |
| `OverflowDropNewest` | 丢弃新包；如果带 ACK，ACK 会收到 `ErrBufferOverflow` |
| `OverflowDropOldest` | 丢弃最旧的包，为新包腾出空间 |
| `OverflowDisconnect` | 触发慢消费者事件并停止当前 Socket |

运行时事件：

- `overflow`：参数为 `*OverflowDetails`，包含缓冲区、策略、新包字节数和当前积压。
- `slow_consumer`：使用断开策略时触发。
- `drain`：某个缓冲区被清空时触发。

`socket.BufferStats()` 返回三个缓冲区当前的包数和估算字节数。字节限制基于待序列化数据大小估算；它用于阻止无界增长，不等同于底层传输最终写出的压缩后字节数。
