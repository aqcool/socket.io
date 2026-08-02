# Prometheus 与 OpenTelemetry

`observability` 模块提供 Prometheus 指标和 OpenTelemetry tracing。核心服务端只发送轻量的结构化 telemetry 事件；未接入可观测性模块时不会创建指标或 span。

## Prometheus

```go
import (
    socketmetrics "github.com/aqcool/socket.io/observability/v3/prometheus"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

registry := prometheus.NewRegistry()
metrics, err := socketmetrics.New(&socketmetrics.Options{
    Registerer: registry,
    Prefix:     "socketio",
})
if err != nil {
    panic(err)
}
defer metrics.Close()

if err := metrics.InstrumentServer(io); err != nil {
    panic(err)
}

http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
```

若应用同时使用 Go Socket.IO 客户端，可以额外采集重连、队列和溢出指标：

```go
err := metrics.InstrumentClient(clientSocket)
```

主要指标：

- `socketio_connections`、`socketio_connections_total`、`socketio_disconnections_total`
- `socketio_events_total`、`socketio_event_bytes_total`、`socketio_event_duration_seconds`
- `socketio_acks_total`、`socketio_ack_duration_seconds`
- `socketio_connection_recoveries_total{result="new|recovered|failed"}`
- `socketio_adapter_operation_duration_seconds`
- `socketio_client_reconnects_total`
- `socketio_client_queue_packets`、`socketio_client_queue_bytes`
- `socketio_client_buffer_overflows_total`

连接速率、断开速率、ACK 超时率和恢复成功率应在 PromQL 中从 Counter 计算，例如：

```promql
rate(socketio_connections_total[5m])
rate(socketio_disconnections_total[5m])
sum(rate(socketio_acks_total{outcome="timeout"}[5m]))
  / sum(rate(socketio_acks_total[5m]))
sum(rate(socketio_connection_recoveries_total{result="recovered"}[5m]))
  / sum(rate(socketio_connection_recoveries_total{result=~"recovered|failed"}[5m]))
```

默认标签只有 Namespace、transport、方向、结果、队列和 Adapter 操作，不包含 Socket ID 或事件参数。

## OpenTelemetry tracing

```go
import socketotel "github.com/aqcool/socket.io/observability/v3/otel"

tracing, err := socketotel.InstrumentServer(io, &socketotel.Options{
    TracerProvider: tracerProvider,
    Propagator:     propagation.TraceContext{},
})
if err != nil {
    panic(err)
}
defer tracing.Close()
```

扩展会创建以下 span：

- `socket.io.connection`
- `socket.io.event.receive` 和 `socket.io.event.send`
- `socket.io.ack`
- `socket.io.adapter.broadcast`、`socket.io.adapter.publish` 和 `socket.io.adapter.response`

连接 span 会读取握手头中的 W3C `traceparent`/`tracestate`。事件元数据中包含同名字段时，事件和 ACK span 也会从中恢复上下文。

发送事件时可以注入上下文：

```go
metadata := socketotel.InjectTraceMetadata(ctx, map[string]any{
    "requestId": requestID,
}, propagation.TraceContext{})

socket.Emit("order:update", payload, metadata)
```

接收端也可以使用 `ExtractTraceMetadata`，或通过 `Options.ContextExtractor` 接入自定义 trace metadata。

默认 span 属性不包含 Socket ID。只有明确设置 `IncludeSocketID: true` 时才会加入该属性；生产环境通常不应启用。
