package otel

import (
	"context"
	"testing"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	api "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestServerTracingAndLowCardinalityDefaults(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	server := socket.NewServer(nil, nil)
	instrumentation, err := InstrumentServer(server, &Options{
		TracerProvider: provider,
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instrumentation.Close)

	now := time.Now()
	connection := socket.TelemetryEvent{
		Kind: socket.TelemetryConnection, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", At: now, Success: true,
	}
	server.EmitReserved("telemetry", connection)
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryEventReceived, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Event: "message", Bytes: 20,
		Duration: time.Millisecond, At: now.Add(time.Millisecond), Success: true,
	})
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryAckCompleted, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Event: "message",
		Duration: 2 * time.Millisecond, At: now.Add(2 * time.Millisecond), Success: true,
	})
	server.Sockets().Adapter().Emit("adapter_operation", socket.AdapterTelemetryEvent{
		Operation: "broadcast", Namespace: "/", Duration: time.Millisecond, At: now, Success: true,
	})
	server.EmitReserved("telemetry", socket.TelemetryEvent{
		Kind: socket.TelemetryDisconnection, Namespace: "/chat", SocketID: "private-id",
		Transport: "websocket", Reason: "test", At: now.Add(time.Second), Success: true,
	})

	spans := recorder.Ended()
	names := map[string]bool{}
	for _, span := range spans {
		names[span.Name()] = true
		for _, attr := range span.Attributes() {
			if string(attr.Key) == "socket.io.socket_id" || attr.Value.AsString() == "private-id" {
				t.Fatalf("Socket ID leaked into span %q", span.Name())
			}
		}
	}
	for _, expected := range []string{
		"socket.io.connection", "socket.io.event.receive",
		"socket.io.ack", "socket.io.adapter.broadcast",
	} {
		if !names[expected] {
			t.Errorf("missing span %q", expected)
		}
	}
}

func TestTraceMetadataPropagation(t *testing.T) {
	propagator := propagation.TraceContext{}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	metadata := InjectTraceMetadata(ctx, nil, propagator)
	if metadata["traceparent"] == nil {
		t.Fatal("traceparent was not injected")
	}
	extracted := ExtractTraceMetadata(context.Background(), metadata, propagator)
	if got := trace.SpanContextFromContext(extracted); got.TraceID() != spanContext.TraceID() {
		t.Fatalf("trace ID = %s, want %s", got.TraceID(), spanContext.TraceID())
	}
}

func TestGlobalDefaultsAreAccepted(t *testing.T) {
	api.SetTextMapPropagator(propagation.TraceContext{})
	server := socket.NewServer(nil, nil)
	instrumentation, err := InstrumentServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	instrumentation.Close()
}
