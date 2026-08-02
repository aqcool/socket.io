// Package otel traces Socket.IO operations with OpenTelemetry.
package otel

import (
	"context"
	"fmt"
	"sync"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
	api "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type ContextExtractor func(*socket.TelemetryEvent) context.Context

type Options struct {
	TracerProvider   trace.TracerProvider
	Propagator       propagation.TextMapPropagator
	ContextExtractor ContextExtractor
	IncludeSocketID  bool
}

type connectionSpan struct {
	ctx  context.Context
	span trace.Span
}

type adapterListener struct {
	adapter  socket.Adapter
	listener types.EventListener
}

type Instrumentation struct {
	tracer               trace.Tracer
	propagator           propagation.TextMapPropagator
	contextExtractor     ContextExtractor
	includeSocketID      bool
	server               *socket.Server
	serverListener       types.EventListener
	newNamespaceListener types.EventListener
	mu                   sync.Mutex
	connections          map[string]connectionSpan
	adapters             map[string]adapterListener
	closeOnce            sync.Once
}

func InstrumentServer(server *socket.Server, options *Options) (*Instrumentation, error) {
	if server == nil {
		return nil, fmt.Errorf("instrument Socket.IO tracing: nil server")
	}
	opts := Options{
		TracerProvider: api.GetTracerProvider(),
		Propagator:     api.GetTextMapPropagator(),
	}
	if options != nil {
		opts = *options
		if opts.TracerProvider == nil {
			opts.TracerProvider = api.GetTracerProvider()
		}
		if opts.Propagator == nil {
			opts.Propagator = api.GetTextMapPropagator()
		}
	}
	instrumentation := &Instrumentation{
		tracer:           opts.TracerProvider.Tracer("github.com/aqcool/socket.io/observability/v3"),
		propagator:       opts.Propagator,
		contextExtractor: opts.ContextExtractor,
		includeSocketID:  opts.IncludeSocketID,
		server:           server,
		connections:      make(map[string]connectionSpan),
		adapters:         make(map[string]adapterListener),
	}
	instrumentation.serverListener = instrumentation.observe
	instrumentation.newNamespaceListener = instrumentation.onNewNamespace
	_ = server.On("telemetry", instrumentation.serverListener)
	_ = server.Sockets().On("new_namespace", instrumentation.newNamespaceListener)
	for _, namespace := range server.Namespaces() {
		instrumentation.attachAdapter(namespace)
	}
	return instrumentation, nil
}

func (i *Instrumentation) observe(args ...any) {
	if len(args) == 0 {
		return
	}
	event, ok := args[0].(socket.TelemetryEvent)
	if !ok {
		return
	}
	key := event.Namespace + "\x00" + string(event.SocketID)
	attributes := []attribute.KeyValue{
		attribute.String("socket.io.namespace", event.Namespace),
		attribute.String("network.transport", event.Transport),
	}
	if i.includeSocketID {
		attributes = append(attributes, attribute.String("socket.io.socket_id", string(event.SocketID)))
	}
	switch event.Kind {
	case socket.TelemetryConnection:
		ctx := i.extractContext(&event)
		ctx, span := i.tracer.Start(ctx, "socket.io.connection",
			trace.WithTimestamp(event.At),
			trace.WithAttributes(append(attributes,
				attribute.Bool("socket.io.recovered", event.Recovered),
				attribute.Bool("socket.io.recovery_attempted", event.RecoveryAttempted),
			)...),
		)
		i.mu.Lock()
		i.connections[key] = connectionSpan{ctx: ctx, span: span}
		i.mu.Unlock()
	case socket.TelemetryDisconnection:
		i.mu.Lock()
		connection, exists := i.connections[key]
		delete(i.connections, key)
		i.mu.Unlock()
		if exists {
			connection.span.SetAttributes(attribute.String("socket.io.disconnect_reason", event.Reason))
			connection.span.End(trace.WithTimestamp(event.At))
		}
	case socket.TelemetryEventReceived, socket.TelemetryEventSent:
		direction := "receive"
		if event.Kind == socket.TelemetryEventSent {
			direction = "send"
		}
		ctx := i.parentContext(key, &event)
		startedAt := event.At.Add(-event.Duration)
		_, span := i.tracer.Start(ctx, "socket.io.event."+direction,
			trace.WithTimestamp(startedAt),
			trace.WithAttributes(append(attributes,
				attribute.String("socket.io.event", event.Event),
				attribute.String("socket.io.direction", direction),
				attribute.Int("socket.io.bytes", event.Bytes),
			)...),
		)
		if !event.Success {
			span.SetStatus(codes.Error, "event processing failed")
		}
		span.End(trace.WithTimestamp(event.At))
	case socket.TelemetryAckCompleted, socket.TelemetryAckTimeout:
		ctx := i.parentContext(key, &event)
		startedAt := event.At.Add(-event.Duration)
		_, span := i.tracer.Start(ctx, "socket.io.ack",
			trace.WithTimestamp(startedAt),
			trace.WithAttributes(append(attributes,
				attribute.String("socket.io.event", event.Event),
				attribute.Bool("socket.io.timeout", event.Kind == socket.TelemetryAckTimeout),
			)...),
		)
		if event.Kind == socket.TelemetryAckTimeout {
			span.SetStatus(codes.Error, "acknowledgement timeout")
		}
		span.End(trace.WithTimestamp(event.At))
	}
}

func (i *Instrumentation) parentContext(key string, event *socket.TelemetryEvent) context.Context {
	if len(event.TraceMetadata) > 0 || i.contextExtractor != nil {
		return i.extractContext(event)
	}
	i.mu.Lock()
	connection, exists := i.connections[key]
	i.mu.Unlock()
	if exists {
		return connection.ctx
	}
	return context.Background()
}

func (i *Instrumentation) extractContext(event *socket.TelemetryEvent) context.Context {
	ctx := context.Background()
	if i.contextExtractor != nil {
		if extracted := i.contextExtractor(event); extracted != nil {
			ctx = extracted
		}
	}
	if len(event.TraceMetadata) > 0 {
		ctx = i.propagator.Extract(ctx, propagation.MapCarrier(event.TraceMetadata))
	}
	return ctx
}

func (i *Instrumentation) onNewNamespace(args ...any) {
	if len(args) == 0 {
		return
	}
	if namespace, ok := args[0].(socket.Namespace); ok {
		i.attachAdapter(namespace)
	}
}

func (i *Instrumentation) attachAdapter(namespace socket.Namespace) {
	i.mu.Lock()
	if _, exists := i.adapters[namespace.Name()]; exists {
		i.mu.Unlock()
		return
	}
	listener := func(args ...any) {
		if len(args) == 0 {
			return
		}
		event, ok := args[0].(socket.AdapterTelemetryEvent)
		if !ok {
			return
		}
		_, span := i.tracer.Start(context.Background(), "socket.io.adapter."+event.Operation,
			trace.WithTimestamp(event.At.Add(-event.Duration)),
			trace.WithAttributes(
				attribute.String("socket.io.namespace", event.Namespace),
				attribute.String("socket.io.adapter.operation", event.Operation),
				attribute.Bool("socket.io.adapter.success", event.Success),
			),
		)
		if !event.Success {
			span.SetStatus(codes.Error, "adapter operation failed")
		}
		span.End(trace.WithTimestamp(event.At))
	}
	i.adapters[namespace.Name()] = adapterListener{adapter: namespace.Adapter(), listener: listener}
	i.mu.Unlock()
	_ = namespace.Adapter().On("adapter_operation", listener)
}

// InjectTraceMetadata injects traceparent/tracestate into event metadata.
func InjectTraceMetadata(
	ctx context.Context,
	metadata map[string]any,
	propagator propagation.TextMapPropagator,
) map[string]any {
	if metadata == nil {
		metadata = make(map[string]any)
	}
	if propagator == nil {
		propagator = api.GetTextMapPropagator()
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	for key, value := range carrier {
		metadata[key] = value
	}
	return metadata
}

// ExtractTraceMetadata extracts a propagated context from event metadata.
func ExtractTraceMetadata(
	ctx context.Context,
	metadata map[string]any,
	propagator propagation.TextMapPropagator,
) context.Context {
	if propagator == nil {
		propagator = api.GetTextMapPropagator()
	}
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		if value, ok := metadata[key].(string); ok {
			carrier[key] = value
		}
	}
	return propagator.Extract(ctx, carrier)
}

func (i *Instrumentation) Close() {
	i.closeOnce.Do(func() {
		i.server.RemoveListener("telemetry", i.serverListener)
		i.server.Sockets().EventEmitter().RemoveListener("new_namespace", i.newNamespaceListener)
		i.mu.Lock()
		connections := make([]connectionSpan, 0, len(i.connections))
		for _, connection := range i.connections {
			connections = append(connections, connection)
		}
		adapters := make([]adapterListener, 0, len(i.adapters))
		for _, listener := range i.adapters {
			adapters = append(adapters, listener)
		}
		i.connections = make(map[string]connectionSpan)
		i.mu.Unlock()
		for _, connection := range connections {
			connection.span.End()
		}
		for _, listener := range adapters {
			listener.adapter.RemoveListener("adapter_operation", listener.listener)
		}
	})
}
