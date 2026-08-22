// Package prometheus exports Socket.IO runtime metrics.
package prometheus

import (
	"fmt"
	"strconv"
	"sync"

	client "github.com/aqcool/socket.io/clients/socket/v4"
	socket "github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	prom "github.com/prometheus/client_golang/prometheus"
)

type Options struct {
	Registerer prom.Registerer
	Prefix     string
}

type socketLabels struct {
	namespace string
	transport string
}

type adapterListener struct {
	adapter  socket.Adapter
	listener types.EventListener
}

type Collector struct {
	registerer prom.Registerer
	collectors []prom.Collector

	connections        *prom.GaugeVec
	connectionsTotal   *prom.CounterVec
	disconnections     *prom.CounterVec
	events             *prom.CounterVec
	eventBytes         *prom.CounterVec
	eventDuration      *prom.HistogramVec
	acks               *prom.CounterVec
	ackDuration        *prom.HistogramVec
	recoveries         *prom.CounterVec
	adapterDuration    *prom.HistogramVec
	clientReconnects   *prom.CounterVec
	clientQueuePackets *prom.GaugeVec
	clientQueueBytes   *prom.GaugeVec
	clientOverflows    *prom.CounterVec

	mu                   sync.Mutex
	active               map[string]socketLabels
	server               *socket.Server
	serverListener       types.EventListener
	newNamespaceListener types.EventListener
	adapters             map[string]adapterListener
	clientCleanups       []func()
	closeOnce            sync.Once
}

func New(options *Options) (*Collector, error) {
	opts := Options{Registerer: prom.DefaultRegisterer, Prefix: "socketio"}
	if options != nil {
		opts = *options
		if opts.Registerer == nil {
			opts.Registerer = prom.DefaultRegisterer
		}
		if opts.Prefix == "" {
			opts.Prefix = "socketio"
		}
	}
	c := &Collector{
		registerer: opts.Registerer,
		active:     make(map[string]socketLabels),
		adapters:   make(map[string]adapterListener),
	}
	c.connections = prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: opts.Prefix, Name: "connections", Help: "Current Socket.IO connections.",
	}, []string{"namespace", "transport"})
	c.connectionsTotal = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "connections_total", Help: "Total Socket.IO connections.",
	}, []string{"namespace", "transport"})
	c.disconnections = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "disconnections_total", Help: "Total Socket.IO disconnections.",
	}, []string{"namespace", "transport"})
	c.events = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "events_total", Help: "Socket.IO events by direction.",
	}, []string{"namespace", "transport", "direction"})
	c.eventBytes = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "event_bytes_total", Help: "Approximate serialized event bytes.",
	}, []string{"namespace", "transport", "direction"})
	c.eventDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: opts.Prefix, Name: "event_duration_seconds", Help: "Event processing duration.",
	}, []string{"namespace", "direction"})
	c.acks = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "acks_total", Help: "Acknowledgements by outcome.",
	}, []string{"namespace", "outcome"})
	c.ackDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: opts.Prefix, Name: "ack_duration_seconds", Help: "Acknowledgement latency.",
	}, []string{"namespace", "outcome"})
	c.recoveries = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "connection_recoveries_total", Help: "Connection recovery attempts.",
	}, []string{"namespace", "result"})
	c.adapterDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: opts.Prefix, Name: "adapter_operation_duration_seconds", Help: "Adapter publish and response latency.",
	}, []string{"namespace", "operation", "success"})
	c.clientReconnects = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "client_reconnects_total", Help: "Client reconnect attempts and outcomes.",
	}, []string{"namespace", "outcome"})
	c.clientQueuePackets = prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: opts.Prefix, Name: "client_queue_packets", Help: "Packets buffered by a Socket.IO client.",
	}, []string{"namespace", "queue"})
	c.clientQueueBytes = prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: opts.Prefix, Name: "client_queue_bytes", Help: "Bytes buffered by a Socket.IO client.",
	}, []string{"namespace", "queue"})
	c.clientOverflows = prom.NewCounterVec(prom.CounterOpts{
		Namespace: opts.Prefix, Name: "client_buffer_overflows_total", Help: "Client buffer overflows.",
	}, []string{"namespace", "queue", "strategy"})
	c.collectors = []prom.Collector{
		c.connections, c.connectionsTotal, c.disconnections, c.events,
		c.eventBytes, c.eventDuration, c.acks, c.ackDuration, c.recoveries,
		c.adapterDuration, c.clientReconnects, c.clientQueuePackets,
		c.clientQueueBytes, c.clientOverflows,
	}
	for _, collector := range c.collectors {
		if err := c.registerer.Register(collector); err != nil {
			c.unregister()
			return nil, fmt.Errorf("register Socket.IO metrics: %w", err)
		}
	}
	return c, nil
}

func (c *Collector) InstrumentServer(server *socket.Server) error {
	if server == nil {
		return fmt.Errorf("instrument Socket.IO server: nil server")
	}
	c.mu.Lock()
	if c.server != nil {
		c.mu.Unlock()
		return fmt.Errorf("instrument Socket.IO server: collector already attached")
	}
	c.server = server
	c.serverListener = c.observeServer
	c.newNamespaceListener = c.onNewNamespace
	c.mu.Unlock()
	_ = server.On("telemetry", c.serverListener)
	_ = server.Sockets().On("new_namespace", c.newNamespaceListener)
	for _, namespace := range server.Namespaces() {
		c.attachAdapter(namespace)
	}
	return nil
}

func (c *Collector) observeServer(args ...any) {
	if len(args) == 0 {
		return
	}
	event, ok := args[0].(socket.TelemetryEvent)
	if !ok {
		return
	}
	key := event.Namespace + "\x00" + string(event.SocketID)
	switch event.Kind {
	case socket.TelemetryConnection:
		labels := socketLabels{namespace: event.Namespace, transport: event.Transport}
		c.mu.Lock()
		c.active[key] = labels
		c.mu.Unlock()
		c.connections.WithLabelValues(labels.namespace, labels.transport).Inc()
		c.connectionsTotal.WithLabelValues(labels.namespace, labels.transport).Inc()
		result := "new"
		if event.RecoveryAttempted && event.Recovered {
			result = "recovered"
		} else if event.RecoveryAttempted {
			result = "failed"
		}
		c.recoveries.WithLabelValues(event.Namespace, result).Inc()
	case socket.TelemetryTransportUpgrade:
		c.mu.Lock()
		labels, exists := c.active[key]
		if exists && labels.transport != event.Transport {
			c.connections.WithLabelValues(labels.namespace, labels.transport).Dec()
			labels.transport = event.Transport
			c.active[key] = labels
			c.connections.WithLabelValues(labels.namespace, labels.transport).Inc()
		}
		c.mu.Unlock()
	case socket.TelemetryDisconnection:
		c.mu.Lock()
		labels, exists := c.active[key]
		delete(c.active, key)
		c.mu.Unlock()
		if exists {
			c.connections.WithLabelValues(labels.namespace, labels.transport).Dec()
			c.disconnections.WithLabelValues(labels.namespace, labels.transport).Inc()
		}
	case socket.TelemetryEventReceived, socket.TelemetryEventSent:
		direction := "received"
		if event.Kind == socket.TelemetryEventSent {
			direction = "sent"
		}
		c.events.WithLabelValues(event.Namespace, event.Transport, direction).Inc()
		c.eventBytes.WithLabelValues(event.Namespace, event.Transport, direction).Add(float64(event.Bytes))
		c.eventDuration.WithLabelValues(event.Namespace, direction).Observe(event.Duration.Seconds())
	case socket.TelemetryAckCompleted, socket.TelemetryAckTimeout:
		outcome := "success"
		if event.Kind == socket.TelemetryAckTimeout {
			outcome = "timeout"
		}
		c.acks.WithLabelValues(event.Namespace, outcome).Inc()
		c.ackDuration.WithLabelValues(event.Namespace, outcome).Observe(event.Duration.Seconds())
	}
}

func (c *Collector) onNewNamespace(args ...any) {
	if len(args) == 0 {
		return
	}
	if namespace, ok := args[0].(socket.Namespace); ok {
		c.attachAdapter(namespace)
	}
}

func (c *Collector) attachAdapter(namespace socket.Namespace) {
	c.mu.Lock()
	if _, exists := c.adapters[namespace.Name()]; exists {
		c.mu.Unlock()
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
		c.adapterDuration.WithLabelValues(
			event.Namespace, event.Operation, strconv.FormatBool(event.Success),
		).Observe(event.Duration.Seconds())
	}
	c.adapters[namespace.Name()] = adapterListener{adapter: namespace.Adapter(), listener: listener}
	c.mu.Unlock()
	_ = namespace.Adapter().On("adapter_operation", listener)
}

func (c *Collector) InstrumentClient(s *client.Socket) error {
	if s == nil {
		return fmt.Errorf("instrument Socket.IO client: nil socket")
	}
	namespace := s.Nsp()
	manager := s.Io()
	reconnectAttempt := func(...any) {
		c.clientReconnects.WithLabelValues(namespace, "attempt").Inc()
	}
	reconnectSuccess := func(...any) {
		c.clientReconnects.WithLabelValues(namespace, "success").Inc()
	}
	reconnectError := func(...any) {
		c.clientReconnects.WithLabelValues(namespace, "error").Inc()
	}
	updateQueues := func(...any) {
		stats := s.BufferStats()
		c.setQueueStats(namespace, stats)
	}
	overflow := func(args ...any) {
		if len(args) > 0 {
			if details, ok := args[0].(*client.OverflowDetails); ok {
				c.clientOverflows.WithLabelValues(namespace, details.Buffer, string(details.Strategy)).Inc()
			}
		}
		updateQueues()
	}
	_ = manager.On("reconnect_attempt", reconnectAttempt)
	_ = manager.On("reconnect", reconnectSuccess)
	_ = manager.On("reconnect_error", reconnectError)
	_ = s.On("connect", updateQueues)
	_ = s.On("drain", updateQueues)
	_ = s.On("overflow", overflow)
	c.mu.Lock()
	c.clientCleanups = append(c.clientCleanups, func() {
		manager.RemoveListener("reconnect_attempt", reconnectAttempt)
		manager.RemoveListener("reconnect", reconnectSuccess)
		manager.RemoveListener("reconnect_error", reconnectError)
		s.RemoveListener("connect", updateQueues)
		s.RemoveListener("drain", updateQueues)
		s.RemoveListener("overflow", overflow)
	})
	c.mu.Unlock()
	updateQueues()
	return nil
}

func (c *Collector) setQueueStats(namespace string, stats client.BufferStats) {
	c.clientQueuePackets.WithLabelValues(namespace, "send").Set(float64(stats.SendPackets))
	c.clientQueuePackets.WithLabelValues(namespace, "receive").Set(float64(stats.ReceivePackets))
	c.clientQueuePackets.WithLabelValues(namespace, "retry").Set(float64(stats.RetryPackets))
	c.clientQueueBytes.WithLabelValues(namespace, "send").Set(float64(stats.SendBytes))
	c.clientQueueBytes.WithLabelValues(namespace, "receive").Set(float64(stats.ReceiveBytes))
	c.clientQueueBytes.WithLabelValues(namespace, "retry").Set(float64(stats.RetryBytes))
}

func (c *Collector) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		server := c.server
		serverListener := c.serverListener
		newNamespaceListener := c.newNamespaceListener
		adapters := make([]adapterListener, 0, len(c.adapters))
		for _, listener := range c.adapters {
			adapters = append(adapters, listener)
		}
		clientCleanups := append([]func(){}, c.clientCleanups...)
		c.mu.Unlock()
		if server != nil {
			server.RemoveListener("telemetry", serverListener)
			server.Sockets().EventEmitter().RemoveListener("new_namespace", newNamespaceListener)
		}
		for _, listener := range adapters {
			listener.adapter.RemoveListener("adapter_operation", listener.listener)
		}
		for _, cleanup := range clientCleanups {
			cleanup()
		}
		c.unregister()
	})
}

func (c *Collector) unregister() {
	for _, collector := range c.collectors {
		c.registerer.Unregister(collector)
	}
}
