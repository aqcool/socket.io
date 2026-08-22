package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
)

func TestMemoryClusterBusPublishesAsynchronouslyInOrder(t *testing.T) {
	const messageCount = 64
	type receivedMessage struct {
		requestID uint64
		data      byte
	}

	bus := NewMemoryClusterBus()
	t.Cleanup(func() { _ = bus.Close() })

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	received := make(chan receivedMessage, messageCount)
	var firstOnce sync.Once
	unsubscribe, err := bus.Subscribe("edge", func(message *ClusterMessage) {
		if message.RequestID == 1 {
			firstOnce.Do(func() { close(firstStarted) })
			<-releaseFirst
		}
		var data byte
		if len(message.Packets) > 0 && len(message.Packets[0].Data) > 0 {
			data = message.Packets[0].Data[0]
		}
		received <- receivedMessage{requestID: message.RequestID, data: data}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unsubscribe)

	firstPublish := make(chan error, 1)
	go func() {
		firstPublish <- bus.Publish(context.Background(), &ClusterMessage{
			RecipientID: "edge",
			RequestID:   1,
			Type:        ClusterMessagePacket,
		})
	}()
	select {
	case err := <-firstPublish:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Publish synchronously waited for the subscription listener")
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("subscription listener did not receive the first message")
	}

	message := &ClusterMessage{RecipientID: "edge", Type: ClusterMessagePacket}
	for requestID := uint64(2); requestID <= messageCount; requestID++ {
		message.RequestID = requestID
		message.Packets = []ClusterPacket{{Data: []byte{byte(requestID)}}}
		if err := bus.Publish(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		// A queued delivery must own a deep clone rather than observing later
		// caller mutations while the first listener invocation is blocked.
		message.RequestID = 0
		message.Packets[0].Data[0] = 0
	}
	close(releaseFirst)

	for want := uint64(1); want <= messageCount; want++ {
		select {
		case got := <-received:
			if got.requestID != want {
				t.Fatalf("delivery %d has request ID %d", want, got.requestID)
			}
			if want > 1 && got.data != byte(want) {
				t.Fatalf("delivery %d has cloned data %d", want, got.data)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for ordered delivery %d", want)
		}
	}
}

func TestMemoryClusterBusStopIsNonBlockingAndDropsQueuedMessages(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*MemoryClusterBus, func())
	}{
		{
			name: "unsubscribe",
			stop: func(_ *MemoryClusterBus, unsubscribe func()) { unsubscribe() },
		},
		{
			name: "close",
			stop: func(bus *MemoryClusterBus, _ func()) { _ = bus.Close() },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bus := NewMemoryClusterBus()
			t.Cleanup(func() { _ = bus.Close() })

			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			delivered := make(chan uint64, 3)
			unsubscribe, err := bus.Subscribe("edge", func(message *ClusterMessage) {
				delivered <- message.RequestID
				if message.RequestID == 1 {
					close(firstStarted)
					<-releaseFirst
				}
			})
			if err != nil {
				t.Fatal(err)
			}

			bus.mu.RLock()
			var subscriber *memoryClusterSubscriber
			for _, candidate := range bus.subscribers {
				subscriber = candidate
				break
			}
			bus.mu.RUnlock()
			if subscriber == nil {
				t.Fatal("subscription was not registered")
			}

			for requestID := uint64(1); requestID <= 3; requestID++ {
				if err := bus.Publish(context.Background(), &ClusterMessage{
					RecipientID: "edge",
					RequestID:   requestID,
					Type:        ClusterMessagePacket,
				}); err != nil {
					t.Fatal(err)
				}
				if requestID == 1 {
					select {
					case <-firstStarted:
					case <-time.After(time.Second):
						t.Fatal("first listener invocation did not start")
					}
				}
			}

			stopped := make(chan struct{})
			go func() {
				test.stop(bus, unsubscribe)
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(250 * time.Millisecond):
				t.Fatal("subscription stop waited for the active listener")
			}

			subscriber.mu.Lock()
			queued := len(subscriber.pending)
			isStopped := subscriber.stopped
			subscriber.mu.Unlock()
			if !isStopped || queued != 0 {
				t.Fatalf("stopped = %t, queued messages = %d", isStopped, queued)
			}

			close(releaseFirst)
			select {
			case <-subscriber.done:
			case <-time.After(time.Second):
				t.Fatal("subscription worker did not stop after active listener returned")
			}
			close(delivered)
			var got []uint64
			for requestID := range delivered {
				got = append(got, requestID)
			}
			if len(got) != 1 || got[0] != 1 {
				t.Fatalf("deliveries after stop = %v, want [1]", got)
			}
		})
	}
}

func TestMemoryClusterBusListenerCanClosePublisher(t *testing.T) {
	bus := NewMemoryClusterBus()
	t.Cleanup(func() { _ = bus.Close() })

	options := config.DefaultServerOptions()
	server, err := NewClusterServer(bus, options, &ClusterOptions{
		ResponseTimeout:          500 * time.Millisecond,
		DelayedConnectionTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher := server.(*clusterServer)
	t.Cleanup(func() { publisher.Close() })

	listenerDone := make(chan struct{})
	unsubscribe, err := bus.Subscribe("closing-edge", func(*ClusterMessage) {
		publisher.Close()
		close(listenerDone)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unsubscribe)

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- publisher.publishTransportMessage(&ClusterMessage{
			RecipientID: "closing-edge",
			Type:        ClusterMessageClose,
			SID:         "publisher-close-test",
		})
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("publisher remained blocked in asynchronous bus delivery")
	}
	select {
	case <-listenerDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("listener deadlocked while closing the publishing ClusterServer")
	}
	if active := publisher.activeTransportPublishes(); active != 0 {
		t.Fatalf("active transport publications after publisher close = %d", active)
	}
}

func TestMemoryClusterBusListenerCanStopOwnSubscription(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*MemoryClusterBus, func())
	}{
		{
			name: "unsubscribe",
			stop: func(_ *MemoryClusterBus, unsubscribe func()) { unsubscribe() },
		},
		{
			name: "close bus",
			stop: func(bus *MemoryClusterBus, _ func()) { _ = bus.Close() },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bus := NewMemoryClusterBus()
			t.Cleanup(func() { _ = bus.Close() })

			listenerDone := make(chan struct{})
			var unsubscribe func()
			var err error
			unsubscribe, err = bus.Subscribe("edge", func(*ClusterMessage) {
				test.stop(bus, unsubscribe)
				close(listenerDone)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := bus.Publish(context.Background(), &ClusterMessage{
				RecipientID: "edge",
				Type:        ClusterMessageClose,
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-listenerDone:
			case <-time.After(250 * time.Millisecond):
				t.Fatal("listener deadlocked while stopping its own subscription")
			}
		})
	}
}

func TestMemoryClusterBusConcurrentPublishAndClose(t *testing.T) {
	const (
		publisherCount = 8
		messagesEach   = 64
	)

	bus := NewMemoryClusterBus()
	unsubscribe, err := bus.Subscribe("edge", func(*ClusterMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()

	bus.mu.RLock()
	var subscriber *memoryClusterSubscriber
	for _, candidate := range bus.subscribers {
		subscriber = candidate
		break
	}
	bus.mu.RUnlock()
	if subscriber == nil {
		t.Fatal("subscription was not registered")
	}

	start := make(chan struct{})
	errorsSeen := make(chan error, publisherCount*messagesEach)
	var publishers sync.WaitGroup
	for publisherID := range publisherCount {
		publishers.Go(func() {
			<-start
			for sequence := range messagesEach {
				err := bus.Publish(context.Background(), &ClusterMessage{
					RecipientID: "edge",
					RequestID:   uint64(publisherID*messagesEach + sequence + 1),
					Type:        ClusterMessagePacket,
				})
				if err != nil {
					errorsSeen <- err
				}
			}
		})
	}
	close(start)
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	publishers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, ErrClusterBusClosed) {
			t.Fatalf("concurrent Publish error = %v", err)
		}
	}
	select {
	case <-subscriber.done:
	case <-time.After(time.Second):
		t.Fatal("subscription worker survived concurrent Close")
	}
}

func TestMemoryClusterBusRejectsPublishAfterClose(t *testing.T) {
	bus := NewMemoryClusterBus()
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	err := bus.Publish(context.Background(), &ClusterMessage{Type: ClusterMessageClose})
	if !errors.Is(err, ErrClusterBusClosed) {
		t.Fatalf("Publish after Close error = %v, want %v", err, ErrClusterBusClosed)
	}
	if _, err := bus.Subscribe("edge", func(*ClusterMessage) {}); !errors.Is(err, ErrClusterBusClosed) {
		t.Fatalf("Subscribe after Close error = %v, want %v", err, ErrClusterBusClosed)
	}
}
