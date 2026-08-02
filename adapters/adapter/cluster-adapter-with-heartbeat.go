package adapter

import (
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

// ClusterAdapterWithHeartbeatBuilder is a builder for creating ClusterAdapterWithHeartbeat instances.
type (
	ClusterAdapterWithHeartbeatBuilder struct {
		Opts ClusterAdapterOptionsInterface
	}

	// clusterAdapterWithHeartbeat implements the ClusterAdapterWithHeartbeat interface.
	clusterAdapterWithHeartbeat struct {
		ClusterAdapter

		_opts                         *ClusterAdapterOptions
		requestsTimeout               time.Duration
		responseTimeoutErrorFormatter ResponseTimeoutErrorFormatter

		heartbeatTimer atomic.Pointer[utils.Timer]
		nodesMap       *types.Map[ServerId, int64] // uid => timestamp of last message
		cleanupTimer   atomic.Pointer[utils.Timer]
		customRequests *types.Map[string, *CustomClusterRequest]
	}
)

func (c *ClusterAdapterWithHeartbeatBuilder) SupportsConnectionStateRecovery() bool { return false }

func (c *ClusterAdapterWithHeartbeatBuilder) Capabilities() socket.AdapterCapabilities {
	capabilities := (&ClusterAdapterBuilder{}).Capabilities()
	capabilities.NodeDiscovery = true
	return capabilities
}

func (c *ClusterAdapterWithHeartbeatBuilder) New(nsp socket.Namespace) Adapter {
	return NewClusterAdapterWithHeartbeat(nsp, c.Opts)
}

func MakeClusterAdapterWithHeartbeat() ClusterAdapterWithHeartbeat {
	c := &clusterAdapterWithHeartbeat{
		ClusterAdapter: MakeClusterAdapter(),

		_opts:                         DefaultClusterAdapterOptions(),
		requestsTimeout:               DEFAULT_TIMEOUT,
		responseTimeoutErrorFormatter: missingResponsesTimeoutError,
		nodesMap:                      &types.Map[ServerId, int64]{},
		customRequests:                &types.Map[string, *CustomClusterRequest]{},
	}

	c.Prototype(c)

	return c
}

func NewClusterAdapterWithHeartbeat(nsp socket.Namespace, opts any) ClusterAdapterWithHeartbeat {
	c := MakeClusterAdapterWithHeartbeat()

	c.SetOpts(opts)

	c.Construct(nsp)

	return c
}

func (a *clusterAdapterWithHeartbeat) SetOpts(opts any) {
	if options, ok := opts.(ClusterAdapterOptionsInterface); ok {
		a._opts.Assign(options)
	}
}

// SetRequestsTimeout configures the default wait for inter-node responses.
func (a *clusterAdapterWithHeartbeat) SetRequestsTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = DEFAULT_TIMEOUT
	}
	a.requestsTimeout = timeout
}

// RequestsTimeout returns the configured default wait for inter-node responses.
func (a *clusterAdapterWithHeartbeat) RequestsTimeout() time.Duration {
	if a.requestsTimeout <= 0 {
		return DEFAULT_TIMEOUT
	}
	return a.requestsTimeout
}

// SetResponseTimeoutErrorFormatter selects the observable timeout error text.
func (a *clusterAdapterWithHeartbeat) SetResponseTimeoutErrorFormatter(formatter ResponseTimeoutErrorFormatter) {
	if formatter == nil {
		formatter = missingResponsesTimeoutError
	}
	a.responseTimeoutErrorFormatter = formatter
}

func missingResponsesTimeoutError(received, expected int) error {
	return fmt.Errorf("timeout reached: missing %d responses", expected-received)
}

func (a *clusterAdapterWithHeartbeat) responseTimeoutError(request *CustomClusterRequest) error {
	expected := request.Expected
	missing := request.MissingUids.Len()
	if expected < missing {
		expected = missing
	}
	received := expected - missing
	if request.Current != nil {
		received = int(request.Current.Load())
	}
	formatter := a.responseTimeoutErrorFormatter
	if formatter == nil {
		formatter = missingResponsesTimeoutError
	}
	return formatter(received, expected)
}

func (a *clusterAdapterWithHeartbeat) Construct(nsp socket.Namespace) {
	a.ClusterAdapter.Construct(nsp)

	if a._opts.GetRawHeartbeatInterval() == nil {
		a._opts.SetHeartbeatInterval(5_000 * time.Millisecond)
	}

	if a._opts.GetRawHeartbeatTimeout() == nil {
		a._opts.SetHeartbeatTimeout(10_000)
	}

	a.cleanupTimer.Store(utils.SetInterval(func() {
		now := time.Now().UnixMilli()
		a.nodesMap.Range(func(uid ServerId, lastSeen int64) bool {
			if now-lastSeen > a._opts.HeartbeatTimeout() {
				adapterLog.Debug("[%s] node %s seems down", a.Uid(), uid)
				a.removeNode(uid)
			}
			return true
		})
	}, 1_000*time.Millisecond))
}

func (a *clusterAdapterWithHeartbeat) Init() {
	a.Publish(&ClusterMessage{
		Type: INITIAL_HEARTBEAT,
	})
}

func (a *clusterAdapterWithHeartbeat) scheduleHeartbeat() {
	if heartbeatTimer := a.heartbeatTimer.Load(); heartbeatTimer != nil {
		heartbeatTimer.Refresh()
	} else {
		a.heartbeatTimer.Store(utils.SetTimeout(func() {
			a.Publish(&ClusterMessage{
				Type: HEARTBEAT,
			})
		}, a._opts.HeartbeatInterval()))
	}
}

func (a *clusterAdapterWithHeartbeat) Close() {
	a.close(true)
}

func (a *clusterAdapterWithHeartbeat) CloseLocal() {
	a.close(false)
}

func (a *clusterAdapterWithHeartbeat) close(publish bool) {
	if publish {
		a.Publish(&ClusterMessage{
			Type: ADAPTER_CLOSE,
		})
	}
	utils.ClearTimeout(a.heartbeatTimer.Load())
	utils.ClearInterval(a.cleanupTimer.Load())
}

func (a *clusterAdapterWithHeartbeat) OnMessage(message *ClusterMessage, offset Offset) {
	if message.Uid == a.Uid() {
		adapterLog.Debug("[%s] ignore message from self", a.Uid())
		return
	}

	if message.Uid != EMITTER_UID {
		// we track the UID of each sender, in order to know how many servers there are in the cluster
		a.nodesMap.Store(message.Uid, time.Now().UnixMilli())
	}

	adapterLog.Debug(
		"[%s] new event of type %d from %s",
		a.Uid(),
		message.Type,
		message.Uid,
	)

	switch message.Type {
	case INITIAL_HEARTBEAT:
		a.Publish(&ClusterMessage{Type: HEARTBEAT})
	case HEARTBEAT:
		// Do nothing
	case ADAPTER_CLOSE:
		a.removeNode(message.Uid)
	case BROADCAST_CLIENT_COUNT, BROADCAST_ACK, FETCH_SOCKETS_RESPONSE,
		SERVER_SIDE_EMIT_RESPONSE, COUNT_SOCKETS_RESPONSE, LIST_ROOMS_RESPONSE:
		// Responses for heartbeat-aware requests must be handled here so the
		// per-node MissingUids set is updated. Passing them through the embedded
		// ClusterAdapter would call its base OnResponse method instead.
		a.OnResponse(message)
	default:
		a.ClusterAdapter.OnMessage(message, offset)
	}
}

func (a *clusterAdapterWithHeartbeat) ServerCount() int64 {
	return int64(a.nodesMap.Len() + 1)
}

func (a *clusterAdapterWithHeartbeat) Publish(message *ClusterMessage) {
	a.scheduleHeartbeat()

	a.ClusterAdapter.Publish(message)
}

func (a *clusterAdapterWithHeartbeat) ServerSideEmit(packet []any) error {
	if len(packet) == 0 {
		return fmt.Errorf("packet cannot be empty")
	}

	data_len := len(packet)
	ack, withAck := packet[data_len-1].(socket.Ack)
	if !withAck {
		a.Publish(&ClusterMessage{
			Type: SERVER_SIDE_EMIT,
			Data: &ServerSideEmitMessage{
				Packet: packet,
			},
		})
		return nil
	}
	expectedResponseCount := a.nodesMap.Len()

	adapterLog.Debug(
		`[%s] waiting for %d responses to "serverSideEmit" request`,
		a.Uid(),
		expectedResponseCount,
	)

	if expectedResponseCount <= 0 {
		ack(nil, nil)
		return nil
	}

	requestId := RandomId()

	timeout := utils.SetTimeout(func() {
		if storedRequest, ok := a.customRequests.Load(requestId); ok {
			storedRequest.Once.Do(func() {
				ack(
					storedRequest.Responses.All(),
					a.responseTimeoutError(storedRequest),
				)
				a.customRequests.Delete(requestId)
			})
		}
	}, a.RequestsTimeout())

	a.customRequests.Store(requestId, &CustomClusterRequest{
		Type: SERVER_SIDE_EMIT,
		Resolve: func(data *types.Slice[any]) {
			ack(data.All(), nil)
		},
		Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(t *atomic.Pointer[utils.Timer]) {
			t.Store(timeout)
		}),
		Expected:    expectedResponseCount,
		Current:     &atomic.Int64{},
		MissingUids: types.NewSet(a.nodesMap.Keys()...),
		Responses:   types.NewSlice[any](),
	})

	a.Publish(&ClusterMessage{
		Type: SERVER_SIDE_EMIT,
		Data: &ServerSideEmitMessage{
			RequestId: &requestId, // the presence of this attribute defines whether an acknowledgement is needed
			Packet:    packet[:data_len-1],
		},
	})
	return nil
}

func (a *clusterAdapterWithHeartbeat) FetchSockets(opts *socket.BroadcastOptions) func(func([]socket.SocketDetails, error)) {
	if opts == nil {
		opts = &socket.BroadcastOptions{
			Rooms:  types.NewSet[socket.Room](),
			Except: types.NewSet[socket.Room](),
		}
	}
	return func(cb func([]socket.SocketDetails, error)) {
		a.ClusterAdapter.FetchSockets(&socket.BroadcastOptions{
			Rooms:  opts.Rooms,
			Except: opts.Except,
			Flags: &socket.BroadcastFlags{
				Local: true,
			},
		})(func(localSockets []socket.SocketDetails, _ error) {
			expectedResponseCount := a.ServerCount() - 1

			if (opts != nil && opts.Flags != nil && opts.Flags.Local) || expectedResponseCount <= 0 {
				cb(localSockets, nil)
				return
			}

			requestId := RandomId()

			t := a.RequestsTimeout()
			if opts != nil && opts.Flags != nil && opts.Flags.Timeout != nil {
				t = *opts.Flags.Timeout
			}

			timeout := utils.SetTimeout(func() {
				if storedRequest, ok := a.customRequests.Load(requestId); ok {
					storedRequest.Once.Do(func() {
						cb(nil, a.responseTimeoutError(storedRequest))
						a.customRequests.Delete(requestId)
					})
				}
			}, t)

			a.customRequests.Store(requestId, &CustomClusterRequest{
				Type: FETCH_SOCKETS,
				Resolve: func(data *types.Slice[any]) {
					cb(slices.Map(data.All(), func(i any) socket.SocketDetails {
						return utils.TryCast[socket.SocketDetails](i)
					}), nil)
				},
				Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(t *atomic.Pointer[utils.Timer]) {
					t.Store(timeout)
				}),
				Expected:    int(expectedResponseCount),
				Current:     &atomic.Int64{},
				MissingUids: types.NewSet(a.nodesMap.Keys()...),
				Responses: types.NewSlice(slices.Map(localSockets, func(client socket.SocketDetails) any {
					return client
				})...),
			})

			a.Publish(&ClusterMessage{
				Type: FETCH_SOCKETS,
				Data: &FetchSocketsMessage{
					Opts:      EncodeOptions(opts),
					RequestId: requestId,
				},
			})
		})
	}
}

func (a *clusterAdapterWithHeartbeat) CountSockets(opts *socket.BroadcastOptions) func(func(uint64, error)) {
	if opts == nil {
		opts = &socket.BroadcastOptions{
			Rooms:  types.NewSet[socket.Room](),
			Except: types.NewSet[socket.Room](),
		}
	}
	return func(callback func(uint64, error)) {
		a.ClusterAdapter.CountSockets(&socket.BroadcastOptions{
			Rooms: opts.Rooms, Except: opts.Except,
			Flags: &socket.BroadcastFlags{Local: true},
		})(func(localCount uint64, localErr error) {
			if localErr != nil {
				callback(0, localErr)
				return
			}
			if (opts.Flags != nil && opts.Flags.Local) || a.ServerCount() <= 1 {
				callback(localCount, nil)
				return
			}
			requestID := RandomId()
			timeoutDuration := a.RequestsTimeout()
			if opts.Flags != nil && opts.Flags.Timeout != nil {
				timeoutDuration = *opts.Flags.Timeout
			}
			timeout := utils.SetTimeout(func() {
				if request, ok := a.customRequests.Load(requestID); ok {
					request.Once.Do(func() {
						callback(0, a.responseTimeoutError(request))
						a.customRequests.Delete(requestID)
					})
				}
			}, timeoutDuration)
			a.customRequests.Store(requestID, &CustomClusterRequest{
				Type: COUNT_SOCKETS,
				Resolve: func(data *types.Slice[any]) {
					count := localCount
					for _, value := range data.All() {
						if remoteCount, ok := value.(uint64); ok {
							count += remoteCount
						}
					}
					callback(count, nil)
				},
				Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(pointer *atomic.Pointer[utils.Timer]) {
					pointer.Store(timeout)
				}),
				Expected:    a.nodesMap.Len(),
				Current:     &atomic.Int64{},
				MissingUids: types.NewSet(a.nodesMap.Keys()...),
				Responses:   types.NewSlice[any](),
			})
			a.Publish(&ClusterMessage{
				Type: COUNT_SOCKETS,
				Data: &CountSocketsMessage{Opts: EncodeOptions(opts), RequestId: requestID},
			})
		})
	}
}

func (a *clusterAdapterWithHeartbeat) ListRooms(opts *socket.BroadcastOptions) func(func(map[socket.Room]uint64, error)) {
	if opts == nil {
		opts = &socket.BroadcastOptions{
			Rooms:  types.NewSet[socket.Room](),
			Except: types.NewSet[socket.Room](),
		}
	}
	return func(callback func(map[socket.Room]uint64, error)) {
		a.ClusterAdapter.ListRooms(&socket.BroadcastOptions{
			Rooms: opts.Rooms, Except: opts.Except,
			Flags: &socket.BroadcastFlags{Local: true},
		})(func(localRooms map[socket.Room]uint64, localErr error) {
			if localErr != nil {
				callback(nil, localErr)
				return
			}
			if (opts.Flags != nil && opts.Flags.Local) || a.ServerCount() <= 1 {
				callback(localRooms, nil)
				return
			}
			requestID := RandomId()
			timeoutDuration := a.RequestsTimeout()
			if opts.Flags != nil && opts.Flags.Timeout != nil {
				timeoutDuration = *opts.Flags.Timeout
			}
			timeout := utils.SetTimeout(func() {
				if request, ok := a.customRequests.Load(requestID); ok {
					request.Once.Do(func() {
						callback(nil, a.responseTimeoutError(request))
						a.customRequests.Delete(requestID)
					})
				}
			}, timeoutDuration)
			a.customRequests.Store(requestID, &CustomClusterRequest{
				Type: LIST_ROOMS,
				Resolve: func(data *types.Slice[any]) {
					rooms := make(map[socket.Room]uint64, len(localRooms))
					maps.Copy(rooms, localRooms)
					for _, value := range data.All() {
						if remoteRooms, ok := value.(map[socket.Room]uint64); ok {
							for room, count := range remoteRooms {
								rooms[room] += count
							}
						}
					}
					callback(rooms, nil)
				},
				Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(pointer *atomic.Pointer[utils.Timer]) {
					pointer.Store(timeout)
				}),
				Expected:    a.nodesMap.Len(),
				Current:     &atomic.Int64{},
				MissingUids: types.NewSet(a.nodesMap.Keys()...),
				Responses:   types.NewSlice[any](),
			})
			a.Publish(&ClusterMessage{
				Type: LIST_ROOMS,
				Data: &ListRoomsMessage{Opts: EncodeOptions(opts), RequestId: requestID},
			})
		})
	}
}

func (a *clusterAdapterWithHeartbeat) OnResponse(response *ClusterResponse) {
	switch response.Type {
	case FETCH_SOCKETS_RESPONSE:
		data, ok := response.Data.(*FetchSocketsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for FETCH_SOCKETS_RESPONSE message", a.Uid())
			return
		}
		adapterLog.Debug("[%s] received response %d to request %s", a.Uid(), response.Type, data.RequestId)
		if request, ok := a.customRequests.Load(data.RequestId); ok {
			if request.Current != nil {
				request.Current.Add(1)
			}
			request.Responses.Push(slices.Map(data.Sockets, func(client *SocketResponse) any {
				return socket.SocketDetails(NewRemoteSocket(client))
			})...)

			request.MissingUids.Delete(response.Uid)
			if request.MissingUids.Len() == 0 {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					a.customRequests.Delete(data.RequestId)
				})
			}
		}

	case SERVER_SIDE_EMIT_RESPONSE:
		data, ok := response.Data.(*ServerSideEmitResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for SERVER_SIDE_EMIT_RESPONSE message", a.Uid())
			return
		}
		adapterLog.Debug("[%s] received response %d to request %s", a.Uid(), response.Type, data.RequestId)
		if request, ok := a.customRequests.Load(data.RequestId); ok {
			if request.Current != nil {
				request.Current.Add(1)
			}
			request.Responses.Push(data.Packet)

			request.MissingUids.Delete(response.Uid)
			if request.MissingUids.Len() == 0 {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					a.customRequests.Delete(data.RequestId)
				})
			}
		}

	case COUNT_SOCKETS_RESPONSE:
		data, ok := response.Data.(*CountSocketsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for COUNT_SOCKETS_RESPONSE message", a.Uid())
			return
		}
		if request, ok := a.customRequests.Load(data.RequestId); ok {
			if request.Current != nil {
				request.Current.Add(1)
			}
			request.Responses.Push(data.Count)
			request.MissingUids.Delete(response.Uid)
			if request.MissingUids.Len() == 0 {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					a.customRequests.Delete(data.RequestId)
				})
			}
		}

	case LIST_ROOMS_RESPONSE:
		data, ok := response.Data.(*ListRoomsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for LIST_ROOMS_RESPONSE message", a.Uid())
			return
		}
		if request, ok := a.customRequests.Load(data.RequestId); ok {
			if request.Current != nil {
				request.Current.Add(1)
			}
			request.Responses.Push(data.Rooms)
			request.MissingUids.Delete(response.Uid)
			if request.MissingUids.Len() == 0 {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					a.customRequests.Delete(data.RequestId)
				})
			}
		}

	default:
		a.ClusterAdapter.OnResponse(response)
	}
}

func (a *clusterAdapterWithHeartbeat) removeNode(uid ServerId) {
	a.customRequests.Range(func(requestId string, request *CustomClusterRequest) bool {
		request.MissingUids.Delete(uid)
		if request.MissingUids.Len() == 0 {
			request.Once.Do(func() {
				utils.ClearTimeout(request.Timeout.Load())
				request.Resolve(request.Responses)
				a.customRequests.Delete(requestId)
			})
		}
		return true
	})

	a.nodesMap.Delete(uid)
}
