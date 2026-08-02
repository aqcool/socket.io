package adapter

import (
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aqcool/socket.io/parsers/socket/v3/parser"
	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/slices"
	"github.com/aqcool/socket.io/v3/pkg/types"
	"github.com/aqcool/socket.io/v3/pkg/utils"
)

// ClusterAdapterBuilder is a builder for creating ClusterAdapter instances.
//
// A cluster-ready adapter. Any extending interface must:
//   - implement ClusterAdapter.DoPublish and ClusterAdapter.DoPublishResponse
//   - call ClusterAdapter.OnMessage and ClusterAdapter.OnResponse
type (
	ClusterAdapterBuilder struct{}

	// clusterAdapter implements the ClusterAdapter interface for cluster communication.
	clusterAdapter struct {
		Adapter

		// uid is the unique server identifier.
		uid ServerId

		requests    *types.Map[string, *ClusterRequest]
		ackRequests *types.Map[string, *ClusterAckRequest]
	}
)

func (cb *ClusterAdapterBuilder) SupportsConnectionStateRecovery() bool { return false }

func (cb *ClusterAdapterBuilder) Capabilities() socket.AdapterCapabilities {
	capabilities := (&AdapterBuilder{}).Capabilities()
	capabilities.NodeDiscovery = true
	return capabilities
}

// New creates a new ClusterAdapter for the given Namespace.
func (cb *ClusterAdapterBuilder) New(nsp socket.Namespace) Adapter {
	return NewClusterAdapter(nsp)
}

// MakeClusterAdapter returns a new default ClusterAdapter instance.
func MakeClusterAdapter() ClusterAdapter {
	c := &clusterAdapter{
		Adapter:     MakeAdapter(),
		requests:    &types.Map[string, *ClusterRequest]{},
		ackRequests: &types.Map[string, *ClusterAckRequest]{},
	}
	c.Prototype(c)
	return c
}

// NewClusterAdapter creates a new ClusterAdapter for the given Namespace.
func NewClusterAdapter(nsp socket.Namespace) ClusterAdapter {
	c := MakeClusterAdapter()
	c.Construct(nsp)
	return c
}

// Uid returns the unique server identifier.
func (c *clusterAdapter) Uid() ServerId {
	return c.uid
}

// Construct initializes the clusterAdapter with the given Namespace.
func (c *clusterAdapter) Construct(nsp socket.Namespace) {
	c.Adapter.Construct(nsp)
	c.uid = ServerId(RandomId())
}

// OnMessage handles incoming messages
func (c *clusterAdapter) OnMessage(message *ClusterMessage, offset Offset) {
	if message.Uid == c.uid {
		adapterLog.Debug("[%s] ignore message from self", c.uid)
		return
	}

	if message.Nsp != c.Nsp().Name() {
		adapterLog.Debug("[%s] ignore message from another namespace (%s)", c.uid, message.Nsp)
		return
	}

	adapterLog.Debug("[%s] new event of type %d from %s", c.uid, message.Type, message.Uid)

	switch message.Type {
	case BROADCAST:
		data, ok := message.Data.(*BroadcastMessage)
		if !ok {
			adapterLog.Debug("[%s] invalid data for BROADCAST message", c.uid)
			return
		}

		opts := DecodeOptions(data.Opts)
		if data.RequestId != nil {
			c.Adapter.BroadcastWithAck(
				data.Packet,
				opts,
				func(clientCount uint64) {
					adapterLog.Debug("[%s] waiting for %d client acknowledgements", c.uid, clientCount)
					c.PublishResponse(message.Uid, &ClusterResponse{
						Type: BROADCAST_CLIENT_COUNT,
						Data: &BroadcastClientCount{
							RequestId:   *data.RequestId,
							ClientCount: clientCount,
						},
					})
				},
				func(args []any, _ error) {
					adapterLog.Debug("[%s] received acknowledgement with value %v", c.uid, args)
					c.PublishResponse(message.Uid, &ClusterResponse{
						Type: BROADCAST_ACK,
						Data: &BroadcastAck{
							RequestId: *data.RequestId,
							Packet:    firstAckArgument(args),
						},
					})
				},
			)
		} else {
			c.addOffsetIfNecessary(data.Packet, opts, offset)
			c.Adapter.Broadcast(data.Packet, opts)
		}

	case SOCKETS_JOIN:
		if data, ok := message.Data.(*SocketsJoinLeaveMessage); ok {
			c.Adapter.AddSockets(DecodeOptions(data.Opts), data.Rooms)
		} else {
			adapterLog.Debug("[%s] invalid data for SOCKETS_JOIN message", c.uid)
		}

	case SOCKETS_LEAVE:
		if data, ok := message.Data.(*SocketsJoinLeaveMessage); ok {
			c.Adapter.DelSockets(DecodeOptions(data.Opts), data.Rooms)
		} else {
			adapterLog.Debug("[%s] invalid data for SOCKETS_LEAVE message", c.uid)
		}

	case DISCONNECT_SOCKETS:
		if data, ok := message.Data.(*DisconnectSocketsMessage); ok {
			c.Adapter.DisconnectSockets(DecodeOptions(data.Opts), data.Close)
		} else {
			adapterLog.Debug("[%s] invalid data for DISCONNECT_SOCKETS message", c.uid)
		}

	case FETCH_SOCKETS:
		data, ok := message.Data.(*FetchSocketsMessage)
		if !ok {
			adapterLog.Debug("[%s] invalid data for FETCH_SOCKETS message", c.uid)
			return
		}
		adapterLog.Debug("[%s] calling fetchSockets with opts %v", c.uid, data.Opts)

		c.Adapter.FetchSockets(DecodeOptions(data.Opts))(
			func(localSockets []socket.SocketDetails, err error) {
				if err != nil {
					adapterLog.Debug("FETCH_SOCKETS Adapter.OnMessage error: %s", err.Error())
					return
				}

				c.PublishResponse(message.Uid, &ClusterResponse{
					Type: FETCH_SOCKETS_RESPONSE,
					Data: &FetchSocketsResponse{
						RequestId: data.RequestId,
						Sockets: slices.Map(localSockets, func(client socket.SocketDetails) *SocketResponse {
							return &SocketResponse{
								Id:        client.Id(),
								Handshake: client.Handshake(),
								Rooms:     client.Rooms().Keys(),
								Data:      client.Data(),
							}
						}),
					},
				})
			},
		)

	case COUNT_SOCKETS:
		data, ok := message.Data.(*CountSocketsMessage)
		if !ok {
			adapterLog.Debug("[%s] invalid data for COUNT_SOCKETS message", c.uid)
			return
		}
		c.Adapter.CountSockets(DecodeOptions(data.Opts))(func(count uint64, err error) {
			if err != nil {
				adapterLog.Debug("COUNT_SOCKETS Adapter.OnMessage error: %s", err.Error())
				return
			}
			c.PublishResponse(message.Uid, &ClusterResponse{
				Type: COUNT_SOCKETS_RESPONSE,
				Data: &CountSocketsResponse{RequestId: data.RequestId, Count: count},
			})
		})

	case LIST_ROOMS:
		data, ok := message.Data.(*ListRoomsMessage)
		if !ok {
			adapterLog.Debug("[%s] invalid data for LIST_ROOMS message", c.uid)
			return
		}
		c.Adapter.ListRooms(DecodeOptions(data.Opts))(func(rooms map[socket.Room]uint64, err error) {
			if err != nil {
				adapterLog.Debug("LIST_ROOMS Adapter.OnMessage error: %s", err.Error())
				return
			}
			c.PublishResponse(message.Uid, &ClusterResponse{
				Type: LIST_ROOMS_RESPONSE,
				Data: &ListRoomsResponse{RequestId: data.RequestId, Rooms: rooms},
			})
		})

	case SERVER_SIDE_EMIT:
		data, ok := message.Data.(*ServerSideEmitMessage)
		if !ok {
			adapterLog.Debug("[%s] invalid data for SERVER_SIDE_EMIT message", c.uid)
			return
		}
		packet := data.Packet
		if data.RequestId == nil {
			c.Nsp().OnServerSideEmit(packet)
			return
		}

		called := &sync.Once{}
		callback := func(arg []any, _ error) {
			// only one argument is expected, ensure Exactly-Once semantics
			called.Do(func() {
				adapterLog.Debug("[%s] calling acknowledgement with %v", c.uid, arg)
				c.PublishResponse(message.Uid, &ClusterResponse{
					Type: SERVER_SIDE_EMIT_RESPONSE,
					Data: &ServerSideEmitResponse{
						RequestId: *data.RequestId,
						Packet:    firstAckArgument(arg),
					},
				})
			})
		}

		c.Nsp().OnServerSideEmit(append(packet, callback))

	case BROADCAST_CLIENT_COUNT, BROADCAST_ACK, FETCH_SOCKETS_RESPONSE,
		SERVER_SIDE_EMIT_RESPONSE, COUNT_SOCKETS_RESPONSE, LIST_ROOMS_RESPONSE:
		// extending classes may not make a distinction between a ClusterMessage and a ClusterResponse payload and may
		// always call the OnMessage() method
		c.OnResponse(message)
	default:
		adapterLog.Debug("[%s] unknown message type: %d", c.uid, message.Type)
	}
}

// OnResponse handles incoming responses
func (c *clusterAdapter) OnResponse(response *ClusterResponse) {
	switch response.Type {
	case BROADCAST_CLIENT_COUNT:
		if data, ok := response.Data.(*BroadcastClientCount); ok {
			adapterLog.Debug("[%s] received response %d to request %s", c.uid, response.Type, data.RequestId)
			if ackRequest, ok := c.ackRequests.Load(data.RequestId); ok {
				ackRequest.ClientCountCallback(data.ClientCount)
			}
		} else {
			adapterLog.Debug("[%s] invalid data for BROADCAST_CLIENT_COUNT message", c.uid)
		}

	case BROADCAST_ACK:
		if data, ok := response.Data.(*BroadcastAck); ok {
			adapterLog.Debug("[%s] received response %d to request %s", c.uid, response.Type, data.RequestId)
			if ackRequest, ok := c.ackRequests.Load(data.RequestId); ok {
				ackRequest.Ack([]any{data.Packet}, nil)
			}
		} else {
			adapterLog.Debug("[%s] invalid data for BROADCAST_ACK message", c.uid)
		}

	case FETCH_SOCKETS_RESPONSE:
		data, ok := response.Data.(*FetchSocketsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for FETCH_SOCKETS_RESPONSE message", c.uid)
			return
		}
		adapterLog.Debug("[%s] received response %d to request %s", c.uid, response.Type, data.RequestId)

		if request, ok := c.requests.Load(data.RequestId); ok {
			request.Responses.Push(slices.Map(data.Sockets, func(client *SocketResponse) any {
				return socket.SocketDetails(NewRemoteSocket(client))
			})...)

			if request.Current.Add(1) == request.Expected {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					c.requests.Delete(data.RequestId)
				})
			}
		}

	case SERVER_SIDE_EMIT_RESPONSE:
		data, ok := response.Data.(*ServerSideEmitResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for SERVER_SIDE_EMIT_RESPONSE message", c.uid)
			return
		}
		adapterLog.Debug("[%s] received response %d to request %s", c.uid, response.Type, data.RequestId)
		if request, ok := c.requests.Load(data.RequestId); ok {
			request.Responses.Push(data.Packet)
			if request.Current.Add(1) == request.Expected {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					c.requests.Delete(data.RequestId)
				})
			}
		}

	case COUNT_SOCKETS_RESPONSE:
		data, ok := response.Data.(*CountSocketsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for COUNT_SOCKETS_RESPONSE message", c.uid)
			return
		}
		if request, ok := c.requests.Load(data.RequestId); ok {
			request.Responses.Push(data.Count)
			if request.Current.Add(1) == request.Expected {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					c.requests.Delete(data.RequestId)
				})
			}
		}

	case LIST_ROOMS_RESPONSE:
		data, ok := response.Data.(*ListRoomsResponse)
		if !ok {
			adapterLog.Debug("[%s] invalid data for LIST_ROOMS_RESPONSE message", c.uid)
			return
		}
		if request, ok := c.requests.Load(data.RequestId); ok {
			request.Responses.Push(data.Rooms)
			if request.Current.Add(1) == request.Expected {
				request.Once.Do(func() {
					utils.ClearTimeout(request.Timeout.Load())
					request.Resolve(request.Responses)
					c.requests.Delete(data.RequestId)
				})
			}
		}
	default:
		adapterLog.Debug("[%s] unknown response type: %d", c.uid, response.Type)
	}
}

func firstAckArgument(args []any) any {
	if len(args) == 0 {
		return nil
	}
	return args[0]
}

func (c *clusterAdapter) Broadcast(packet *parser.Packet, opts *socket.BroadcastOptions) {
	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local

	if !onlyLocal {
		offset, err := c.PublishAndReturnOffset(&ClusterMessage{
			Type: BROADCAST,
			Data: &BroadcastMessage{
				Packet: packet,
				Opts:   EncodeOptions(opts),
			},
		})
		if err != nil {
			adapterLog.Debug("[%s] error while broadcasting message: %s", c.uid, err.Error())
		} else {
			c.addOffsetIfNecessary(packet, opts, offset)
		}
	}

	c.Adapter.Broadcast(packet, opts)
}

// Adds an offset at the end of the data array in order to allow the client to receive any missed packets when it
// reconnects after a temporary disconnection.
func (c *clusterAdapter) addOffsetIfNecessary(packet *parser.Packet, opts *socket.BroadcastOptions, offset Offset) {
	if c.Nsp().Server().Opts().ConnectionStateRecovery() == nil {
		return
	}

	isEventPacket := packet.Type == parser.EVENT
	// packets with acknowledgement are not stored because the acknowledgement function cannot be serialized and
	// restored on another server upon reconnection
	withoutAcknowledgement := packet.Id == nil
	notVolatile := opts == nil || opts.Flags == nil || !opts.Flags.Volatile

	if isEventPacket && withoutAcknowledgement && notVolatile {
		packet.Data = append(utils.TryCast[[]any](packet.Data), offset)
	}
}

func (c *clusterAdapter) BroadcastWithAck(packet *parser.Packet, opts *socket.BroadcastOptions, clientCountCallback func(uint64), ack socket.Ack) {
	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local
	clientAck := ack
	if opts != nil && opts.Flags != nil && opts.Flags.ExpectSingleResponse {
		// BroadcastOperator keeps one value per client for multi-ack broadcasts,
		// but RemoteSocket expects the selected client's full acknowledgement
		// argument list. Preserve that list as the single aggregate value.
		clientAck = func(args []any, err error) {
			ack([]any{append([]any(nil), args...)}, err)
		}
	}
	if !onlyLocal {
		requestId := RandomId()

		c.ackRequests.Store(requestId, &ClusterAckRequest{
			ClientCountCallback: clientCountCallback,
			Ack:                 clientAck,
		})

		c.Publish(&ClusterMessage{
			Type: BROADCAST,
			Data: &BroadcastMessage{
				Packet:    packet,
				RequestId: &requestId,
				Opts:      EncodeOptions(opts),
			},
		})

		timeout := DEFAULT_TIMEOUT
		if opts != nil && opts.Flags != nil && opts.Flags.Timeout != nil {
			timeout = *opts.Flags.Timeout
		}

		// we have no way to know at this level whether the server has received an acknowledgement from each client, so we
		// will simply clean up the ackRequests map after the given delay
		utils.SetTimeout(func() {
			c.ackRequests.Delete(requestId)
		}, timeout)
	}

	c.Adapter.BroadcastWithAck(packet, opts, clientCountCallback, clientAck)
}

func (c *clusterAdapter) AddSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	if opts == nil || opts.Flags == nil || !opts.Flags.Local {
		_, err := c.PublishAndReturnOffset(&ClusterMessage{
			Type: SOCKETS_JOIN,
			Data: &SocketsJoinLeaveMessage{
				Opts:  EncodeOptions(opts),
				Rooms: rooms,
			},
		})
		if err != nil {
			adapterLog.Debug("[%s] error while publishing message: %s", c.uid, err.Error())
		}
	}
	c.Adapter.AddSockets(opts, rooms)
}

func (c *clusterAdapter) DelSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	if opts == nil || opts.Flags == nil || !opts.Flags.Local {
		_, err := c.PublishAndReturnOffset(&ClusterMessage{
			Type: SOCKETS_LEAVE,
			Data: &SocketsJoinLeaveMessage{
				Opts:  EncodeOptions(opts),
				Rooms: rooms,
			},
		})
		if err != nil {
			adapterLog.Debug("[%s] error while publishing message: %s", c.uid, err.Error())
		}
	}
	c.Adapter.DelSockets(opts, rooms)
}

func (c *clusterAdapter) DisconnectSockets(opts *socket.BroadcastOptions, state bool) {
	if opts == nil || opts.Flags == nil || !opts.Flags.Local {
		_, err := c.PublishAndReturnOffset(&ClusterMessage{
			Type: DISCONNECT_SOCKETS,
			Data: &DisconnectSocketsMessage{
				Opts:  EncodeOptions(opts),
				Close: state,
			},
		})
		if err != nil {
			adapterLog.Debug("[%s] error while publishing message: %s", c.uid, err.Error())
		}
	}
	c.Adapter.DisconnectSockets(opts, state)
}

func (c *clusterAdapter) FetchSockets(opts *socket.BroadcastOptions) func(func([]socket.SocketDetails, error)) {
	return func(callback func([]socket.SocketDetails, error)) {
		c.Adapter.FetchSockets(opts)(func(localSockets []socket.SocketDetails, _ error) {
			expectedResponseCount := c.Proto().ServerCount() - 1

			if (opts != nil && opts.Flags != nil && opts.Flags.Local) || expectedResponseCount <= 0 {
				callback(localSockets, nil)
				return
			}

			requestId := RandomId()

			t := DEFAULT_TIMEOUT
			if opts != nil && opts.Flags != nil && opts.Flags.Timeout != nil {
				t = *opts.Flags.Timeout
			}

			timeout := utils.SetTimeout(func() {
				if storedRequest, ok := c.requests.Load(requestId); ok {
					storedRequest.Once.Do(func() {
						callback(nil, fmt.Errorf("timeout reached: only %d responses received out of %d", storedRequest.Current.Load(), storedRequest.Expected))
						c.requests.Delete(requestId)
					})
				}
			}, t)

			c.requests.Store(requestId, &ClusterRequest{
				Type: FETCH_SOCKETS,
				Resolve: func(data *types.Slice[any]) {
					callback(slices.Map(data.All(), func(i any) socket.SocketDetails {
						return utils.TryCast[socket.SocketDetails](i)
					}), nil)
				},
				Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(t *atomic.Pointer[utils.Timer]) {
					t.Store(timeout)
				}),
				Current:  &atomic.Int64{},
				Expected: expectedResponseCount,
				Responses: types.NewSlice(slices.Map(localSockets, func(client socket.SocketDetails) any {
					return client
				})...),
			})

			c.Publish(&ClusterMessage{
				Type: FETCH_SOCKETS,
				Data: &FetchSocketsMessage{
					Opts:      EncodeOptions(opts),
					RequestId: requestId,
				},
			})
		})
	}
}

func (c *clusterAdapter) CountSockets(opts *socket.BroadcastOptions) func(func(uint64, error)) {
	return func(callback func(uint64, error)) {
		c.Adapter.CountSockets(opts)(func(localCount uint64, localErr error) {
			if localErr != nil {
				callback(0, localErr)
				return
			}
			expected := c.Proto().ServerCount() - 1
			if (opts != nil && opts.Flags != nil && opts.Flags.Local) || expected <= 0 {
				callback(localCount, nil)
				return
			}
			requestID := RandomId()
			timeoutDuration := DEFAULT_TIMEOUT
			if opts != nil && opts.Flags != nil && opts.Flags.Timeout != nil {
				timeoutDuration = *opts.Flags.Timeout
			}
			timeout := utils.SetTimeout(func() {
				if request, ok := c.requests.Load(requestID); ok {
					request.Once.Do(func() {
						callback(0, fmt.Errorf(
							"timeout reached: only %d responses received out of %d",
							request.Current.Load(),
							request.Expected,
						))
						c.requests.Delete(requestID)
					})
				}
			}, timeoutDuration)
			c.requests.Store(requestID, &ClusterRequest{
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
				Current:   &atomic.Int64{},
				Expected:  expected,
				Responses: types.NewSlice[any](),
			})
			c.Publish(&ClusterMessage{
				Type: COUNT_SOCKETS,
				Data: &CountSocketsMessage{Opts: EncodeOptions(opts), RequestId: requestID},
			})
		})
	}
}

func (c *clusterAdapter) ListRooms(opts *socket.BroadcastOptions) func(func(map[socket.Room]uint64, error)) {
	return func(callback func(map[socket.Room]uint64, error)) {
		c.Adapter.ListRooms(opts)(func(localRooms map[socket.Room]uint64, localErr error) {
			if localErr != nil {
				callback(nil, localErr)
				return
			}
			expected := c.Proto().ServerCount() - 1
			if (opts != nil && opts.Flags != nil && opts.Flags.Local) || expected <= 0 {
				callback(localRooms, nil)
				return
			}
			requestID := RandomId()
			timeoutDuration := DEFAULT_TIMEOUT
			if opts != nil && opts.Flags != nil && opts.Flags.Timeout != nil {
				timeoutDuration = *opts.Flags.Timeout
			}
			timeout := utils.SetTimeout(func() {
				if request, ok := c.requests.Load(requestID); ok {
					request.Once.Do(func() {
						callback(nil, fmt.Errorf(
							"timeout reached: only %d responses received out of %d",
							request.Current.Load(), request.Expected,
						))
						c.requests.Delete(requestID)
					})
				}
			}, timeoutDuration)
			c.requests.Store(requestID, &ClusterRequest{
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
				Current:   &atomic.Int64{},
				Expected:  expected,
				Responses: types.NewSlice[any](),
			})
			c.Publish(&ClusterMessage{
				Type: LIST_ROOMS,
				Data: &ListRoomsMessage{Opts: EncodeOptions(opts), RequestId: requestID},
			})
		})
	}
}

func (c *clusterAdapter) ServerSideEmit(packet []any) error {
	packetLen := len(packet)
	if packetLen == 0 {
		return fmt.Errorf("packet cannot be empty")
	}

	ack, withAck := packet[packetLen-1].(socket.Ack)
	if !withAck {
		c.Publish(&ClusterMessage{
			Type: SERVER_SIDE_EMIT,
			Data: &ServerSideEmitMessage{
				Packet: packet,
			},
		})
		return nil
	}

	expectedResponseCount := c.Proto().ServerCount() - 1
	adapterLog.Debug(`[%s] waiting for %d responses to "serverSideEmit" request`, c.uid, expectedResponseCount)

	if expectedResponseCount <= 0 {
		ack(nil, nil)
		return nil
	}

	requestId := RandomId()

	timeout := utils.SetTimeout(func() {
		if storedRequest, ok := c.requests.Load(requestId); ok {
			storedRequest.Once.Do(func() {
				ack(
					storedRequest.Responses.All(),
					fmt.Errorf(`timeout reached: only %d responses received out of %d`, storedRequest.Current.Load(), storedRequest.Expected),
				)
				c.requests.Delete(requestId)
			})
		}
	}, DEFAULT_TIMEOUT)

	c.requests.Store(requestId, &ClusterRequest{
		Type: SERVER_SIDE_EMIT,
		Resolve: func(data *types.Slice[any]) {
			ack(data.All(), nil)
		},
		Timeout: utils.Tap(&atomic.Pointer[utils.Timer]{}, func(t *atomic.Pointer[utils.Timer]) {
			t.Store(timeout)
		}),
		Current:   &atomic.Int64{},
		Expected:  expectedResponseCount,
		Responses: types.NewSlice[any](),
	})

	c.Publish(&ClusterMessage{
		Type: SERVER_SIDE_EMIT,
		Data: &ServerSideEmitMessage{
			RequestId: &requestId, // the presence of this attribute defines whether an acknowledgement is needed
			Packet:    packet[:packetLen-1],
		},
	})
	return nil
}

func (c *clusterAdapter) Publish(message *ClusterMessage) {
	if _, err := c.PublishAndReturnOffset(message); err != nil {
		adapterLog.Debug(`[%s] error while publishing message: %s`, c.uid, err.Error())
	}
}

func (c *clusterAdapter) PublishAndReturnOffset(message *ClusterMessage) (Offset, error) {
	startedAt := time.Now()
	message.Uid = c.uid
	message.Nsp = c.Nsp().Name()
	offset, err := c.Proto().(ClusterAdapter).DoPublish(message)
	c.Emit("adapter_operation", socket.AdapterTelemetryEvent{
		Operation: "publish",
		Namespace: c.Nsp().Name(),
		Duration:  time.Since(startedAt),
		Success:   err == nil,
		At:        time.Now(),
	})
	return offset, err
}

// Send a message to the other members of the cluster.
func (c *clusterAdapter) DoPublish(message *ClusterMessage) (Offset, error) {
	return "", errors.New("DoPublish() is not supported on parent ClusterAdapter")
}

func (c *clusterAdapter) PublishResponse(requesterUid ServerId, response *ClusterResponse) {
	startedAt := time.Now()
	response.Uid = c.uid
	response.Nsp = c.Nsp().Name()

	err := c.Proto().(ClusterAdapter).DoPublishResponse(requesterUid, response)
	c.Emit("adapter_operation", socket.AdapterTelemetryEvent{
		Operation: "response",
		Namespace: c.Nsp().Name(),
		Duration:  time.Since(startedAt),
		Success:   err == nil,
		At:        time.Now(),
	})
	if err != nil {
		adapterLog.Debug(`[%s] error while publishing response: %s`, c.uid, err.Error())
	}
}

// Send a response to the given member of the cluster.
func (c *clusterAdapter) DoPublishResponse(requesterUid ServerId, response *ClusterResponse) error {
	return errors.New("DoPublishResponse() is not supported on parent ClusterAdapter")
}
