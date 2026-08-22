// Package adapter provides a MongoDB-based adapter implementation for Socket.IO clustering.
// It uses MongoDB Change Streams for pub/sub communication between nodes.
// The document format is compatible with the Node.js @socket.io/mongo-adapter package,
// allowing mixed Go and Node.js deployments.
package adapter

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aqcool/socket.io/adapters/adapter/v4"
	"github.com/aqcool/socket.io/adapters/mongo/v4"
	"github.com/aqcool/socket.io/parsers/socket/v4/parser"
	"github.com/aqcool/socket.io/servers/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/log"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoLog is the logger for the MongoDB adapter.
var mongoLog = log.NewLog("socket.io-mongo")

// mongoAdapter implements the MongoAdapter interface using MongoDB Change Streams.
// It extends ClusterAdapterWithHeartbeat with MongoDB-specific functionality for
// message publishing and notification handling.
type mongoAdapter struct {
	adapter.ClusterAdapterWithHeartbeat

	mongoClient       *mongo.MongoClient
	opts              *MongoAdapterOptions
	addCreatedAtField bool
	cleanupFunc       types.Callable // Cleanup callback for resource management
}

type persistedSessionDocument struct {
	Kind      string                  `bson:"kind"`
	Pid       socket.PrivateSessionId `bson:"pid"`
	Nsp       string                  `bson:"nsp"`
	Payload   []byte                  `bson:"payload"`
	ExpiresAt time.Time               `bson:"expiresAt"`
}

const mongoSessionEventType adapter.MessageType = 13

type mongoSessionData struct {
	Sid       socket.SocketId         `bson:"sid"`
	Pid       socket.PrivateSessionId `bson:"pid"`
	Rooms     []socket.Room           `bson:"rooms"`
	Data      any                     `bson:"data"`
	Tombstone bool                    `bson:"tombstone,omitempty"`
}

type officialSessionDocument struct {
	Data mongoSessionData `bson:"data"`
}

func (a *mongoAdapter) SupportsConnectionStateRecovery() bool { return true }

// MakeMongoAdapter creates a new uninitialized mongoAdapter.
// Call Construct() to complete initialization before use.
func MakeMongoAdapter() MongoAdapter {
	clusterAdapter := adapter.MakeClusterAdapterWithHeartbeat()
	clusterAdapter.SetRequestsTimeout(DefaultRequestsTimeout)
	clusterAdapter.SetResponseTimeoutErrorFormatter(func(received, expected int) error {
		return fmt.Errorf("timeout reached: only %d responses received out of %d", received, expected)
	})

	a := &mongoAdapter{
		ClusterAdapterWithHeartbeat: clusterAdapter,
		opts:                        DefaultMongoAdapterOptions(),
		cleanupFunc:                 nil,
	}

	a.Prototype(a)

	return a
}

// NewMongoAdapter creates and initializes a new MongoDB adapter.
// This is the preferred way to create a MongoDB adapter instance.
func NewMongoAdapter(nsp socket.Namespace, client *mongo.MongoClient, opts any) MongoAdapter {
	a := MakeMongoAdapter()

	a.SetMongo(client)
	a.SetOpts(opts)
	a.Construct(nsp)

	return a
}

// SetMongo sets the MongoDB client for the adapter.
func (a *mongoAdapter) SetMongo(client *mongo.MongoClient) {
	a.mongoClient = client
}

// SetOpts sets the configuration options for the adapter.
// Options are merged with the parent ClusterAdapterWithHeartbeat options.
func (a *mongoAdapter) SetOpts(opts any) {
	a.ClusterAdapterWithHeartbeat.SetOpts(opts)

	if options, ok := opts.(MongoAdapterOptionsInterface); ok {
		a.opts.Assign(options)
		a.addCreatedAtField = options.AddCreatedAtField()
		if options.GetRawRequestsTimeout() != nil {
			a.SetRequestsTimeout(options.RequestsTimeout())
		}
	}
}

// Construct initializes the MongoDB adapter for the given namespace.
// This method must be called before using the adapter.
func (a *mongoAdapter) Construct(nsp socket.Namespace) {
	a.ClusterAdapterWithHeartbeat.Construct(nsp)
}

// DoPublish publishes a cluster message to other nodes by inserting a document into MongoDB.
// Returns the hex-encoded ObjectID of the inserted document as the offset,
// matching the Node.js adapter's publish() return value.
func (a *mongoAdapter) DoPublish(message *ClusterMessage) (adapter.Offset, error) {
	mongoLog.Debug("publishing message of type %d", message.Type)
	// Binary ACKs decoded from a Socket.IO client are represented by the
	// repository's BufferInterface. The MongoDB BSON encoder would otherwise
	// serialize its implementation fields as {buffer: {}}, while the official
	// Node adapter stores a BSON Binary value. Normalize every binary-bearing
	// message before insertion so Node and Go peers both receive real binary.
	normalizeMongoMessage(message.Data)

	doc := bson.D{
		{Key: "uid", Value: string(message.Uid)},
		{Key: "nsp", Value: message.Nsp},
		{Key: "type", Value: message.Type},
	}

	if message.Data != nil {
		doc = append(doc, bson.E{Key: "data", Value: message.Data})
	}

	if a.addCreatedAtField {
		doc = append(doc, bson.E{Key: "createdAt", Value: time.Now()})
	}
	if a.isRecoverableBroadcast(message) {
		doc = append(doc, bson.E{Key: "expiresAt", Value: time.Now().Add(a.maxDisconnectionDuration())})
	}

	result, err := a.mongoClient.Collection.InsertOne(a.mongoClient.Context, doc)
	if err != nil {
		return "", err
	}

	// Convert the inserted ID to hex string offset, matching Node.js:
	// result.insertedId.toString("hex")
	if oid, ok := result.InsertedID.(bson.ObjectID); ok {
		return adapter.Offset(oid.Hex()), nil
	}

	return "", nil
}

func (a *mongoAdapter) isRecoverableBroadcast(message *ClusterMessage) bool {
	if a.Nsp().Server().Opts().ConnectionStateRecovery() == nil || message.Type != adapter.BROADCAST {
		return false
	}
	data, ok := message.Data.(*BroadcastMessage)
	if !ok || data.Packet == nil {
		return false
	}
	return data.Packet.Type == parser.EVENT &&
		data.Packet.Id == nil &&
		(data.Opts == nil || data.Opts.Flags == nil || !data.Opts.Flags.Volatile)
}

func (a *mongoAdapter) maxDisconnectionDuration() time.Duration {
	recovery := a.Nsp().Server().Opts().ConnectionStateRecovery()
	if recovery == nil {
		return 0
	}
	return time.Duration(recovery.MaxDisconnectionDuration()) * time.Millisecond
}

// PersistSession stores the disconnected session with an absolute expiry.
func (a *mongoAdapter) PersistSession(session *socket.SessionToPersist) {
	doc := bson.D{
		{Key: "uid", Value: string(a.Uid())},
		{Key: "nsp", Value: a.Nsp().Name()},
		{Key: "type", Value: mongoSessionEventType},
		{Key: "data", Value: &mongoSessionData{
			Sid:   session.Sid,
			Pid:   session.Pid,
			Rooms: session.Rooms.Keys(),
			Data:  session.Data,
		}},
		{Key: "expiresAt", Value: time.Now().Add(a.maxDisconnectionDuration())},
	}
	if a.addCreatedAtField {
		doc = append(doc, bson.E{Key: "createdAt", Value: time.Now()})
	}
	_, err := a.mongoClient.Collection.InsertOne(a.mongoClient.Context, doc)
	if err != nil {
		a.mongoClient.Emit("error", fmt.Errorf("persist recovery session: %w", err))
	}
}

// RestoreSession loads a non-expired session and all matching broadcasts after
// the client's last MongoDB ObjectID offset.
func (a *mongoAdapter) RestoreSession(pid socket.PrivateSessionId, offset string) (*socket.Session, error) {
	objectID, err := bson.ObjectIDFromHex(offset)
	if err != nil {
		return nil, fmt.Errorf("invalid recovery offset: %w", err)
	}

	count, err := a.mongoClient.Collection.CountDocuments(a.mongoClient.Context, bson.D{
		{Key: "_id", Value: objectID},
		{Key: "nsp", Value: a.Nsp().Name()},
		{Key: "type", Value: adapter.BROADCAST},
	})
	if err != nil {
		return nil, fmt.Errorf("verify recovery offset: %w", err)
	}
	if count == 0 {
		return nil, nil
	}

	var official officialSessionDocument
	sessionFilter := bson.D{
		{Key: "type", Value: mongoSessionEventType},
		{Key: "nsp", Value: a.Nsp().Name()},
		{Key: "data.pid", Value: pid},
	}
	if a.addCreatedAtField {
		err = a.mongoClient.Collection.FindOneAndDelete(
			a.mongoClient.Context,
			sessionFilter,
			options.FindOneAndDelete().SetSort(bson.D{{Key: "_id", Value: -1}}),
		).Decode(&official)
	} else {
		err = a.mongoClient.Collection.FindOne(
			a.mongoClient.Context,
			sessionFilter,
			options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
		).Decode(&official)
	}
	if errors.Is(err, mongod.ErrNoDocuments) {
		return a.restoreLegacySession(pid, objectID)
	}
	if err != nil {
		return nil, fmt.Errorf("restore recovery session: %w", err)
	}
	if official.Data.Tombstone || official.Data.Sid == "" {
		return nil, nil
	}
	if !a.addCreatedAtField {
		// Capped collections cannot delete documents. Match the official
		// adapter by appending a tombstone so the same session cannot be
		// restored a second time.
		tombstone := bson.D{
			{Key: "uid", Value: string(a.Uid())},
			{Key: "nsp", Value: a.Nsp().Name()},
			{Key: "type", Value: mongoSessionEventType},
			{Key: "data", Value: &mongoSessionData{Pid: pid, Tombstone: true}},
			{Key: "expiresAt", Value: time.Now().Add(a.maxDisconnectionDuration())},
		}
		if _, insertErr := a.mongoClient.Collection.InsertOne(a.mongoClient.Context, tombstone); insertErr != nil {
			return nil, fmt.Errorf("persist recovery tombstone: %w", insertErr)
		}
	}
	session := &socket.Session{SessionToPersist: &socket.SessionToPersist{
		Sid:   official.Data.Sid,
		Pid:   official.Data.Pid,
		Rooms: types.NewSet(official.Data.Rooms...),
		Data:  normalizeMongoValue(official.Data.Data),
	}}
	return a.appendMissedPackets(session, objectID)
}

func (a *mongoAdapter) restoreLegacySession(pid socket.PrivateSessionId, objectID bson.ObjectID) (*socket.Session, error) {
	var stored persistedSessionDocument
	err := a.mongoClient.Collection.FindOneAndDelete(a.mongoClient.Context, bson.D{
		{Key: "kind", Value: "session"},
		{Key: "nsp", Value: a.Nsp().Name()},
		{Key: "pid", Value: pid},
		{Key: "expiresAt", Value: bson.D{{Key: "$gt", Value: time.Now()}}},
	}).Decode(&stored)
	if errors.Is(err, mongod.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("restore legacy recovery session: %w", err)
	}
	session := &socket.Session{}
	if decodeErr := utils.MsgPack().Decode(stored.Payload, &session.SessionToPersist); decodeErr != nil {
		return nil, fmt.Errorf("decode legacy recovery session: %w", decodeErr)
	}
	return a.appendMissedPackets(session, objectID)
}

func (a *mongoAdapter) appendMissedPackets(session *socket.Session, objectID bson.ObjectID) (*socket.Session, error) {
	cursor, err := a.mongoClient.Collection.Find(
		a.mongoClient.Context,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$gt", Value: objectID}}},
			{Key: "nsp", Value: a.Nsp().Name()},
			{Key: "type", Value: adapter.BROADCAST},
		},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("query missed packets: %w", err)
	}
	defer func() { _ = cursor.Close(a.mongoClient.Context) }()

	for cursor.Next(a.mongoClient.Context) {
		var event mongo.AdapterEvent
		if err := cursor.Decode(&event); err != nil {
			return nil, fmt.Errorf("decode missed packet: %w", err)
		}
		decoded, err := a.decodeBsonData(event.Type, event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode missed broadcast: %w", err)
		}
		broadcast, ok := decoded.(*BroadcastMessage)
		if !ok || broadcast.Packet == nil {
			continue
		}
		if socket.ShouldIncludePacket(session.Rooms, adapter.DecodeOptions(broadcast.Opts)) {
			packetData := append(mongoPacketData(broadcast.Packet.Data), event.ID.Hex())
			session.MissedPackets = append(session.MissedPackets, packetData)
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate missed packets: %w", err)
	}

	return session, nil
}

func mongoPacketData(data any) []any {
	switch value := data.(type) {
	case []any:
		return value
	case bson.A:
		return []any(value)
	default:
		return nil
	}
}

func normalizeMongoValue(value any) any {
	switch typed := value.(type) {
	case types.BufferInterface:
		if typed == nil || (reflect.ValueOf(typed).Kind() == reflect.Ptr && reflect.ValueOf(typed).IsNil()) {
			return nil
		}
		return append([]byte(nil), typed.Bytes()...)
	case bson.D:
		result := make(map[string]any, len(typed))
		for _, element := range typed {
			result[element.Key] = normalizeMongoValue(element.Value)
		}
		return result
	case bson.M:
		result := make(map[string]any, len(typed))
		for key, element := range typed {
			result[key] = normalizeMongoValue(element)
		}
		return result
	case bson.A:
		result := make([]any, len(typed))
		for index, element := range typed {
			result[index] = normalizeMongoValue(element)
		}
		return result
	case bson.Binary:
		return append([]byte(nil), typed.Data...)
	}

	// BSON decoding commonly returns bson.A/bson.M, while application-provided
	// Packet.Data often contains ordinary or named Go slices and maps. Walk all
	// string-keyed maps and non-byte sequences so deeply nested BufferInterface
	// values are encoded as BSON Binary instead of implementation structs. Byte
	// slices (including bson.Raw aliases) and BSON structs remain untouched.
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice:
		if reflected.IsNil() || reflected.Type().Elem().Kind() == reflect.Uint8 {
			return value
		}
		result := make([]any, reflected.Len())
		for index := range reflected.Len() {
			result[index] = normalizeMongoValue(reflected.Index(index).Interface())
		}
		return result
	case reflect.Map:
		if reflected.IsNil() || reflected.Type().Key().Kind() != reflect.String {
			return value
		}
		result := make(map[string]any, reflected.Len())
		iterator := reflected.MapRange()
		for iterator.Next() {
			result[iterator.Key().String()] = normalizeMongoValue(iterator.Value().Interface())
		}
		return result
	default:
		return value
	}
}

// DoPublishResponse publishes a response message to the cluster.
// This is used for request-response patterns between nodes.
func (a *mongoAdapter) DoPublishResponse(requesterUid adapter.ServerId, response *ClusterResponse) error {
	_, err := a.DoPublish(response)
	return err
}

// OnEvent processes a change stream document from MongoDB.
// It decodes the BSON data based on message type and routes it to OnMessage.
func (a *mongoAdapter) OnEvent(document *mongo.AdapterEvent) {
	// The change stream pipeline already filters by operationType=insert,
	// but we still check uid to filter out our own messages.
	if document.Uid == a.Uid() {
		return
	}

	mongoLog.Debug("new event of type %d from %s", document.Type, document.Uid)

	// @socket.io/mongo-adapter@0.4.0 reserves type 13 for persisted
	// sessions. The newer generic cluster protocol uses the same numeric value
	// for ADAPTER_CLOSE, so Mongo session documents must never be dispatched to
	// ClusterAdapterWithHeartbeat as node-close messages.
	if document.Type == adapter.ADAPTER_CLOSE {
		return
	}

	// Build ClusterMessage with decoded data
	message := &ClusterMessage{
		Uid:  document.Uid,
		Nsp:  document.Nsp,
		Type: document.Type,
	}

	// Decode the data field based on message type (two-pass decoding)
	if document.Data.Type != 0 {
		data, err := a.decodeBsonData(document.Type, document.Data)
		if err != nil {
			mongoLog.Debug("failed to decode data for type %d: %s", document.Type, err.Error())
			return
		}
		message.Data = data
	}

	// The offset is the hex string of the ObjectID, matching Node.js:
	// result.insertedId.toString("hex")
	offset := adapter.Offset(document.ID.Hex())
	a.OnMessage(message, offset)
}

// Cleanup registers a cleanup callback to be called when the adapter is closed.
func (a *mongoAdapter) Cleanup(cleanup func()) {
	a.cleanupFunc = cleanup
}

// Close releases resources and invokes the registered cleanup callback.
func (a *mongoAdapter) Close() {
	defer a.CloseLocal()

	if a.cleanupFunc != nil {
		a.cleanupFunc()
	}
}

// buildChangeStreamPipeline creates the MongoDB aggregation pipeline for the Change Stream.
// Unlike the Node.js version which filters by uid != self.uid, we handle self-message
// filtering in OnMessage() to avoid pipeline rebuilds on uid changes.
func buildChangeStreamPipeline() mongod.Pipeline {
	return mongod.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: "insert"},
			{Key: "fullDocument.uid", Value: bson.D{{Key: "$exists", Value: true}}},
		}}},
	}
}

// decodeBsonData deserializes a BSON data value based on the message type.
func (a *mongoAdapter) decodeBsonData(messageType adapter.MessageType, rawData bson.RawValue) (any, error) {
	target := allocateTarget(messageType)
	if target == nil {
		return nil, nil
	}

	if err := rawData.Unmarshal(target); err != nil {
		return nil, err
	}
	normalizeMongoMessage(target)

	return target, nil
}

func normalizeMongoMessage(message any) {
	switch typed := message.(type) {
	case *BroadcastMessage:
		if typed.Packet != nil {
			typed.Packet.Data = normalizeMongoValue(typed.Packet.Data)
		}
	case *ServerSideEmitMessage:
		for index := range typed.Packet {
			typed.Packet[index] = normalizeMongoValue(typed.Packet[index])
		}
	case *ServerSideEmitResponse:
		typed.Packet = normalizeMongoValue(typed.Packet)
	case *BroadcastAck:
		typed.Packet = normalizeMongoValue(typed.Packet)
	case *FetchSocketsResponse:
		for _, remote := range typed.Sockets {
			if remote != nil {
				remote.Data = normalizeMongoValue(remote.Data)
			}
		}
	}
}

// allocateTarget returns a pointer to the appropriate struct for the given message type.
func allocateTarget(messageType adapter.MessageType) any {
	switch messageType {
	case adapter.INITIAL_HEARTBEAT, adapter.HEARTBEAT, adapter.ADAPTER_CLOSE:
		return nil
	case adapter.BROADCAST:
		return &BroadcastMessage{}
	case adapter.SOCKETS_JOIN, adapter.SOCKETS_LEAVE:
		return &SocketsJoinLeaveMessage{}
	case adapter.DISCONNECT_SOCKETS:
		return &DisconnectSocketsMessage{}
	case adapter.FETCH_SOCKETS:
		return &FetchSocketsMessage{}
	case adapter.FETCH_SOCKETS_RESPONSE:
		return &FetchSocketsResponse{}
	case adapter.COUNT_SOCKETS:
		return &adapter.CountSocketsMessage{}
	case adapter.COUNT_SOCKETS_RESPONSE:
		return &adapter.CountSocketsResponse{}
	case adapter.LIST_ROOMS:
		return &adapter.ListRoomsMessage{}
	case adapter.LIST_ROOMS_RESPONSE:
		return &adapter.ListRoomsResponse{}
	case adapter.SERVER_SIDE_EMIT:
		return &ServerSideEmitMessage{}
	case adapter.SERVER_SIDE_EMIT_RESPONSE:
		return &ServerSideEmitResponse{}
	case adapter.BROADCAST_CLIENT_COUNT:
		return &BroadcastClientCount{}
	case adapter.BROADCAST_ACK:
		return &BroadcastAck{}
	default:
		return nil
	}
}
