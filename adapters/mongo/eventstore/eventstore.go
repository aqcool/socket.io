package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	socketmongo "github.com/aqcool/socket.io/adapters/mongo/v3"
	"github.com/aqcool/socket.io/reliability/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
	events *mongo.Collection
	acks   *mongo.Collection
	dedup  *mongo.Collection
}

type eventDocument struct {
	ID        bson.ObjectID      `bson:"_id,omitempty"`
	Target    reliability.Target `bson:"target"`
	Payload   []byte             `bson:"payload"`
	Size      int64              `bson:"size"`
	CreatedAt time.Time          `bson:"createdAt"`
	ExpiresAt *time.Time         `bson:"expiresAt,omitempty"`
}

func New(ctx context.Context, client *socketmongo.MongoClient, prefix string) (*Store, error) {
	if client == nil || client.Collection == nil {
		return nil, errors.New("mongo eventstore: client is required")
	}
	if prefix == "" {
		prefix = "socket_io_reliability"
	}
	db := client.Collection.Database()
	store := &Store{
		events: db.Collection(prefix + "_events"),
		acks:   db.Collection(prefix + "_acks"),
		dedup:  db.Collection(prefix + "_dedup"),
	}
	_, err := store.events.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "target.kind", Value: 1}, {Key: "target.namespace", Value: 1}, {Key: "target.id", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	})
	if err != nil {
		return nil, err
	}
	_, err = store.dedup.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expiresAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	return store, err
}

func (s *Store) Append(ctx context.Context, target reliability.Target, event *reliability.Event) (reliability.Offset, error) {
	event.Target = target
	payload, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	doc := eventDocument{Target: target, Payload: payload, Size: event.Size, CreatedAt: event.CreatedAt}
	if !event.ExpiresAt.IsZero() {
		doc.ExpiresAt = &event.ExpiresAt
	}
	result, err := s.events.InsertOne(ctx, doc)
	if err != nil {
		return "", err
	}
	id, ok := result.InsertedID.(bson.ObjectID)
	if !ok {
		return "", errors.New("mongo eventstore: inserted ID is not an ObjectID")
	}
	event.Offset = reliability.Offset(id.Hex())
	return event.Offset, nil
}

func (s *Store) Replay(ctx context.Context, target reliability.Target, after reliability.Offset, limit int) ([]reliability.Event, error) {
	filter := bson.D{
		{Key: "target.kind", Value: target.Kind},
		{Key: "target.namespace", Value: target.Namespace},
		{Key: "target.id", Value: target.ID},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "expiresAt", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "expiresAt", Value: bson.D{{Key: "$gt", Value: time.Now()}}}},
		}},
	}
	if after != "" {
		id, err := bson.ObjectIDFromHex(string(after))
		if err != nil {
			return nil, fmt.Errorf("mongo eventstore: invalid offset: %w", err)
		}
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: id}}})
	}
	findOptions := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	if limit > 0 {
		findOptions.SetLimit(int64(limit))
	}
	cursor, err := s.events.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	result := make([]reliability.Event, 0)
	for cursor.Next(ctx) {
		var doc eventDocument
		if err = cursor.Decode(&doc); err != nil {
			return nil, err
		}
		var event reliability.Event
		if err = json.Unmarshal(doc.Payload, &event); err != nil {
			return nil, err
		}
		event.Offset = reliability.Offset(doc.ID.Hex())
		result = append(result, event)
	}
	return result, cursor.Err()
}

func (s *Store) Ack(ctx context.Context, clientID string, offset reliability.Offset) error {
	id, err := bson.ObjectIDFromHex(string(offset))
	if err != nil {
		return err
	}
	_, err = s.acks.UpdateOne(ctx, bson.D{{Key: "_id", Value: clientID}},
		bson.D{{Key: "$max", Value: bson.D{{Key: "offset", Value: id}}}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) LastAck(ctx context.Context, clientID string) (reliability.Offset, error) {
	var doc struct {
		Offset bson.ObjectID `bson:"offset"`
	}
	err := s.acks.FindOne(ctx, bson.D{{Key: "_id", Value: clientID}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil
	}
	return reliability.Offset(doc.Offset.Hex()), err
}

func (s *Store) MarkInbound(ctx context.Context, clientID, eventID string, expiresAt time.Time) (bool, error) {
	id := clientID + "\x00" + eventID
	document := bson.D{{Key: "_id", Value: id}}
	if !expiresAt.IsZero() {
		document = append(document, bson.E{Key: "expiresAt", Value: expiresAt})
	}
	_, err := s.dedup.InsertOne(ctx, document)
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) Prune(ctx context.Context, limits reliability.Limits) error {
	conditions := bson.A{bson.D{{Key: "expiresAt", Value: bson.D{{Key: "$lte", Value: time.Now()}}}}}
	if limits.Retention > 0 {
		conditions = append(conditions, bson.D{{Key: "createdAt", Value: bson.D{{Key: "$lt", Value: time.Now().Add(-limits.Retention)}}}})
	}
	if _, err := s.events.DeleteMany(ctx, bson.D{{Key: "$or", Value: conditions}}); err != nil {
		return err
	}
	if limits.MaxEvents <= 0 && limits.MaxBytes <= 0 {
		return nil
	}
	cursor, err := s.events.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}))
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close(ctx) }()
	var keptEvents, keptBytes int64
	remove := make([]bson.ObjectID, 0)
	for cursor.Next(ctx) {
		var doc eventDocument
		if err = cursor.Decode(&doc); err != nil {
			return err
		}
		keptEvents++
		keptBytes += doc.Size
		if (limits.MaxEvents > 0 && keptEvents > limits.MaxEvents) ||
			(limits.MaxBytes > 0 && keptBytes > limits.MaxBytes) {
			remove = append(remove, doc.ID)
		}
	}
	if len(remove) > 0 {
		_, err = s.events.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: remove}}}})
	}
	return err
}

var _ reliability.EventStore = (*Store)(nil)
