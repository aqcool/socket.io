package adapter

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aqcool/socket.io/servers/socket/v4"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func assertOfficial040ServerSideDelivery(
	t *testing.T,
	source *socket.Server,
	target *socket.Server,
	event string,
) {
	t.Helper()

	received := make(chan string, 8)
	if err := target.On(event, func(args ...any) {
		if len(args) > 0 {
			received <- fmt.Sprint(args[0])
		}
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		token := fmt.Sprintf("%s-%d", event, attempt)
		if err := source.ServerSideEmit(event, token); err != nil {
			t.Fatal(err)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case actual := <-received:
			timer.Stop()
			if actual == token {
				return
			}
		case <-timer.C:
		}
	}
	t.Fatalf("event %q was not delivered after the change stream restarted", event)
}

// TestOfficialMongoAdapter040ChangeStreamLifecycle maps the two MongoDB-specific
// executable tests at the end of @socket.io/mongo-adapter@0.4.0/test/index.ts.
// A replica-set stepdown is the Go driver's equivalent of closing and
// reconnecting the Node MongoClient: all pooled connections and active change
// streams must reconnect before cross-node delivery resumes.
func TestOfficialMongoAdapter040ChangeStreamLifecycle(t *testing.T) {
	uri := os.Getenv("SOCKET_IO_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("SOCKET_IO_MONGO_TEST_URI is not set")
	}

	client, err := mongod.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(t.Context()) })
	if err = client.Ping(t.Context(), readpref.Primary()); err != nil {
		t.Fatal(err)
	}

	database := client.Database(fmt.Sprintf("socket_io_official_lifecycle_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = database.Drop(t.Context()) })

	t.Run("should not throw when receiving a drop event", func(t *testing.T) {
		fixture := newOfficial040MongoRecoveryFixture(t, database, "events-drop", true)
		assertOfficial040ServerSideDelivery(t, fixture.sourceServer, fixture.targetServer, "before-drop")

		if err := fixture.collection.Drop(t.Context()); err != nil {
			t.Fatal(err)
		}
		// The official assertion only waits for the invalidation event and
		// checks that it does not surface as an uncaught error. We additionally
		// prove that the Go watch loop opens a new stream and delivers again.
		time.Sleep(1100 * time.Millisecond)
		assertOfficial040ServerSideDelivery(t, fixture.sourceServer, fixture.targetServer, "after-drop")
	})

	t.Run("should resume the change stream upon reconnection", func(t *testing.T) {
		if os.Getenv("SOCKET_IO_MONGO_STEPDOWN_TEST") != "1" {
			t.Skip("set SOCKET_IO_MONGO_STEPDOWN_TEST=1 on a dedicated replica set")
		}
		fixture := newOfficial040MongoRecoveryFixture(t, database, "events-reconnect", true)
		assertOfficial040ServerSideDelivery(t, fixture.sourceServer, fixture.targetServer, "before-reconnect")

		// A one-second forced stepdown closes the active getMore operations.
		// The command itself may return a network/not-primary error once the
		// server steps down, so readiness is established with Ping below.
		_ = client.Database("admin").RunCommand(t.Context(), bson.D{
			{Key: "replSetStepDown", Value: 1},
			{Key: "force", Value: true},
		}).Err()

		deadline := time.Now().Add(15 * time.Second)
		for {
			if pingErr := client.Ping(t.Context(), readpref.Primary()); pingErr == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("MongoDB replica set did not elect a primary after stepdown")
			}
			time.Sleep(100 * time.Millisecond)
		}

		assertOfficial040ServerSideDelivery(t, fixture.sourceServer, fixture.targetServer, "after-reconnect-forward")
		assertOfficial040ServerSideDelivery(t, fixture.targetServer, fixture.sourceServer, "after-reconnect-reverse")
	})
}
