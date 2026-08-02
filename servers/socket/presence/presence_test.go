package presence

import (
	"testing"
	"time"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func TestClusterQueriesAndMultiDeviceUserMapping(t *testing.T) {
	io := socket.NewServer(nil, nil)
	tracker, err := New(io, &Options{CleanupInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Close)
	nsp := io.Of("/app", nil)
	userRoom := tracker.userRoom("user-1")
	nsp.Adapter().AddAll("socket-1", types.NewSet(userRoom, socket.Room("lobby")))
	nsp.Adapter().AddAll("socket-2", types.NewSet(userRoom, socket.Room("lobby")))

	tracker.CountSockets("/app")(func(count uint64, countErr error) {
		if countErr != nil || count != 2 {
			t.Fatalf("CountSockets = (%d, %v), want (2, nil)", count, countErr)
		}
	})
	tracker.CountRoom("/app", "lobby")(func(count uint64, countErr error) {
		if countErr != nil || count != 2 {
			t.Fatalf("CountRoom = (%d, %v), want (2, nil)", count, countErr)
		}
	})
	tracker.IsUserOnline("/app", "user-1")(func(online bool, onlineErr error) {
		if onlineErr != nil || !online {
			t.Fatalf("IsUserOnline = (%v, %v), want (true, nil)", online, onlineErr)
		}
	})
}

func TestRoomEventsMetadataAndEmptyTTL(t *testing.T) {
	io := socket.NewServer(nil, nil)
	tracker, err := New(io, &Options{
		EmptyRoomTTL:    50 * time.Millisecond,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Close)
	nsp := io.Of("/app", nil)
	changes := make(chan RoomCountChange, 2)
	expired := make(chan RoomStats, 1)
	_ = tracker.On("room_count_changed", func(args ...any) {
		changes <- args[0].(RoomCountChange)
	})
	_ = tracker.On("room_expired", func(args ...any) {
		expired <- args[0].(RoomStats)
	})

	if err = tracker.SetRoomMetadata("/app", "lobby", map[string]any{"title": "大厅"}); err != nil {
		t.Fatal(err)
	}
	nsp.Adapter().AddAll("socket-1", types.NewSet(socket.Room("lobby")))
	joined := waitChange(t, changes)
	if joined.Count != 1 || joined.Delta != 1 {
		t.Fatalf("join change = %+v", joined)
	}

	tracker.RoomStats("/app", "lobby")(func(stats RoomStats, statsErr error) {
		if statsErr != nil {
			t.Fatal(statsErr)
		}
		metadata := stats.Metadata.(map[string]any)
		if stats.Count != 1 || metadata["title"] != "大厅" {
			t.Fatalf("stats = %+v", stats)
		}
	})

	nsp.Adapter().DelAll("socket-1")
	left := waitChange(t, changes)
	if left.Count != 0 || left.Delta != -1 {
		t.Fatalf("leave change = %+v", left)
	}

	tracker.ListRooms("/app")(func(rooms []RoomStats, listErr error) {
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(rooms) != 1 || rooms[0].Room != "lobby" || rooms[0].EmptySince == nil {
			t.Fatalf("rooms = %+v", rooms)
		}
	})
	tracker.cleanup(time.Now().Add(time.Second))
	select {
	case stats := <-expired:
		if stats.Room != "lobby" {
			t.Fatalf("expired room = %q", stats.Room)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for room_expired")
	}
}

func TestListRoomsHidesInternalUserRooms(t *testing.T) {
	io := socket.NewServer(nil, nil)
	tracker, err := New(io, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Close)
	nsp := io.Of("/app", nil)
	nsp.Adapter().AddAll("socket-1", types.NewSet(
		socket.Room("visible"),
		tracker.userRoom("user-1"),
	))

	tracker.ListRooms("/app")(func(rooms []RoomStats, listErr error) {
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(rooms) != 1 || rooms[0].Room != "visible" {
			t.Fatalf("rooms = %+v", rooms)
		}
	})
}

func waitChange(t *testing.T, changes <-chan RoomCountChange) RoomCountChange {
	t.Helper()
	select {
	case change := <-changes:
		return change
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for room_count_changed")
		return RoomCountChange{}
	}
}
