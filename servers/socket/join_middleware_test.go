package socket

import (
	"errors"
	"testing"
)

func TestJoinMiddlewareRejectsBeforeAdapter(t *testing.T) {
	client := MakeSocket()
	expected := errors.New("room limit")
	client.UseJoinMiddleware(func(_ *Socket, rooms []Room) error {
		if len(rooms) == 1 && rooms[0] == "blocked" {
			return expected
		}
		return nil
	})
	if err := client.TryJoin("blocked"); !errors.Is(err, expected) {
		t.Fatalf("unexpected error %v", err)
	}
}
