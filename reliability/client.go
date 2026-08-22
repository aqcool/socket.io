package reliability

import (
	"sync"

	clientsocket "github.com/aqcool/socket.io/clients/socket/v4"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/aqcool/socket.io/v4/pkg/utils"
)

type Client struct {
	socket *clientsocket.Socket
	mu     sync.Mutex
	seen   map[string]struct{}
}

func BindClient(socket *clientsocket.Socket) (*Client, error) {
	client := &Client{socket: socket, seen: make(map[string]struct{})}
	if err := socket.On(DeliveryEvent, client.onDelivery); err != nil {
		return nil, err
	}
	return client, nil
}

// Emit sends a deduplicated client-to-server event. Reusing id is safe.
func (c *Client) Emit(id, name string, args ...any) error {
	if id == "" {
		id = utils.Base64Id().GenerateId()
	}
	return c.socket.Emit(PublishEvent, map[string]any{"id": id, "name": name, "args": args})
}

func (c *Client) onDelivery(args ...any) {
	event, ok := decodeEvent(first(args))
	if !ok || event.ID == "" || event.Name == "" {
		return
	}
	c.mu.Lock()
	_, duplicate := c.seen[event.ID]
	if !duplicate {
		c.seen[event.ID] = struct{}{}
	}
	c.mu.Unlock()
	if !duplicate {
		c.socket.EventEmitter.Emit(types.EventName(event.Name), event.Args...)
	}
	_ = c.socket.Emit(AckEvent, string(event.Offset))
}

func decodeEvent(value any) (Event, bool) {
	if event, ok := value.(Event); ok {
		return event, true
	}
	if event, ok := value.(*Event); ok && event != nil {
		return *event, true
	}
	data, ok := value.(map[string]any)
	if !ok {
		return Event{}, false
	}
	event := Event{
		ID:     stringValue(data["id"]),
		Offset: Offset(stringValue(data["offset"])),
		Name:   stringValue(data["name"]),
	}
	event.Args, _ = data["args"].([]any)
	return event, true
}
