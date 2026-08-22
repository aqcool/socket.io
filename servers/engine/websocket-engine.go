package engine

import (
	"net/http"

	"github.com/aqcool/socket.io/servers/engine/v4/config"
	"github.com/aqcool/socket.io/v4/pkg/types"
	"github.com/gorilla/websocket"
)

// GorillaWebSocketEngine is the default WebSocket upgrader.
type GorillaWebSocketEngine struct{}

func (*GorillaWebSocketEngine) Upgrade(
	writer http.ResponseWriter,
	request *http.Request,
	headers http.Header,
	options config.WebSocketUpgradeOptions,
) (types.WebSocketConnection, error) {
	upgrader := &websocket.Upgrader{
		ReadBufferSize:    options.ReadBufferSize,
		WriteBufferSize:   options.WriteBufferSize,
		EnableCompression: options.EnableCompression,
		CheckOrigin:       options.CheckOrigin,
		Error:             options.Error,
	}
	return upgrader.Upgrade(writer, request, headers)
}

var _ config.WebSocketEngine = (*GorillaWebSocketEngine)(nil)
