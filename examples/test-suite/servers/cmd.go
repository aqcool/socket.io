package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aqcool/socket.io/servers/socket/v3"
	"github.com/aqcool/socket.io/v3/pkg/log"
	"github.com/aqcool/socket.io/v3/pkg/types"
)

func Socket(addr string) *socket.Server {
	config := socket.DefaultServerOptions()
	config.SetPingInterval(300 * time.Millisecond)
	config.SetPingTimeout(200 * time.Millisecond)
	config.SetMaxHttpBufferSize(1000000)
	config.SetConnectTimeout(1000 * time.Millisecond)
	config.SetCors(&types.Cors{
		Origin: "*",
	})

	httpServer := types.NewWebServer(nil)
	io := socket.NewServer(httpServer, config)

	httpServer.Listen(addr, nil)

	return io
}

func main() {
	log.DEBUG.Store(true)

	io := Socket(":3000")

	io.On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*socket.Socket)
		if !ok {
			return
		}

		defer client.Emit("auth", client.Handshake().Auth)

		client.On("message", func(args ...any) {
			client.Emit("message-back", args...)
		})

		client.On("message-with-ack", func(args ...any) {
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(socket.Ack); ok {
					ack(args[:len(args)-1], nil)
				}
			}
		})
	})

	io.Of("/custom", nil).On("connection", func(clients ...any) {
		if len(clients) == 0 {
			return
		}
		client, ok := clients[0].(*socket.Socket)
		if !ok {
			return
		}
		defer client.Emit("auth", client.Handshake().Auth)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	<-ctx.Done()
	io.Close(nil)
}
