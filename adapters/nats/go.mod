module github.com/aqcool/socket.io/adapters/nats/v4

go 1.26.0

require (
	github.com/aqcool/socket.io/adapters/broker/v4 v4.0.0
	github.com/nats-io/nats.go v1.34.1
)

require (
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/aqcool/socket.io/adapters/adapter/v4 v4.0.0 // indirect
	github.com/aqcool/socket.io/parsers/engine/v4 v4.0.0 // indirect
	github.com/aqcool/socket.io/parsers/socket/v4 v4.0.0 // indirect
	github.com/aqcool/socket.io/servers/engine/v4 v4.0.0 // indirect
	github.com/aqcool/socket.io/servers/socket/v4 v4.0.0 // indirect
	github.com/aqcool/socket.io/v4 v4.0.0 // indirect
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/gookit/color v1.6.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.60.0 // indirect
	github.com/quic-go/webtransport-go v0.10.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
)

replace (
	github.com/aqcool/socket.io/adapters/adapter/v4 => ../adapter
	github.com/aqcool/socket.io/adapters/broker/v4 => ../broker
	github.com/aqcool/socket.io/clients/engine/v4 => ../../clients/engine
	github.com/aqcool/socket.io/clients/socket/v4 => ../../clients/socket
	github.com/aqcool/socket.io/parsers/engine/v4 => ../../parsers/engine
	github.com/aqcool/socket.io/parsers/socket/v4 => ../../parsers/socket
	github.com/aqcool/socket.io/servers/engine/v4 => ../../servers/engine
	github.com/aqcool/socket.io/servers/socket/v4 => ../../servers/socket
	github.com/aqcool/socket.io/v4 => ../..
)

replace github.com/aqcool/socket.io/adapters/amqp/v4 => ../amqp

replace github.com/aqcool/socket.io/adapters/kafka/v4 => ../kafka

replace github.com/aqcool/socket.io/adapters/mongo/v4 => ../mongo

replace github.com/aqcool/socket.io/adapters/postgres/v4 => ../postgres

replace github.com/aqcool/socket.io/adapters/redis/v4 => ../redis

replace github.com/aqcool/socket.io/adapters/unix/v4 => ../unix

replace github.com/aqcool/socket.io/adapters/valkey/v4 => ../valkey

replace github.com/aqcool/socket.io/examples/basic-crud-application => ../../examples/basic-crud-application

replace github.com/aqcool/socket.io/examples/benchmark => ../../examples/benchmark

replace github.com/aqcool/socket.io/examples/chat => ../../examples/chat

replace github.com/aqcool/socket.io/examples/middleware-auth => ../../examples/middleware-auth

replace github.com/aqcool/socket.io/examples/test-suite => ../../examples/test-suite

replace github.com/aqcool/socket.io/examples/unix-adapter-debug => ../../examples/unix-adapter-debug

replace github.com/aqcool/socket.io/instrumentation/v4 => ../../instrumentation

replace github.com/aqcool/socket.io/observability/v4 => ../../observability

replace github.com/aqcool/socket.io/reliability/v4 => ../../reliability

replace github.com/aqcool/socket.io/sticky/v4 => ../../sticky

replace github.com/aqcool/socket.io/typed/v4 => ../../typed
