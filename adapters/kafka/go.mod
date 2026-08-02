module github.com/aqcool/socket.io/adapters/kafka/v3

go 1.26.0

require (
	github.com/IBM/sarama v1.45.1
	github.com/aqcool/socket.io/adapters/broker/v3 v3.0.4
)

require (
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/aqcool/socket.io/adapters/adapter/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/parsers/engine/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/parsers/socket/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/servers/engine/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/servers/socket/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/v3 v3.0.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gookit/color v1.6.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.60.0 // indirect
	github.com/quic-go/webtransport-go v0.10.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
)

replace (
	github.com/aqcool/socket.io/adapters/adapter/v3 => ../adapter
	github.com/aqcool/socket.io/adapters/broker/v3 => ../broker
	github.com/aqcool/socket.io/clients/engine/v3 => ../../clients/engine
	github.com/aqcool/socket.io/clients/socket/v3 => ../../clients/socket
	github.com/aqcool/socket.io/parsers/engine/v3 => ../../parsers/engine
	github.com/aqcool/socket.io/parsers/socket/v3 => ../../parsers/socket
	github.com/aqcool/socket.io/servers/engine/v3 => ../../servers/engine
	github.com/aqcool/socket.io/servers/socket/v3 => ../../servers/socket
	github.com/aqcool/socket.io/v3 => ../../
)
