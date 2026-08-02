module github.com/aqcool/socket.io/observability/v3

go 1.26.0

require (
	github.com/aqcool/socket.io/clients/socket/v3 v3.0.4
	github.com/aqcool/socket.io/servers/socket/v3 v3.0.4
	github.com/aqcool/socket.io/v3 v3.0.4
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	go.opentelemetry.io/otel v1.34.0
	go.opentelemetry.io/otel/sdk v1.34.0
	go.opentelemetry.io/otel/trace v1.34.0
)

require (
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/aqcool/socket.io/clients/engine/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/parsers/engine/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/parsers/socket/v3 v3.0.4 // indirect
	github.com/aqcool/socket.io/servers/engine/v3 v3.0.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gookit/color v1.6.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.60.0 // indirect
	github.com/quic-go/webtransport-go v0.10.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	resty.dev/v3 v3.0.0-rc.1 // indirect
)

replace (
	github.com/aqcool/socket.io/clients/engine/v3 => ../clients/engine
	github.com/aqcool/socket.io/clients/socket/v3 => ../clients/socket
	github.com/aqcool/socket.io/parsers/engine/v3 => ../parsers/engine
	github.com/aqcool/socket.io/parsers/socket/v3 => ../parsers/socket
	github.com/aqcool/socket.io/servers/engine/v3 => ../servers/engine
	github.com/aqcool/socket.io/servers/socket/v3 => ../servers/socket
	github.com/aqcool/socket.io/v3 => ../
)
