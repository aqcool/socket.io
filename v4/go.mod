module github.com/aqcool/socket.io/v4

go 1.26.0

require github.com/aqcool/socket.io/servers/socket/v3 v3.0.4

replace (
	github.com/aqcool/socket.io/parsers/engine/v3 => ../parsers/engine
	github.com/aqcool/socket.io/parsers/socket/v3 => ../parsers/socket
	github.com/aqcool/socket.io/servers/engine/v3 => ../servers/engine
	github.com/aqcool/socket.io/servers/socket/v3 => ../servers/socket
	github.com/aqcool/socket.io/v3 => ..
)
