package utils

import (
	"github.com/aqcool/socket.io/v3/pkg/log"
)

func Log() *log.Log {
	return log.Default()
}
