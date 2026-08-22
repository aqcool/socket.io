package utils

import (
	"github.com/aqcool/socket.io/v4/pkg/log"
)

func Log() *log.Log {
	return log.Default()
}
