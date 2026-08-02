package amqp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func newMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate AMQP message ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
