// Package auth provides authentication middleware for Socket.IO namespaces.
package auth

import (
	"context"
	"errors"
	"strings"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
)

var (
	ErrMissingCredential = errors.New("socket.io auth: missing credential")
	ErrInvalidCredential = errors.New("socket.io auth: invalid credential")
	ErrInvalidOptions    = errors.New("socket.io auth: invalid options")
)

type APIKeyExtractor func(*socket.Socket) (string, error)
type APIKeyValidator func(context.Context, string) (any, error)

type APIKeyOptions struct {
	Extractor       APIKeyExtractor
	Validate        APIKeyValidator
	OnAuthenticated func(*socket.Socket, any) error
}

// APIKey returns Namespace middleware which validates an API key.
func APIKey(options APIKeyOptions) (socket.NamespaceMiddleware, error) {
	if options.Validate == nil {
		return nil, ErrInvalidOptions
	}
	extractor := options.Extractor
	if extractor == nil {
		extractor = DefaultAPIKeyExtractor
	}
	return func(s *socket.Socket, next func(*socket.ExtendedError)) {
		key, err := extractor(s)
		if err == nil {
			var principal any
			principal, err = options.Validate(context.Background(), key)
			if err == nil && options.OnAuthenticated != nil {
				err = options.OnAuthenticated(s, principal)
			}
		}
		nextAuth(err, next)
	}, nil
}

// DefaultAPIKeyExtractor reads auth.apiKey, then X-API-Key.
func DefaultAPIKeyExtractor(s *socket.Socket) (string, error) {
	if key, ok := s.Handshake().Auth["apiKey"].(string); ok && key != "" {
		return key, nil
	}
	for name, value := range s.Handshake().Headers {
		if !strings.EqualFold(name, "X-API-Key") {
			continue
		}
		if key := firstHeader(value); key != "" {
			return key, nil
		}
	}
	return "", ErrMissingCredential
}

func firstHeader(value any) string {
	switch header := value.(type) {
	case string:
		return header
	case []string:
		if len(header) > 0 {
			return header[0]
		}
	case []any:
		if len(header) > 0 {
			value, _ := header[0].(string)
			return value
		}
	}
	return ""
}

func nextAuth(err error, next func(*socket.ExtendedError)) {
	if err == nil {
		next(nil)
		return
	}
	next(socket.NewExtendedError(ErrInvalidCredential.Error(), map[string]any{
		"cause": err.Error(),
	}))
}
