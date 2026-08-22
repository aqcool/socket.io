package v4preview

import (
	"errors"
	"log/slog"
	"strings"
	"time"
)

func DefaultConfig() Config {
	return Config{
		Path:           "/socket.io",
		ServeClient:    true,
		ConnectTimeout: 45 * time.Second,
		Queue: QueueOptions{
			MaxPending: 4096,
			Overflow:   OverflowDisconnect,
		},
	}
}

func WithPath(path string) Option {
	return func(cfg *Config) error {
		if cfg == nil {
			return errors.New("socketio: config is nil")
		}
		path = strings.TrimSpace(path)
		if path == "" || path[0] != '/' {
			return errors.New("socketio: path must start with /")
		}
		cfg.Path = strings.TrimSuffix(path, "/")
		if cfg.Path == "" {
			cfg.Path = "/"
		}
		return nil
	}
}

func WithClientServing(enabled bool) Option {
	return func(cfg *Config) error {
		cfg.ServeClient = enabled
		return nil
	}
}

func WithConnectTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return errors.New("socketio: connect timeout must be positive")
		}
		cfg.ConnectTimeout = timeout
		return nil
	}
}

func WithAdapter(factory AdapterFactory) Option {
	return func(cfg *Config) error {
		if factory == nil {
			return errors.New("socketio: adapter factory is required")
		}
		cfg.Adapter = factory
		return nil
	}
}

func WithParser(codec PacketCodec) Option {
	return func(cfg *Config) error {
		if codec == nil {
			return errors.New("socketio: packet codec is required")
		}
		cfg.PacketCodec = codec
		return nil
	}
}

func WithValueCodec(codec ValueCodec) Option {
	return func(cfg *Config) error {
		if codec == nil {
			return errors.New("socketio: value codec is required")
		}
		cfg.ValueCodec = codec
		return nil
	}
}

func WithConnectionStateRecovery(recovery RecoveryOptions) Option {
	return func(cfg *Config) error {
		if recovery.MaxDisconnectionDuration <= 0 {
			return errors.New("socketio: recovery duration must be positive")
		}
		cfg.Recovery = &recovery
		return nil
	}
}

func WithCleanupEmptyChildNamespaces(enabled bool) Option {
	return func(cfg *Config) error {
		cfg.CleanupEmptyChildNamespaces = enabled
		return nil
	}
}

func WithQueue(queue QueueOptions) Option {
	return func(cfg *Config) error {
		if queue.MaxPending <= 0 {
			return errors.New("socketio: queue max pending must be positive")
		}
		switch queue.Overflow {
		case OverflowDisconnect, OverflowDropNewest, OverflowReject:
		default:
			return errors.New("socketio: invalid queue overflow policy")
		}
		cfg.Queue = queue
		return nil
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(cfg *Config) error {
		if logger == nil {
			return errors.New("socketio: logger is required")
		}
		cfg.Logger = logger
		return nil
	}
}
