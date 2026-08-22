package socketio

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type OverflowPolicy uint8

const (
	OverflowDisconnect OverflowPolicy = iota
	OverflowDropNewest
	OverflowReject
)

type QueueOptions struct {
	MaxPending int
	Overflow   OverflowPolicy
}

type RecoveryOptions struct {
	MaxDisconnectionDuration time.Duration
	SkipMiddleware           bool
	CleanupInterval          time.Duration
}

type Config struct {
	Path                        string
	ServeClient                 bool
	ConnectTimeout              time.Duration
	CleanupEmptyChildNamespaces bool
	Queue                       QueueOptions
	Recovery                    *RecoveryOptions
	Logger                      *slog.Logger
	Adapter                     AdapterFactory
	PacketCodec                 PacketCodec
	ValueCodec                  ValueCodec
}

type Option func(*Config) error

func DefaultConfig() Config {
	return Config{
		Path:           "/socket.io",
		ServeClient:    true,
		ConnectTimeout: 45 * time.Second,
		Queue: QueueOptions{
			MaxPending: 4096,
			Overflow:   OverflowDisconnect,
		},
		Logger:     slog.Default(),
		ValueCodec: JSONValueCodec{},
	}
}

func WithPath(path string) Option {
	return func(cfg *Config) error {
		if cfg == nil {
			return ErrInvalidArgument
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return fmt.Errorf("%w: path is required", ErrInvalidArgument)
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		if path != "/" {
			path = strings.TrimRight(path, "/")
		}
		cfg.Path = path
		return nil
	}
}

func WithClientServing(enabled bool) Option {
	return func(cfg *Config) error {
		if cfg == nil {
			return ErrInvalidArgument
		}
		cfg.ServeClient = enabled
		return nil
	}
}

func WithConnectTimeout(d time.Duration) Option {
	return func(cfg *Config) error {
		if cfg == nil || d <= 0 {
			return fmt.Errorf("%w: connect timeout must be positive", ErrInvalidArgument)
		}
		cfg.ConnectTimeout = d
		return nil
	}
}

func WithAdapter(factory AdapterFactory) Option {
	return func(cfg *Config) error {
		if cfg == nil || factory == nil {
			return fmt.Errorf("%w: adapter factory is required", ErrInvalidArgument)
		}
		cfg.Adapter = factory
		return nil
	}
}

func WithParser(codec PacketCodec) Option {
	return func(cfg *Config) error {
		if cfg == nil || codec == nil {
			return fmt.Errorf("%w: packet codec is required", ErrInvalidArgument)
		}
		cfg.PacketCodec = codec
		return nil
	}
}

func WithValueCodec(codec ValueCodec) Option {
	return func(cfg *Config) error {
		if cfg == nil || codec == nil {
			return fmt.Errorf("%w: value codec is required", ErrInvalidArgument)
		}
		cfg.ValueCodec = codec
		return nil
	}
}

func WithConnectionStateRecovery(opts RecoveryOptions) Option {
	return func(cfg *Config) error {
		if cfg == nil {
			return ErrInvalidArgument
		}
		if opts.MaxDisconnectionDuration <= 0 {
			opts.MaxDisconnectionDuration = 2 * time.Minute
		}
		if opts.CleanupInterval <= 0 {
			opts.CleanupInterval = time.Minute
		}
		copy := opts
		cfg.Recovery = &copy
		return nil
	}
}

func WithCleanupEmptyChildNamespaces(enabled bool) Option {
	return func(cfg *Config) error {
		if cfg == nil {
			return ErrInvalidArgument
		}
		cfg.CleanupEmptyChildNamespaces = enabled
		return nil
	}
}

func WithQueue(opts QueueOptions) Option {
	return func(cfg *Config) error {
		if cfg == nil || opts.MaxPending <= 0 {
			return fmt.Errorf("%w: queue max pending must be positive", ErrInvalidArgument)
		}
		if opts.Overflow > OverflowReject {
			return fmt.Errorf("%w: unknown queue overflow policy", ErrInvalidArgument)
		}
		cfg.Queue = opts
		return nil
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(cfg *Config) error {
		if cfg == nil || logger == nil {
			return fmt.Errorf("%w: logger is required", ErrInvalidArgument)
		}
		cfg.Logger = logger
		return nil
	}
}
