package socket

import (
	"context"
	"time"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

// SocketOptionsInterface defines the interface for accessing and modifying Socket options.
// It provides methods for authentication, retry logic, and acknowledgement timeouts.
type (
	// AuthProvider dynamically provides namespace authentication data before
	// each connection attempt.
	AuthProvider func(context.Context) (map[string]any, error)

	// OverflowStrategy controls how a bounded client buffer handles a new
	// packet when its packet or byte limit is reached.
	OverflowStrategy string
)

const (
	OverflowReject     OverflowStrategy = "reject"
	OverflowDropNewest OverflowStrategy = "drop_newest"
	OverflowDropOldest OverflowStrategy = "drop_oldest"
	OverflowDisconnect OverflowStrategy = "disconnect"
)

type (
	SocketOptionsInterface interface {
		SetAuth(map[string]any)
		GetRawAuth() types.Optional[map[string]any]
		Auth() map[string]any

		SetAuthProvider(AuthProvider)
		GetRawAuthProvider() types.Optional[AuthProvider]
		AuthProvider() AuthProvider

		SetRetries(float64)
		GetRawRetries() types.Optional[float64]
		Retries() float64

		SetAckTimeout(time.Duration)
		GetRawAckTimeout() types.Optional[time.Duration]
		AckTimeout() time.Duration

		SetMaxSendBufferPackets(int)
		GetRawMaxSendBufferPackets() types.Optional[int]
		MaxSendBufferPackets() int

		SetMaxReceiveBufferPackets(int)
		GetRawMaxReceiveBufferPackets() types.Optional[int]
		MaxReceiveBufferPackets() int

		SetMaxRetryQueuePackets(int)
		GetRawMaxRetryQueuePackets() types.Optional[int]
		MaxRetryQueuePackets() int

		SetMaxSendBufferBytes(int64)
		GetRawMaxSendBufferBytes() types.Optional[int64]
		MaxSendBufferBytes() int64

		SetMaxReceiveBufferBytes(int64)
		GetRawMaxReceiveBufferBytes() types.Optional[int64]
		MaxReceiveBufferBytes() int64

		SetMaxRetryQueueBytes(int64)
		GetRawMaxRetryQueueBytes() types.Optional[int64]
		MaxRetryQueueBytes() int64

		SetOverflowStrategy(OverflowStrategy)
		GetRawOverflowStrategy() types.Optional[OverflowStrategy]
		OverflowStrategy() OverflowStrategy
	}

	// SocketOptions defines configuration options for individual Socket.IO sockets.
	// These options control the behavior of a specific namespace connection.
	SocketOptions struct {
		auth                    types.Optional[map[string]any]
		authProvider            types.Optional[AuthProvider]
		retries                 types.Optional[float64]
		ackTimeout              types.Optional[time.Duration]
		maxSendBufferPackets    types.Optional[int]
		maxReceiveBufferPackets types.Optional[int]
		maxRetryQueuePackets    types.Optional[int]
		maxSendBufferBytes      types.Optional[int64]
		maxReceiveBufferBytes   types.Optional[int64]
		maxRetryQueueBytes      types.Optional[int64]
		overflowStrategy        types.Optional[OverflowStrategy]
	}
)

// DefaultSocketOptions creates a new SocketOptions instance with default values.
// Use this function to create a base configuration that can be customized.
func DefaultSocketOptions() *SocketOptions {
	return &SocketOptions{}
}

// Assign copies all options from another SocketOptionsInterface instance.
// If data is nil, it returns the current SocketOptions instance.
func (s *SocketOptions) Assign(data SocketOptionsInterface) SocketOptionsInterface {
	if data == nil {
		return s
	}

	if data.GetRawAuth() != nil {
		s.SetAuth(data.Auth())
	}
	if data.GetRawAuthProvider() != nil {
		s.SetAuthProvider(data.AuthProvider())
	}
	if data.GetRawRetries() != nil {
		s.SetRetries(data.Retries())
	}
	if data.GetRawAckTimeout() != nil {
		s.SetAckTimeout(data.AckTimeout())
	}
	if data.GetRawMaxSendBufferPackets() != nil {
		s.SetMaxSendBufferPackets(data.MaxSendBufferPackets())
	}
	if data.GetRawMaxReceiveBufferPackets() != nil {
		s.SetMaxReceiveBufferPackets(data.MaxReceiveBufferPackets())
	}
	if data.GetRawMaxRetryQueuePackets() != nil {
		s.SetMaxRetryQueuePackets(data.MaxRetryQueuePackets())
	}
	if data.GetRawMaxSendBufferBytes() != nil {
		s.SetMaxSendBufferBytes(data.MaxSendBufferBytes())
	}
	if data.GetRawMaxReceiveBufferBytes() != nil {
		s.SetMaxReceiveBufferBytes(data.MaxReceiveBufferBytes())
	}
	if data.GetRawMaxRetryQueueBytes() != nil {
		s.SetMaxRetryQueueBytes(data.MaxRetryQueueBytes())
	}
	if data.GetRawOverflowStrategy() != nil {
		s.SetOverflowStrategy(data.OverflowStrategy())
	}

	return s
}

func (s *SocketOptions) SetAuthProvider(provider AuthProvider) {
	s.authProvider = types.NewSome(provider)
}

func (s *SocketOptions) GetRawAuthProvider() types.Optional[AuthProvider] {
	return s.authProvider
}

func (s *SocketOptions) AuthProvider() AuthProvider {
	if s.authProvider == nil {
		return nil
	}
	return s.authProvider.Get()
}

// SetAuth configures the authentication data to be sent with the connection.
//
// Parameters:
//   - auth: A map containing authentication credentials or tokens
func (s *SocketOptions) SetAuth(auth map[string]any) {
	s.auth = types.NewSome(auth)
}
func (s *SocketOptions) GetRawAuth() types.Optional[map[string]any] {
	return s.auth
}
func (s *SocketOptions) Auth() map[string]any {
	if s.auth == nil {
		return nil
	}

	return s.auth.Get()
}

// SetRetries sets the maximum number of retries for packet delivery
//
// Parameters:
//   - retries: The maximum number of retries
func (s *SocketOptions) SetRetries(retries float64) {
	s.retries = types.NewSome(retries)
}
func (s *SocketOptions) GetRawRetries() types.Optional[float64] {
	return s.retries
}
func (s *SocketOptions) Retries() float64 {
	if s.retries == nil {
		return 0
	}

	return s.retries.Get()
}

// SetAckTimeout sets how long to wait for an acknowledgement before timing out.
//
// Parameters:
//   - d: The timeout duration
func (s *SocketOptions) SetAckTimeout(ackTimeout time.Duration) {
	s.ackTimeout = types.NewSome(ackTimeout)
}
func (s *SocketOptions) GetRawAckTimeout() types.Optional[time.Duration] {
	return s.ackTimeout
}
func (s *SocketOptions) AckTimeout() time.Duration {
	if s.ackTimeout == nil {
		return 0
	}

	return s.ackTimeout.Get()
}

func (s *SocketOptions) SetMaxSendBufferPackets(value int) {
	s.maxSendBufferPackets = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxSendBufferPackets() types.Optional[int] {
	return s.maxSendBufferPackets
}
func (s *SocketOptions) MaxSendBufferPackets() int {
	if s.maxSendBufferPackets == nil {
		return 0
	}
	return s.maxSendBufferPackets.Get()
}

func (s *SocketOptions) SetMaxReceiveBufferPackets(value int) {
	s.maxReceiveBufferPackets = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxReceiveBufferPackets() types.Optional[int] {
	return s.maxReceiveBufferPackets
}
func (s *SocketOptions) MaxReceiveBufferPackets() int {
	if s.maxReceiveBufferPackets == nil {
		return 0
	}
	return s.maxReceiveBufferPackets.Get()
}

func (s *SocketOptions) SetMaxRetryQueuePackets(value int) {
	s.maxRetryQueuePackets = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxRetryQueuePackets() types.Optional[int] {
	return s.maxRetryQueuePackets
}
func (s *SocketOptions) MaxRetryQueuePackets() int {
	if s.maxRetryQueuePackets == nil {
		return 0
	}
	return s.maxRetryQueuePackets.Get()
}

func (s *SocketOptions) SetMaxSendBufferBytes(value int64) {
	s.maxSendBufferBytes = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxSendBufferBytes() types.Optional[int64] {
	return s.maxSendBufferBytes
}
func (s *SocketOptions) MaxSendBufferBytes() int64 {
	if s.maxSendBufferBytes == nil {
		return 0
	}
	return s.maxSendBufferBytes.Get()
}

func (s *SocketOptions) SetMaxReceiveBufferBytes(value int64) {
	s.maxReceiveBufferBytes = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxReceiveBufferBytes() types.Optional[int64] {
	return s.maxReceiveBufferBytes
}
func (s *SocketOptions) MaxReceiveBufferBytes() int64 {
	if s.maxReceiveBufferBytes == nil {
		return 0
	}
	return s.maxReceiveBufferBytes.Get()
}

func (s *SocketOptions) SetMaxRetryQueueBytes(value int64) {
	s.maxRetryQueueBytes = types.NewSome(value)
}
func (s *SocketOptions) GetRawMaxRetryQueueBytes() types.Optional[int64] {
	return s.maxRetryQueueBytes
}
func (s *SocketOptions) MaxRetryQueueBytes() int64 {
	if s.maxRetryQueueBytes == nil {
		return 0
	}
	return s.maxRetryQueueBytes.Get()
}

func (s *SocketOptions) SetOverflowStrategy(value OverflowStrategy) {
	s.overflowStrategy = types.NewSome(value)
}
func (s *SocketOptions) GetRawOverflowStrategy() types.Optional[OverflowStrategy] {
	return s.overflowStrategy
}
func (s *SocketOptions) OverflowStrategy() OverflowStrategy {
	if s.overflowStrategy == nil {
		return OverflowReject
	}
	return s.overflowStrategy.Get()
}
