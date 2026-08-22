package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/aqcool/socket.io/v4/pkg/log"
	"github.com/aqcool/socket.io/v4/pkg/types"
)

const (
	// DefaultMaxAttachments is the default maximum number of binary attachments allowed per packet.
	// This prevents resource exhaustion from malicious clients sending excessively large attachment counts.
	DefaultMaxAttachments uint64 = 10
	// DefaultMaxNamespaceLength is the default maximum allowed length of a namespace name.
	// This prevents resource exhaustion from malicious clients sending excessively long namespace names.
	DefaultMaxNamespaceLength int = 512

	// DefaultMaxPacketIDLength is the default maximum allowed length of a packet ID string.
	// uint64 max is 18446744073709551615 (20 digits).
	DefaultMaxPacketIDLength int = 20
)

var (
	// parserLog is the logger for the parser package.
	parserLog = log.NewLog("socket.io:parser")

	// ReservedEvents contains event names that have special meaning in Socket.IO
	// and cannot be used as custom event names.
	ReservedEvents = types.NewSet(
		"connect",        // Used on the client side to indicate connection
		"connect_error",  // Used on the client side to indicate connection error
		"disconnect",     // Used on both sides to indicate disconnection
		"disconnecting",  // Used on the server side during disconnection
		"newListener",    // Used by the Node.js EventEmitter
		"removeListener", // Used by the Node.js EventEmitter
	)
)

// Error definitions for decoder operations.
var (
	ErrPlaintextDuringReconstruction = errors.New("got plaintext data when reconstructing a packet")
	ErrBinaryWithoutReconstruction   = errors.New("got binary data when not reconstructing a packet")
	ErrInvalidPayload                = errors.New("invalid payload")
	ErrIllegalNamespace              = errors.New("illegal namespace")
	ErrIllegalID                     = errors.New("illegal id")
	// ErrInvalidAttachmentCount matches the error exposed by the official
	// Socket.IO parser when a binary packet does not contain a valid
	// "<attachments>-" prefix. ErrIllegalAttachments remains reserved for a
	// placeholder that cannot be reconstructed from the received buffers.
	ErrInvalidAttachmentCount = errors.New("Illegal attachments") //nolint:staticcheck // Official parser compatibility requires this exact text.
	ErrTooManyAttachments     = errors.New("too many attachments")
)

// decoder implements the Decoder interface for Socket.IO packet decoding.
type decoder struct {
	types.EventEmitter

	// reconstructor manages binary packet reconstruction state.
	reconstructor atomic.Pointer[binaryReconstructor]

	opts *DecoderOptions
}

// NewDecoder creates a new Decoder instance.
// The optional constructor argument can be either DecoderOptionsInterface or
// JSONReviver, matching the object and legacy function forms accepted by the
// official parser.
func NewDecoder(opts ...any) Decoder {
	options := DefaultDecoderOptions()
	options.SetMaxAttachments(DefaultMaxAttachments)
	options.SetMaxNamespaceLength(DefaultMaxNamespaceLength)
	options.SetMaxPacketIDLength(DefaultMaxPacketIDLength)

	if len(opts) > 0 {
		switch option := opts[0].(type) {
		case nil:
		case DecoderOptionsInterface:
			if !isNilDecoderOption(option) {
				options.Assign(option)
			}
		case JSONReviver:
			options.SetReviver(option)
		case func(string, any) any:
			options.SetReviver(JSONReviver(option))
		default:
			panic(fmt.Sprintf("parser: unsupported decoder option %T", option))
		}
	}

	return &decoder{
		EventEmitter: types.NewEventEmitter(),
		opts:         options,
	}
}

func isNilDecoderOption(option DecoderOptionsInterface) bool {
	value := reflect.ValueOf(option)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Add processes incoming data (string or binary) and emits decoded packets.
// For string data, it decodes immediately. For binary data, it accumulates
// buffers until the packet is complete, then emits the reconstructed packet.
func (d *decoder) Add(data any) error {
	switch typedData := data.(type) {
	case string:
		return d.handleStringData(types.NewStringBufferString(typedData))

	case *strings.Reader:
		buffer, err := types.NewStringBufferReader(typedData)
		if err != nil {
			return err
		}
		return d.handleStringData(buffer)

	case *types.StringBuffer:
		return d.handleStringData(typedData)

	default:
		return d.handleBinaryData(data)
	}
}

// handleStringData processes string-based packet data.
func (d *decoder) handleStringData(buffer types.BufferInterface) error {
	if d.reconstructor.Load() != nil {
		return ErrPlaintextDuringReconstruction
	}
	return d.decodeAsString(buffer)
}

// handleBinaryData processes binary packet data for reconstruction.
func (d *decoder) handleBinaryData(data any) error {
	if !IsBinary(data) {
		return fmt.Errorf("Unknown type: %v", data) //nolint:staticcheck // Official parser compatibility requires this exact text.
	}

	reconstructor := d.reconstructor.Load()
	if reconstructor == nil {
		return ErrBinaryWithoutReconstruction
	}

	buffer, err := d.readBinaryData(data)
	if err != nil {
		return err
	}

	packet, err := reconstructor.takeBinaryData(buffer)
	if err != nil {
		return err
	}

	if packet != nil {
		// Received final buffer, packet is complete
		d.reconstructor.Store(nil)
		d.Emit("decoded", packet)
	}

	return nil
}

// readBinaryData reads binary data from various source types into a buffer.
func (d *decoder) readBinaryData(data any) (types.BufferInterface, error) {
	buffer := types.NewBytesBuffer(nil)

	switch typedData := data.(type) {
	case io.Reader:
		if closer, ok := data.(io.Closer); ok {
			defer func() {
				if err := closer.Close(); err != nil {
					parserLog.Debug("failed to close binary reader: %v", err)
				}
			}()
		}
		if _, err := buffer.ReadFrom(typedData); err != nil {
			return nil, err
		}
	case []byte:
		if _, err := buffer.Write(typedData); err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

// decodeAsString decodes a string buffer and handles binary packet initialization.
func (d *decoder) decodeAsString(buffer types.BufferInterface) error {
	packet, err := d.decodePacket(buffer)
	if err != nil {
		parserLog.Debug("decode error: %v", err)
		return err
	}

	if packet.Type == BINARY_EVENT || packet.Type == BINARY_ACK {
		if packet.Type == BINARY_EVENT {
			packet.Type = EVENT
		} else {
			packet.Type = ACK
		}
		d.reconstructor.Store(newBinaryReconstructor(packet))
	} else {
		// Non-binary packet, emit immediately
		d.Emit("decoded", packet)
	}

	return nil
}

// decodePacket parses a packet from a string buffer.
func (d *decoder) decodePacket(buffer types.BufferInterface) (*Packet, error) {
	originalStr := buffer.String() // For debug logging
	packet := &Packet{}

	// Parse packet type
	if err := d.parsePacketType(buffer, packet); err != nil {
		return nil, err
	}

	// Parse attachments for binary packets
	if err := d.parseAttachments(buffer, packet); err != nil {
		return nil, err
	}

	// Parse namespace
	if err := d.parseNamespace(buffer, packet); err != nil {
		return nil, err
	}

	// Parse packet ID
	if err := d.parsePacketID(buffer, packet); err != nil {
		return nil, err
	}

	// Parse payload data
	if err := d.parsePayload(buffer, packet); err != nil {
		return nil, err
	}

	parserLog.Debug("decoded %s as %v", originalStr, packet)
	return packet, nil
}

// parsePacketType reads and validates the packet type.
func (d *decoder) parsePacketType(buffer types.BufferInterface, packet *Packet) error {
	typeByte, err := buffer.ReadByte()
	if err != nil {
		return ErrInvalidPayload
	}

	packet.Type = PacketType(int(typeByte) - '0')
	if !packet.Type.Valid() {
		return fmt.Errorf("unknown packet type %d", packet.Type)
	}

	return nil
}

// parseAttachments reads attachment count for binary packets.
func (d *decoder) parseAttachments(buffer types.BufferInterface, packet *Packet) error {
	if packet.Type != BINARY_EVENT && packet.Type != BINARY_ACK {
		return nil
	}

	attachmentStr, err := buffer.ReadString('-')
	if err != nil {
		return ErrInvalidAttachmentCount
	}

	strLen := len(attachmentStr)
	if strLen < 2 { // Must be at least "X-" where X is a digit
		return ErrInvalidAttachmentCount
	}

	attachmentCount, err := strconv.ParseUint(attachmentStr[:strLen-1], 10, 64)
	if err != nil {
		return ErrInvalidAttachmentCount
	}

	if attachmentCount == 0 {
		return ErrInvalidAttachmentCount
	}

	if attachmentCount > d.opts.MaxAttachments() {
		return ErrTooManyAttachments
	}

	packet.Attachments = &attachmentCount
	return nil
}

// parseNamespace reads the namespace from the buffer.
func (d *decoder) parseNamespace(buffer types.BufferInterface, packet *Packet) error {
	firstByte, err := buffer.ReadByte()
	if err != nil {
		if err == io.EOF {
			packet.Nsp = "/"
			return nil
		}
		return ErrIllegalNamespace
	}

	if firstByte != '/' {
		// No namespace specified, use default and put byte back
		if unreadErr := buffer.UnreadByte(); unreadErr != nil {
			return ErrIllegalNamespace
		}
		packet.Nsp = "/"
		return nil
	}

	// Read the rest of the namespace until comma
	nspSuffix, err := buffer.ReadString(',')
	if err != nil {
		if err == io.EOF {
			if len(nspSuffix)+1 > d.opts.MaxNamespaceLength() {
				return ErrIllegalNamespace
			}
			packet.Nsp = "/" + nspSuffix
			return nil
		}
		return ErrIllegalNamespace
	}

	// Remove trailing comma
	nsp := "/" + nspSuffix[:len(nspSuffix)-1]
	if len(nsp) > d.opts.MaxNamespaceLength() {
		return ErrIllegalNamespace
	}
	packet.Nsp = nsp
	return nil
}

// parsePacketID reads the optional packet ID for acknowledgments.
func (d *decoder) parsePacketID(buffer types.BufferInterface, packet *Packet) error {
	if buffer.Len() == 0 {
		return nil
	}

	var idBuilder strings.Builder

	for {
		if idBuilder.Len() >= d.opts.MaxPacketIDLength() {
			return ErrIllegalID
		}

		b, err := buffer.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if b >= '0' && b <= '9' {
			if err := idBuilder.WriteByte(b); err != nil {
				return err
			}
		} else {
			if err := buffer.UnreadByte(); err != nil {
				return ErrIllegalID
			}
			break
		}
	}

	if idBuilder.Len() > 0 {
		packetID, err := strconv.ParseUint(idBuilder.String(), 10, 64)
		if err != nil {
			return err
		}
		packet.Id = &packetID
	}

	return nil
}

// parsePayload reads and validates the JSON payload.
func (d *decoder) parsePayload(buffer types.BufferInterface, packet *Packet) error {
	if buffer.Len() == 0 {
		return d.validatePayload(packet.Type, nil)
	}

	var payload any
	jsonDecoder := json.NewDecoder(buffer)
	if err := jsonDecoder.Decode(&payload); err != nil {
		return ErrInvalidPayload
	}
	if err := jsonDecoder.Decode(&struct{}{}); err != io.EOF {
		return ErrInvalidPayload
	}

	if reviver := d.opts.Reviver(); reviver != nil {
		var valid bool
		payload, valid = applyJSONReviver(payload, reviver)
		if !valid {
			return ErrInvalidPayload
		}
	}

	if err := d.validatePayload(packet.Type, payload); err != nil {
		return err
	}

	packet.Data = payload
	return nil
}

// applyJSONReviver applies reviver in the same post-order traversal used by
// JSON.parse(). A panic from user code is treated as a parse failure, as the
// official parser catches exceptions thrown by its reviver.
func applyJSONReviver(payload any, reviver JSONReviver) (result any, valid bool) {
	valid = true
	defer func() {
		if recover() != nil {
			result = nil
			valid = false
		}
	}()

	return reviveJSONValue("", payload, reviver), true
}

func reviveJSONValue(key string, value any, reviver JSONReviver) any {
	switch typedValue := value.(type) {
	case []any:
		for index, item := range typedValue {
			typedValue[index] = reviveJSONValue(strconv.Itoa(index), item, reviver)
		}
	case map[string]any:
		for property, item := range typedValue {
			typedValue[property] = reviveJSONValue(property, item, reviver)
		}
	}

	return reviver(key, value)
}

// validatePayload checks if the payload is valid for the given packet type.
func (d *decoder) validatePayload(packetType PacketType, payload any) error {
	if !isPayloadValid(packetType, payload) {
		return ErrInvalidPayload
	}
	return nil
}

// Destroy releases the decoder's resources and stops any ongoing reconstruction.
func (d *decoder) Destroy() {
	if reconstructor := d.reconstructor.Swap(nil); reconstructor != nil {
		reconstructor.finishedReconstruction()
	}
}

// Payload validation helpers

// isPayloadValid checks if the payload matches the expected format for the packet type.
func isPayloadValid(packetType PacketType, payload any) bool {
	switch packetType {
	case CONNECT:
		return payload == nil || isMap(payload)
	case DISCONNECT:
		return payload == nil
	case CONNECT_ERROR:
		return isMap(payload) || isString(payload)
	case EVENT, BINARY_EVENT:
		return isValidEventPayload(payload)
	case ACK, BINARY_ACK:
		return isSlice(payload)
	default:
		return false
	}
}

// isMap checks if the payload is a map[string]any.
func isMap(payload any) bool {
	_, ok := payload.(map[string]any)
	return ok
}

// isString checks if the payload is a string.
func isString(payload any) bool {
	_, ok := payload.(string)
	return ok
}

// isSlice checks if the payload is a slice.
func isSlice(payload any) bool {
	_, ok := payload.([]any)
	return ok
}

// isValidEventPayload validates that an event payload has a valid event name.
// The event name can be either a string (not in reserved events) or a number.
func isValidEventPayload(payload any) bool {
	data, ok := payload.([]any)
	if !ok || len(data) == 0 {
		return false
	}

	// Event name can be a string or a number
	switch eventName := data[0].(type) {
	case string:
		return !ReservedEvents.Has(eventName)
	case float64: // JSON numbers are decoded as float64 in Go
		return true
	case int, int64, int32:
		return true
	default:
		return false
	}
}
