package parser

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/aqcool/socket.io/v3/pkg/types"
)

// ErrCircularReference is raised by Encoder.Encode when packet data contains
// a cycle that cannot be represented by JSON.
var ErrCircularReference = errors.New("circular reference")

// encoder implements the Encoder interface for Socket.IO packet encoding.
type encoder struct{}

// NewEncoder creates a new Encoder instance.
func NewEncoder() Encoder {
	return &encoder{}
}

// Encode encodes a Socket.IO packet into a sequence of buffers.
// For non-binary packets, it returns a single string buffer.
// For binary packets, it returns the encoded packet header followed by binary buffers.
func (e *encoder) Encode(packet *Packet) []types.BufferInterface {
	parserLog.Debug("encoding packet %v", packet)
	if hasCircularReference(packet.Data) {
		panic(ErrCircularReference)
	}

	// Check if the packet contains binary data and upgrade packet type if needed
	if packet.Type == EVENT || packet.Type == ACK {
		if HasBinary(packet.Data) {
			data := *packet
			if data.Type == EVENT {
				data.Type = BINARY_EVENT
			} else {
				data.Type = BINARY_ACK
			}
			return e.encodeAsBinary(&data)
		}
	}

	return []types.BufferInterface{e.encodeAsString(packet)}
}

func hasCircularReference(data any) bool {
	return hasCircularValue(reflect.ValueOf(data), make(map[visit]bool))
}

func hasCircularValue(value reflect.Value, stack map[visit]bool) bool {
	if !value.IsValid() || isNilReflectValue(value) {
		return false
	}

	if value.CanInterface() {
		data := value.Interface()
		if IsBinary(data) {
			return false
		}
		if _, ok := data.(JSONTransformer); ok {
			// ToJSON is evaluated later by the normal binary-detection and
			// deconstruction pipeline. Do not invoke user code an extra time here.
			return false
		}
		if _, ok := data.(json.Marshaler); ok {
			return false
		}
	}

	switch value.Kind() {
	case reflect.Interface:
		return hasCircularValue(value.Elem(), stack)
	case reflect.Pointer:
		if !enterCircularVisit(value, stack) {
			return true
		}
		defer leaveCircularVisit(value, stack)
		return hasCircularValue(value.Elem(), stack)
	case reflect.Map:
		if !enterCircularVisit(value, stack) {
			return true
		}
		defer leaveCircularVisit(value, stack)
		iterator := value.MapRange()
		for iterator.Next() {
			if hasCircularValue(iterator.Value(), stack) {
				return true
			}
		}
	case reflect.Slice:
		if !enterCircularVisit(value, stack) {
			return true
		}
		defer leaveCircularVisit(value, stack)
		for index := 0; index < value.Len(); index++ {
			if hasCircularValue(value.Index(index), stack) {
				return true
			}
		}
	case reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if hasCircularValue(value.Index(index), stack) {
				return true
			}
		}
	case reflect.Struct:
		valueType := value.Type()
		for index := 0; index < value.NumField(); index++ {
			if valueType.Field(index).PkgPath != "" {
				continue
			}
			if hasCircularValue(value.Field(index), stack) {
				return true
			}
		}
	}

	return false
}

func enterCircularVisit(value reflect.Value, stack map[visit]bool) bool {
	key := visit{typeOf: value.Type(), pointer: value.Pointer()}
	if stack[key] {
		return false
	}
	stack[key] = true
	return true
}

func leaveCircularVisit(value reflect.Value, stack map[visit]bool) {
	delete(stack, visit{typeOf: value.Type(), pointer: value.Pointer()})
}

// encodeAsString encodes a packet as a string buffer.
// The format is: <type>[<attachments>-][/<namespace>,][<id>][<data>]
func (e *encoder) encodeAsString(packet *Packet) types.BufferInterface {
	// Start with packet type
	buffer := types.NewStringBuffer([]byte{byte(packet.Type) + '0'})

	// Add attachment count for binary packets
	if (packet.Type == BINARY_EVENT || packet.Type == BINARY_ACK) && packet.Attachments != nil {
		_, _ = buffer.WriteString(strconv.FormatUint(*packet.Attachments, 10))
		_ = buffer.WriteByte('-')
	}

	// Add namespace (if not the default "/")
	if len(packet.Nsp) > 0 && packet.Nsp != "/" {
		_, _ = buffer.WriteString(packet.Nsp)
		_ = buffer.WriteByte(',')
	}

	// Add packet ID for acknowledgments
	if packet.Id != nil {
		_, _ = buffer.WriteString(strconv.FormatUint(*packet.Id, 10))
	}

	// Add JSON-encoded data
	if packet.Data != nil {
		processedData := preprocessData(packet.Data)
		if jsonBytes, err := json.Marshal(processedData); err == nil {
			if len(jsonBytes) <= types.MaxPayloadSize {
				_, _ = buffer.Write(jsonBytes)
			}
		}
	}

	parserLog.Debug("encoded %v as %v", packet, buffer)
	return buffer
}

// encodeAsBinary encodes a packet that contains binary data.
// It deconstructs the packet to extract binary data, then encodes the packet header
// followed by all binary buffers.
func (e *encoder) encodeAsBinary(packet *Packet) []types.BufferInterface {
	deconstructedPacket, buffers := DeconstructPacket(packet)
	header := e.encodeAsString(deconstructedPacket)
	return append([]types.BufferInterface{header}, buffers...)
}

// preprocessData recursively processes data to convert special types
// that need transformation before JSON encoding.
func preprocessData(data any) any {
	switch typedData := data.(type) {
	case nil:
		return nil
	case *strings.Reader:
		// Convert strings.Reader to StringBuffer for proper handling
		buffer, _ := types.NewStringBufferReader(typedData)
		return buffer
	case []any:
		result := make([]any, 0, len(typedData))
		for _, item := range typedData {
			result = append(result, preprocessData(item))
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typedData))
		for key, value := range typedData {
			result[key] = preprocessData(value)
		}
		return result
	default:
		return data
	}
}
