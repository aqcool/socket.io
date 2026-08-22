package parser

import (
	"io"
	"reflect"
	"strings"

	"github.com/aqcool/socket.io/v4/pkg/types"
)

// JSONTransformer exposes the value that should be inspected before JSON
// encoding. It is the Go equivalent of an object's toJSON() method.
type JSONTransformer interface {
	ToJSON() any
}

// IsBinary determines if the given data is a binary type.
// Returns true for []byte and io.Reader types (excluding StringBuffer and strings.Reader),
// which are treated as binary data in the Socket.IO protocol.
func IsBinary(data any) bool {
	switch data.(type) {
	case *types.StringBuffer, *strings.Reader:
		// StringBuffer and strings.Reader are text-based, not binary
		return false
	case []byte, io.Reader:
		// Byte slices and other readers are considered binary
		return true
	default:
		return false
	}
}

// HasBinary recursively checks if the data contains any binary content.
// It traverses slices and maps to detect nested binary data.
func HasBinary(data any) bool {
	return hasBinary(data, true, make(map[visit]bool))
}

func hasBinary(data any, allowToJSON bool, visited map[visit]bool) bool {
	switch value := data.(type) {
	case nil:
		return false
	case []any:
		if !enterBinaryVisit(reflect.ValueOf(value), visited) {
			return false
		}
		defer leaveBinaryVisit(reflect.ValueOf(value), visited)
		for _, item := range value {
			if hasBinary(item, true, visited) {
				return true
			}
		}
		return false
	case map[string]any:
		if !enterBinaryVisit(reflect.ValueOf(value), visited) {
			return false
		}
		defer leaveBinaryVisit(reflect.ValueOf(value), visited)
		if allowToJSON {
			if transformer, ok := data.(JSONTransformer); ok {
				return hasBinary(transformer.ToJSON(), false, visited)
			}
		}
		for _, item := range value {
			if hasBinary(item, true, visited) {
				return true
			}
		}
		return false
	default:
		if IsBinary(data) {
			return true
		}

		reflected := reflect.ValueOf(data)
		if isNilReflectValue(reflected) {
			return false
		}
		if allowToJSON {
			if transformer, ok := data.(JSONTransformer); ok {
				return hasBinary(transformer.ToJSON(), false, visited)
			}
		}
		return hasBinaryValue(reflected, false, visited)
	}
}

type visit struct {
	typeOf  reflect.Type
	pointer uintptr
}

func hasBinaryValue(value reflect.Value, allowToJSON bool, visited map[visit]bool) bool {
	if !value.IsValid() {
		return false
	}
	if value.CanInterface() {
		data := value.Interface()
		if IsBinary(data) {
			return true
		}
		if allowToJSON {
			if transformer, ok := data.(JSONTransformer); ok {
				return hasBinary(transformer.ToJSON(), false, visited)
			}
		}
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return false
		}
		return hasBinaryValue(value.Elem(), allowToJSON, visited)
	case reflect.Pointer:
		if value.IsNil() {
			return false
		}
		if !enterBinaryVisit(value, visited) {
			return false
		}
		defer leaveBinaryVisit(value, visited)
		return hasBinaryValue(value.Elem(), false, visited)
	case reflect.Struct:
		for field := range value.Fields() {
			fieldValue := value.FieldByIndex(field.Index)
			if hasBinaryValue(fieldValue, true, visited) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if hasBinaryValue(value.Index(i), true, visited) {
				return true
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if hasBinaryValue(iterator.Value(), true, visited) {
				return true
			}
		}
	}
	return false
}

func enterBinaryVisit(value reflect.Value, visited map[visit]bool) bool {
	if isNilReflectValue(value) {
		return false
	}

	key := visit{typeOf: value.Type(), pointer: value.Pointer()}
	if visited[key] {
		return false
	}
	visited[key] = true
	return true
}

func leaveBinaryVisit(value reflect.Value, visited map[visit]bool) {
	delete(visited, visit{typeOf: value.Type(), pointer: value.Pointer()})
}

func isNilReflectValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
