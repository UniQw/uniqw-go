package uniqw

import (
	"encoding/json"

	"github.com/bytedance/sonic"
)

// Encoder defines the interface for task payload serialization.
type Encoder interface {
	// Encode serializes a value to bytes.
	Encode(any) ([]byte, error)
	// Decode deserializes bytes to a value.
	Decode([]byte, any) error
}

// JSONEncoder is the default implementation of Encoder using JSON.
// It uses standard library for encoding and sonic for decoding.
type JSONEncoder struct{}

// Encode serializes a value to JSON using standard library.
func (*JSONEncoder) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode deserializes JSON bytes using sonic.
func (*JSONEncoder) Decode(data []byte, v any) error {
	return sonic.Unmarshal(data, v)
}
