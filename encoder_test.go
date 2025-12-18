package uniqw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONEncoder_Roundtrip(t *testing.T) {
	enc := &JSONEncoder{}
	type P struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	in := P{A: 42, B: "x"}
	data, err := enc.Encode(in)
	require.NoError(t, err, "encode should not error")

	var out P
	require.NoError(t, enc.Decode(data, &out), "decode should not error")
	assert.Equal(t, in, out, "roundtrip mismatch")
}

func TestJSONEncoder_DecodeError(t *testing.T) {
	enc := &JSONEncoder{}
	var out struct{ A int }
	err := enc.Decode([]byte("{"), &out)
	require.Error(t, err, "expected error for invalid JSON")
}
