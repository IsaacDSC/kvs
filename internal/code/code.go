// Package code serializes values for in-memory storage using CBOR.
// Callers pass and receive ordinary Go values (any); they never handle raw CBOR.
package code

import (
	"github.com/fxamacker/cbor/v2"
)

// Encode serializes v to CBOR bytes suitable for storing in memory or on disk.
func Encode(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

// Decode restores a value previously produced by Encode into an arbitrary Go value
// (maps decode as map[interface{}]interface{}). For memdb.Item bytes, unmarshal into Item instead.
func Decode(data []byte) (any, error) {
	var v any
	if err := cbor.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}
