// Package code serializes values for in-memory storage using CBOR.
// Callers pass and receive ordinary Go values (any); they never handle raw CBOR.
package code

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Encode serializes v to CBOR bytes suitable for storing in memory or on disk.
func Encode(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

// DecodeItem unmarshals CBOR into item (e.g. *item.Entity). Map values in any fields
// (e.g. Entity.Value) decode as map[interface{}]interface{} — same as cbor.Unmarshal to any.
func DecodeItem(data []byte, item any) error {
	if err := cbor.Unmarshal(data, item); err != nil {
		return fmt.Errorf("memdb.decode error on decoding item: %w", err)
	}

	return nil
}
