package memdb

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found in table")
	ErrEncodeValue    = errors.New("error on encode value")
	ErrDecodeValue    = errors.New("error on decode stored value")
	ErrInvalidVersion = errors.New("error invalid version")
)
