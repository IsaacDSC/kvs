package db

import "errors"

var (
	ErrTableNotFound = errors.New("table not found")
	ErrNotFound      = errors.New("not found")
	ErrNotFoundSk    = errors.New("secondary key not found")
)
