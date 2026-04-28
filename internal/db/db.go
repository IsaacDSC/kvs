package db

import (
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
)

// Item and Table are aliases so callers can use the db package without importing memdb.
type Item = item.Entity
type Table = memdb.Table
type Tx = memdb.Tx

// DB wraps *memdb.DB and forwards all methods via embedding.
type DB struct {
	*memdb.DB
}

// Wrap returns a façade over an existing in-memory database (e.g. from store after Open).
func Wrap(m *memdb.DB) *DB {
	if m == nil {
		return nil
	}
	return &DB{DB: m}
}
