package db

import (
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/old/memdb"
)

// Item and Table are aliases so callers can use the db package without importing memdb.
type Item = item.Entity
type Table = memdb.Table
type Tx = memdb.Tx

// OldDb wraps *memdb.OldDb and forwards all methods via embedding.
type OldDb struct {
	*memdb.DB
}

// Wrap returns a façade over an existing in-memory database (e.g. from store after Open).
func Wrap(m *memdb.DB) *OldDb {
	if m == nil {
		return nil
	}
	return &OldDb{DB: m}
}
