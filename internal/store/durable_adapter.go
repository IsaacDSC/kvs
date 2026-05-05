package store

import (
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/old/memdb"
)

var (
	_ memdb.DurableWriter        = (*storeDurable)(nil)
	_ memdb.TransactionCommitter = (*storeDurable)(nil)
)

// storeDurable bridges memdb to Store without exposing Put/Delete/CommitTransaction on *Store.
type storeDurable struct {
	s *Store
}

func (d *storeDurable) Put(table string, item item.Entity) error {
	d.s.mu.Lock()
	defer d.s.mu.Unlock()
	return d.s.putLocked(table, item)
}

func (d *storeDurable) Delete(table, key string) error {
	d.s.mu.Lock()
	defer d.s.mu.Unlock()
	return d.s.deleteLocked(table, key)
}

func (d *storeDurable) CommitTransaction(table string, muts []memdb.TxMutation) error {
	return d.s.commitTransaction(table, muts)
}
