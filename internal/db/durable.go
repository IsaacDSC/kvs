package db

// DurableWriter receives mutations that must be persisted (e.g. append to WAL).
// A nil DurableWriter means Table.Set/Delete update memory only (tests, replay helpers).
type DurableWriter interface {
	Put(table string, item Item) error
	Delete(table, key string) error
}

// TxMutation is one step in a transactional session. Exactly one of Put or DelKey is set.
type TxMutation struct {
	Put    *Item
	DelKey string
}

// TransactionCommitter persists an atomic group of mutations (e.g. WAL BEGIN…COMMIT).
// Implemented by the store when opened with durability.
type TransactionCommitter interface {
	CommitTransaction(table string, muts []TxMutation) error
}
