package db

// DurableWriter receives mutations that must be persisted (e.g. append to WAL).
// A nil DurableWriter means Table.Set/Delete update memory only (tests, replay helpers).
type DurableWriter interface {
	Put(table string, item Item) error
	Delete(table, key string) error
}
