package wal

// Durability controls how WAL writes reach stable storage.
type Durability int

const (
	// SyncEveryWrite calls fsync after each WAL append (slowest, strongest single-node durability).
	SyncEveryWrite Durability = iota
	// Buffered keeps writes in memory until Flush or Close (weaker until flush).
	Buffered
)

// Options configures WAL behavior.
type Options struct {
	Durability Durability
	// AfterSync is invoked after each successful WAL fsync (SyncEveryWrite or Flush). Optional, for tests.
	AfterSync func()
}

// WalFileName is the conventional WAL filename under a data directory.
const WalFileName = "data.wal"
