package store

import "github.com/IsaacDSC/kvs/internal/wal"

// Durability controls how WAL writes reach stable storage.
type Durability = wal.Durability

const (
	// SyncEveryWrite calls fsync after each WAL append (slowest, strongest single-node durability).
	SyncEveryWrite = wal.SyncEveryWrite
	// Buffered keeps writes in memory until Flush or Close (weaker until flush).
	Buffered = wal.Buffered
)

// Options configures a Store and its WAL.
type Options = wal.Options

// DefaultDataDir is the conventional directory name under the process working directory ("./tmp").
const DefaultDataDir = "tmp"
