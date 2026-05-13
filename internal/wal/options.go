package wal

import (
	"errors"
	"fmt"
)

// Durability controls how WAL writes reach stable storage.
type Durability int

const (
	// SyncEveryWrite calls fsync after each WAL append (slowest, strongest single-node durability).
	SyncEveryWrite Durability = iota
	// Buffered keeps writes in memory until Flush or Close (weaker until flush).
	Buffered
)

// CheckpointConfig controls optional filesystem checkpoint metadata (LastSeq) used at Load.
// Dir is a directory path (not the WAL file path); empty Dir disables checkpoint reads/writes in WAL.
type CheckpointConfig struct {
	// Dir holds checkpoint.cbor (see internal/durable). Empty: Load replays the full WAL.
	Dir string
	// EveryNWrites and MaxWalBytes are reserved for future automatic checkpoint triggers
	// (requires coordination with fsdb so LastSeq never races ahead of durable data).
	EveryNWrites uint64
	MaxWalBytes  int64
	// TruncateAfterCheckpoint, when true, truncates the WAL file to empty after a successful
	// Checkpoint() save. New writes continue with Seq > previous LastSeq; Load uses max(lastSeq, maxSeqInFile).
	TruncateAfterCheckpoint bool
}

// Options configures WAL behavior.
type Options struct {
	Durability Durability
	// AfterSync is invoked after each successful WAL fsync (SyncEveryWrite or Flush). Optional, for tests.
	AfterSync  func()
	Checkpoint CheckpointConfig
}

// CheckpointConfigured reports whether checkpoint metadata (LastSeq) is loaded and saved.
func (o Options) CheckpointConfigured() bool {
	return o.Checkpoint.Dir != ""
}

// Validate checks option combinations. New and Open call this before use.
func (o Options) Validate() error {
	if o.Checkpoint.TruncateAfterCheckpoint && !o.CheckpointConfigured() {
		return errors.New("wal: options: TruncateAfterCheckpoint requires non-empty Checkpoint.Dir")
	}
	switch o.Durability {
	case SyncEveryWrite, Buffered:
	default:
		return fmt.Errorf("wal: options: invalid Durability %d", o.Durability)
	}
	return nil
}

// WalFileName is the conventional WAL filename under a data directory.
const WalFileName = "data.wal"
