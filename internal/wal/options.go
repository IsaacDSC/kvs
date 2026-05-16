package wal

import (
	"context"
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

// CheckpointPolicy controls optional automatic checkpoint triggers and WAL truncation.
// Checkpoint metadata path is Options.CheckpointDir (checkpoint.cbor under that directory).
type CheckpointPolicy struct {
	// EveryNWrites triggers Checkpoint after this many WAL appends since the last successful checkpoint (0 disables).
	EveryNWrites uint64
	// MaxWalBytes triggers Checkpoint after this many payload bytes appended since the last successful checkpoint (0 disables).
	MaxWalBytes int64
	// TruncateAfterCheckpoint, when true, truncates the WAL file to empty after a successful
	// Checkpoint() save. New writes continue with Seq > previous LastSeq; Load uses max(lastSeq, maxSeqInFile).
	TruncateAfterCheckpoint bool
}

// Options configures WAL behavior.
type Options struct {
	Durability Durability
	// AfterSync is invoked after each successful WAL fsync (SyncEveryWrite or Flush). Optional, for tests.
	AfterSync func()
	// CheckpointDir holds checkpoint.cbor (see internal/durable). Empty: Load replays the full WAL.
	CheckpointDir string
	// CheckpointPolicy optional auto-checkpoint and truncate flags when CheckpointDir is set.
	CheckpointPolicy CheckpointPolicy
	// CheckpointStore persists LastSeq (and optional table data in checkpoint files). Required when CheckpointDir is set.
	CheckpointStore CheckpointStore
	// BeforeCheckpoint runs after replay and before SaveLastSeq when checkpoint is configured.
	// Use to flush deferred stores (e.g. fsdb WriteBatcher) so on-disk state matches w.seq.
	BeforeCheckpoint func(context.Context) error
}

// CheckpointConfigured reports whether checkpoint metadata (LastSeq) is loaded and saved.
func (o Options) CheckpointConfigured() bool {
	return o.CheckpointDir != ""
}

// Validate checks option combinations. New and Open call this before use.
func (o Options) Validate() error {
	if o.CheckpointConfigured() && o.CheckpointStore == nil {
		return errors.New("wal: options: CheckpointDir requires non-empty CheckpointStore")
	}
	if o.CheckpointPolicy.TruncateAfterCheckpoint && !o.CheckpointConfigured() {
		return errors.New("wal: options: TruncateAfterCheckpoint requires non-empty CheckpointDir")
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
