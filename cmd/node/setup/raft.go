// Package setup centralises node startup wiring for the kvs binary.
// It is intentionally cmd-level code: not a reusable library, only a binary concern.
package setup

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/internal/wal"
)

// RaftNode is a Raft consensus node backed by a durable write-ahead log.
// Embedding *raft.Node promotes Run, Applied, State and all other node methods.
// The WAL ensures commitIndex and lastApplied are correctly restored after
// a restart, so the leader never re-delivers already-applied entries.
type RaftNode struct {
	*raft.Node
	rw        *wal.RaftWAL
	nextIndex int
}

// OpenRaft opens (or creates) the Raft WAL at nodeDir/raft.wal, restores
// persisted log entries and stable state (currentTerm, votedFor), and returns
// a node ready to call Run on.
func OpenRaft(nodeDir, id string, peers []string, transport *raft.Transport, logger *slog.Logger, codec wal.Codec) (*RaftNode, error) {
	walPath := filepath.Join(nodeDir, wal.RaftWALFileName)
	rw, err := wal.OpenRaftWAL(walPath, codec)
	if err != nil {
		return nil, fmt.Errorf("setup: raft wal open: %w", err)
	}

	entries, meta, err := rw.Load()
	if err != nil {
		_ = rw.Close()
		return nil, fmt.Errorf("setup: raft wal load: %w", err)
	}

	logger.Info("raft wal loaded", "entries", len(entries), "term", meta.CurrentTerm, "voted_for", meta.VotedFor)

	log := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		log[i] = raft.LogEntry{Term: e.Term, Data: e.Data}
	}

	node := raft.NewNodeWithState(id, peers, transport, logger, raft.PersistedState{
		Log:         log,
		CurrentTerm: meta.CurrentTerm,
		VotedFor:    meta.VotedFor,
	})

	return &RaftNode{Node: node, rw: rw, nextIndex: len(entries)}, nil
}

// PersistApplied durably records the applied entry in the Raft WAL.
// Call this in the Applied() loop before (or immediately after) committing the
// entry to the KV state machine so that on restart the log is complete.
func (rn *RaftNode) PersistApplied(entry raft.LogEntry) error {
	if err := rn.rw.AppendEntry(rn.nextIndex, entry.Term, entry.Data); err != nil {
		return fmt.Errorf("setup: raft persist index=%d: %w", rn.nextIndex, err)
	}
	rn.nextIndex++
	return nil
}

// NextIndex returns the index that will be assigned to the next persisted entry.
func (rn *RaftNode) NextIndex() int {
	return rn.nextIndex
}

// Close flushes and closes the Raft WAL.
// The embedded *raft.Node does not own any resources; only the WAL needs closing.
func (rn *RaftNode) Close() error {
	return rn.rw.Close()
}

// RunAppliedLoop processes entries from Applied() until ctx is cancelled.
// For each entry it calls persist then apply; either function returning an
// error is treated as fatal and calls onFatal with the error.
func (rn *RaftNode) RunAppliedLoop(ctx context.Context, apply func(context.Context, raft.LogEntry) error, onFatal func(error)) {
	go func() {
		for {
			select {
			case entry := <-rn.Applied():
				if err := rn.PersistApplied(entry); err != nil {
					onFatal(err)
					return
				}
				if err := apply(ctx, entry); err != nil {
					onFatal(err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
