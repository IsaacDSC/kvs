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

	rn := &RaftNode{rw: rw, nextIndex: len(entries)}

	node := raft.NewNodeWithState(id, peers, transport, logger, raft.PersistedState{
		Log:         log,
		CurrentTerm: meta.CurrentTerm,
		VotedFor:    meta.VotedFor,
	})

	// Persist currentTerm and votedFor whenever they change so that on restart
	// the node starts with the correct stable state (Raft §5.1/§5.2).
	node.SetMetaPersister(func(term int, votedFor string) {
		if err := rw.SaveMeta(term, votedFor); err != nil {
			logger.Error("raft: failed to persist meta", "term", term, "error", err)
		}
	})

	rn.Node = node
	return rn, nil
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
// For each entry it calls apply then persist. Apply-before-persist ensures
// that a crash between the two steps is safe: on restart the entry is not in
// raft.wal so the leader re-sends it and it is re-applied (all state-machine
// operations are idempotent). The opposite order (persist-then-apply) would
// lose the entry permanently — on restart commitIndex already covers it so
// runDelivery never re-delivers it.
func (rn *RaftNode) RunAppliedLoop(ctx context.Context, apply func(context.Context, raft.LogEntry) error, onFatal func(error)) {
	go func() {
		for {
			select {
			case entry := <-rn.Applied():
				if err := apply(ctx, entry); err != nil {
					onFatal(err)
					return
				}
				if err := rn.PersistApplied(entry); err != nil {
					onFatal(err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
