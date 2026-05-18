package wal

import (
	"fmt"
	"sort"

	"github.com/IsaacDSC/kvs/internal/commands"
)

// RaftWALFileName is the conventional filename for the Raft log WAL,
// stored alongside data.wal in the same node directory.
const RaftWALFileName = "raft.wal"

// raftTable is the reserved WAL table name used for all Raft entries.
// It must never conflict with user-created KV tables.
const raftTable = "__raft__"

// RaftEntry is one committed+applied Raft log entry recovered from the WAL.
type RaftEntry struct {
	Index int
	Term  int
	Data  commands.Data
}

// RaftMeta holds the Raft stable state that must survive restarts.
// currentTerm and votedFor are required by §5.1/§5.2 of the Raft paper.
type RaftMeta struct {
	CurrentTerm int
	VotedFor    string
}

// raftLogPayload is the CBOR-encoded body of an OpRaftLog frame.
type raftLogPayload struct {
	Index int           `cbor:"i"`
	Term  int           `cbor:"t"`
	Data  commands.Data `cbor:"d"`
}

// raftMetaPayload is the CBOR-encoded body of an OpRaftMeta frame.
type raftMetaPayload struct {
	CurrentTerm int    `cbor:"ct"`
	VotedFor    string `cbor:"vf"`
}

// RaftWAL persists committed Raft log entries and stable state (currentTerm,
// votedFor) in a dedicated append-only file.
//
// Entries are fsynced before AppendEntry/SaveMeta return, satisfying the Raft
// requirement that stable state is durable before responding to RPCs.
//
// On node startup call Load to reconstruct the log and meta, then pass the
// result to raft.NewNodeWithState so the node starts with the correct
// commitIndex/lastApplied and avoids re-delivering already-applied entries.
type RaftWAL struct {
	w   *WAL
	seq uint64 // monotonic counter for Entry.Seq; separate from the KV WAL seq
}

// OpenRaftWAL opens (or creates) the Raft WAL at path.
// The codec must be the same CBOR codec used by the rest of the node.
func OpenRaftWAL(path string, codec Codec) (*RaftWAL, error) {
	w, err := New(path, Options{Durability: SyncEveryWrite}, codec)
	if err != nil {
		return nil, fmt.Errorf("raft wal: open %q: %w", path, err)
	}
	return &RaftWAL{w: w}, nil
}

// AppendEntry persists a committed Raft log entry with fsync.
// Call this after the entry has been successfully applied to the KV state
// machine so that on restart the persisted log exactly matches what was applied.
func (r *RaftWAL) AppendEntry(index, term int, data commands.Data) error {
	b, err := r.w.codec.Encode(raftLogPayload{Index: index, Term: term, Data: data})
	if err != nil {
		return fmt.Errorf("raft wal: encode entry index=%d: %w", index, err)
	}
	r.seq++
	if err := r.w.Append(Entry{
		Seq:        r.seq,
		Op:         OpRaftLog,
		Table:      raftTable,
		ValueBytes: b,
	}); err != nil {
		r.seq-- // rollback on failure
		return fmt.Errorf("raft wal: append entry index=%d: %w", index, err)
	}
	return nil
}

// SaveMeta persists currentTerm and votedFor with fsync.
// Should be called whenever these values change inside the Raft node
// (before responding to RequestVote / AppendEntries RPCs per §5.1, §5.2).
func (r *RaftWAL) SaveMeta(currentTerm int, votedFor string) error {
	b, err := r.w.codec.Encode(raftMetaPayload{CurrentTerm: currentTerm, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("raft wal: encode meta: %w", err)
	}
	r.seq++
	if err := r.w.Append(Entry{
		Seq:        r.seq,
		Op:         OpRaftMeta,
		Table:      raftTable,
		ValueBytes: b,
	}); err != nil {
		r.seq--
		return fmt.Errorf("raft wal: append meta: %w", err)
	}
	return nil
}

// Load reads the Raft WAL from disk and reconstructs the ordered log and the
// last persisted meta. A missing or empty file returns (nil, RaftMeta{}, nil).
//
// Entries are returned sorted by index. Gaps in the index sequence are treated
// as a corruption error — they indicate a lost write that could cause
// inconsistency between the KV state machine and the Raft log.
func (r *RaftWAL) Load() (entries []RaftEntry, meta RaftMeta, err error) {
	byIndex := make(map[int]RaftEntry)

	maxSeq, err := r.w.Replay(func(e Entry) error {
		switch e.Op {
		case OpRaftLog:
			var p raftLogPayload
			if decErr := r.w.codec.Decode(e.ValueBytes, &p); decErr != nil {
				return fmt.Errorf("raft wal: decode entry seq=%d: %w", e.Seq, decErr)
			}
			byIndex[p.Index] = RaftEntry{Index: p.Index, Term: p.Term, Data: p.Data}
		case OpRaftMeta:
			var p raftMetaPayload
			if decErr := r.w.codec.Decode(e.ValueBytes, &p); decErr != nil {
				return fmt.Errorf("raft wal: decode meta seq=%d: %w", e.Seq, decErr)
			}
			meta = RaftMeta{CurrentTerm: p.CurrentTerm, VotedFor: p.VotedFor}
		}
		return nil
	})
	if err != nil {
		return nil, RaftMeta{}, err
	}

	// Restore the seq counter so future AppendEntry/SaveMeta calls are monotonic.
	r.seq = maxSeq

	if len(byIndex) == 0 {
		return nil, meta, nil
	}

	// Build a contiguous, sorted slice and verify there are no index gaps.
	indices := make([]int, 0, len(byIndex))
	for idx := range byIndex {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	if indices[0] != 0 {
		return nil, RaftMeta{}, fmt.Errorf("raft wal: log does not start at index 0 (got %d)", indices[0])
	}
	for i := 1; i < len(indices); i++ {
		if indices[i] != indices[i-1]+1 {
			return nil, RaftMeta{}, fmt.Errorf("raft wal: gap in log between index %d and %d", indices[i-1], indices[i])
		}
	}

	entries = make([]RaftEntry, len(indices))
	for i, idx := range indices {
		entries[i] = byIndex[idx]
	}
	return entries, meta, nil
}

// Close flushes and closes the underlying WAL file.
func (r *RaftWAL) Close() error {
	return r.w.Close()
}
