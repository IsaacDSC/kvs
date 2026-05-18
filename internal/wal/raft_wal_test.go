package wal_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/internal/wal"
)

func init() {
	_ = cfg.Load()
}

func openRaftWAL(t *testing.T) (*wal.RaftWAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), wal.RaftWALFileName)
	rw, err := wal.OpenRaftWAL(path, code.NewCBOR())
	if err != nil {
		t.Fatalf("OpenRaftWAL: %v", err)
	}
	t.Cleanup(func() { _ = rw.Close() })
	return rw, path
}

// TestRaftWAL_roundTrip verifies that entries and meta written survive a
// reopen (simulating a process restart).
func TestRaftWAL_roundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, wal.RaftWALFileName)

	entries := []wal.RaftEntry{
		{Index: 0, Term: 1, Data: commands.Data{Cmd: commands.CreateTableCmd, TableName: "t"}},
		{Index: 1, Term: 1, Data: commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "k1", SK: "f", Value: map[string]any{"v": 1}}}},
		{Index: 2, Term: 2, Data: commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "k2", SK: "f", Value: map[string]any{"v": 2}}}},
	}
	meta := wal.RaftMeta{CurrentTerm: 2, VotedFor: "node1"}

	// ── Write ─────────────────────────────────────────────────────────────
	rw1, err := wal.OpenRaftWAL(path, code.NewCBOR())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := rw1.AppendEntry(e.Index, e.Term, e.Data); err != nil {
			t.Fatalf("AppendEntry index=%d: %v", e.Index, err)
		}
	}
	if err := rw1.SaveMeta(meta.CurrentTerm, meta.VotedFor); err != nil {
		t.Fatalf("SaveMeta: %v", err)
	}
	_ = rw1.Close()

	// ── Reopen and load ───────────────────────────────────────────────────
	rw2, err := wal.OpenRaftWAL(path, code.NewCBOR())
	if err != nil {
		t.Fatal(err)
	}
	defer rw2.Close()

	got, gotMeta, err := rw2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("entries: got %d want %d", len(got), len(entries))
	}
	for i, want := range entries {
		if got[i].Index != want.Index || got[i].Term != want.Term {
			t.Errorf("entry[%d]: got {Index:%d Term:%d} want {Index:%d Term:%d}",
				i, got[i].Index, got[i].Term, want.Index, want.Term)
		}
		if string(got[i].Data.Cmd) != string(want.Data.Cmd) {
			t.Errorf("entry[%d].Cmd: got %q want %q", i, got[i].Data.Cmd, want.Data.Cmd)
		}
	}
	if gotMeta.CurrentTerm != meta.CurrentTerm || gotMeta.VotedFor != meta.VotedFor {
		t.Errorf("meta: got %+v want %+v", gotMeta, meta)
	}
}

// TestRaftWAL_appendAfterLoad verifies the seq counter is correctly restored so
// that entries appended after a reopen are still monotonic and survive a second
// reopen.
func TestRaftWAL_appendAfterLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, wal.RaftWALFileName)

	rw1, _ := wal.OpenRaftWAL(path, code.NewCBOR())
	_ = rw1.AppendEntry(0, 1, commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "a", Value: map[string]any{"x": 1}}})
	_ = rw1.Close()

	rw2, _ := wal.OpenRaftWAL(path, code.NewCBOR())
	entries, _, err := rw2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// append a second entry after reload
	_ = rw2.AppendEntry(1, 1, commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "b", Value: map[string]any{"x": 2}}})
	_ = rw2.Close()

	rw3, _ := wal.OpenRaftWAL(path, code.NewCBOR())
	defer rw3.Close()
	all, _, err := rw3.Load()
	if err != nil {
		t.Fatalf("Load after append: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("want 2 entries after reload+append, got %d", len(all))
	}
	_ = entries
}

// TestRaftWAL_emptyFile verifies a fresh node starts without error.
func TestRaftWAL_emptyFile(t *testing.T) {
	rw, _ := openRaftWAL(t)
	entries, meta, err := rw.Load()
	if err != nil {
		t.Fatalf("Load on empty file: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("want 0 entries, got %d", len(entries))
	}
	if meta.CurrentTerm != 0 || meta.VotedFor != "" {
		t.Fatalf("want zero meta, got %+v", meta)
	}
}

// TestNewNodeWithState_noRedelivery is the core regression test: a node
// restored with a non-empty log must NOT deliver already-applied entries
// through the Applied() channel when it receives a heartbeat from the leader.
func TestNewNodeWithState_noRedelivery(t *testing.T) {
	// Seed a Raft WAL as if two entries were already committed and applied.
	dir := t.TempDir()
	rw, err := wal.OpenRaftWAL(filepath.Join(dir, wal.RaftWALFileName), code.NewCBOR())
	if err != nil {
		t.Fatal(err)
	}
	_ = rw.AppendEntry(0, 1, commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "k1", Value: map[string]any{"v": 1}}})
	_ = rw.AppendEntry(1, 1, commands.Data{Cmd: commands.SetCmd, TableName: "t", Item: dto.Item{Key: "k2", Value: map[string]any{"v": 2}}})
	_ = rw.Close()

	// Reload
	rw2, _ := wal.OpenRaftWAL(filepath.Join(dir, wal.RaftWALFileName), code.NewCBOR())
	defer rw2.Close()
	restored, meta, _ := rw2.Load()

	raftLog := make([]raft.LogEntry, len(restored))
	for i, re := range restored {
		raftLog[i] = raft.LogEntry{Term: re.Term, Data: re.Data}
	}

	transport := raft.NewTransport()
	node := raft.NewNodeWithState("node1", nil, transport, nil, raft.PersistedState{
		Log:         raftLog,
		CurrentTerm: meta.CurrentTerm,
		VotedFor:    meta.VotedFor,
	})

	// Simulate the leader delivering an AppendEntries heartbeat that covers
	// only the already-applied entries (leaderCommit = 1 = last applied index).
	reply := &raft.AppendEntriesReply{}
	node.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 1, // matches our last entry
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 1,
	}, reply)

	if !reply.Success {
		t.Fatal("expected Success=true for a heartbeat that matches restored log")
	}

	// The Applied() channel must be empty — no re-delivery.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	select {
	case e := <-node.Applied():
		t.Fatalf("unexpected re-delivery of already-applied entry: %+v", e)
	case <-ctx.Done():
		// correct: nothing was re-delivered
	}

	// Cleanup: delete tmp dir files created by the test (OS handles via t.TempDir()).
	_ = os.RemoveAll(dir)
}
