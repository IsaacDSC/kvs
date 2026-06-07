package api

import (
	"context"
	"testing"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/internal/node"
	"github.com/IsaacDSC/kvs/internal/raft"
)

// ─── test doubles ────────────────────────────────────────────────────────────

type replicateDbMock struct {
	bulkDelCalls int
	gotTable     string
	gotDelItems  dto.DeleteItems
}

func (m *replicateDbMock) CreateTable(string) error { return nil }

func (m *replicateDbMock) ApplyReplicated(context.Context, string, dto.Item) error { return nil }

func (m *replicateDbMock) ApplyReplicatedBulk(context.Context, string, dto.Items) error { return nil }

func (m *replicateDbMock) ApplyReplicatedBulkDelete(_ context.Context, tableName string, its dto.DeleteItems) error {
	m.bulkDelCalls++
	m.gotTable = tableName
	m.gotDelItems = its
	return nil
}

func (m *replicateDbMock) ApplyReplicatedDelete(context.Context, string, dto.DeleteItem) error {
	return nil
}

type raftNodeMock struct{ role string }

func (m *raftNodeMock) NextIndex() int    { return 1 }
func (m *raftNodeMock) State() node.State { return node.State{Role: m.role} }

type nopLog struct{}

func (nopLog) Info(string, ...any) {}

// ─── tests ───────────────────────────────────────────────────────────────────

// A committed bulk_del entry on a follower lands on ApplyReplicatedBulkDelete with
// the Raft-transported items converted back to delete items.
func TestGrpcHandle_bulkDelAppliesOnFollower(t *testing.T) {
	database := &replicateDbMock{}
	handle := GrpcHandle(nopLog{}, database, &raftNodeMock{role: raft.Follower.String()})

	entry := raft.LogEntry{Data: commands.Data{
		Cmd:       commands.BulkDelCmd,
		TableName: "t",
		Items:     dto.Items{{Key: "k1"}, {Key: "k2"}},
	}}
	if err := handle(context.Background(), entry); err != nil {
		t.Fatalf("GrpcHandle: %v", err)
	}

	if database.bulkDelCalls != 1 {
		t.Fatalf("ApplyReplicatedBulkDelete calls = %d, want 1", database.bulkDelCalls)
	}
	if database.gotTable != "t" {
		t.Fatalf("table = %q, want %q", database.gotTable, "t")
	}
	if len(database.gotDelItems) != 2 || database.gotDelItems[0].Key != "k1" || database.gotDelItems[1].Key != "k2" {
		t.Fatalf("delete items = %#v", database.gotDelItems)
	}
}

// The leader applied the batch eagerly on propose: the applied loop must not reapply.
func TestGrpcHandle_bulkDelSkippedOnLeader(t *testing.T) {
	database := &replicateDbMock{}
	handle := GrpcHandle(nopLog{}, database, &raftNodeMock{role: raft.Leader.String()})

	entry := raft.LogEntry{Data: commands.Data{
		Cmd:       commands.BulkDelCmd,
		TableName: "t",
		Items:     dto.Items{{Key: "k1"}},
	}}
	if err := handle(context.Background(), entry); err != nil {
		t.Fatalf("GrpcHandle: %v", err)
	}
	if database.bulkDelCalls != 0 {
		t.Fatalf("leader must not reapply, got %d calls", database.bulkDelCalls)
	}
}
