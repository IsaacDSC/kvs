package raft

import (
	"log/slog"
	"testing"

	"github.com/IsaacDSC/kvs/internal/commands"
)

func TestNode_validateStrictRepMinAcks_fourServers(t *testing.T) {
	// Four servers total → len(peers) == 3, majority == 3.
	tr := NewTransport()
	n := NewNode("a", []string{"b", "c", "d"}, tr, slog.Default())

	if err := n.validateStrictRepMinAcks(2); err == nil {
		t.Fatal("expected error for minAcks below majority")
	}
	if err := n.validateStrictRepMinAcks(5); err == nil {
		t.Fatal("expected error for minAcks above cluster size")
	}
	for _, acks := range []int{3, 4} {
		if err := n.validateStrictRepMinAcks(acks); err != nil {
			t.Fatalf("minAcks=%d: %v", acks, err)
		}
	}
}

func TestNode_minAcksRequiredToCommit_setCmd(t *testing.T) {
	n := NewNode("a", []string{"b", "c", "d"}, NewTransport(), slog.Default())

	n.log = []LogEntry{{Term: 1, Data: commands.Data{
		Cmd: commands.SetCmd, TableName: "t", MinAcks: 4,
	}}}
	if got := n.minAcksRequiredToCommit(0); got != 4 {
		t.Fatalf("set with MinAcks=4: want 4 got %d", got)
	}
	n.log[0].Data.MinAcks = 0
	if got := n.minAcksRequiredToCommit(0); got != n.effectiveRepMinAcks() {
		t.Fatalf("omit min acks: want effective %d got %d", n.effectiveRepMinAcks(), got)
	}
}

func TestNode_effectiveRepMinAcks_soloVsCluster(t *testing.T) {
	solo := NewNode("solo", nil, NewTransport(), slog.Default())
	if got := solo.effectiveRepMinAcks(); got != 1 {
		t.Fatalf("solo: want 1 got %d", got)
	}
	if got := solo.FullClusterReplicationMinAcks(); got != 1 {
		t.Fatalf("solo FullCluster: want 1 got %d", got)
	}

	n := NewNode("a", []string{"b", "c"}, NewTransport(), slog.Default())
	if got := n.effectiveRepMinAcks(); got != 3 {
		t.Fatalf("3 nodes: want 3 got %d", got)
	}
	if got := n.FullClusterReplicationMinAcks(); got != 3 {
		t.Fatalf("FullCluster: want 3 got %d", got)
	}
}
