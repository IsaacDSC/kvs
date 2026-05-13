package wal

import (
	"strings"
	"testing"
)

func TestOptions_Validate_truncateRequiresDir(t *testing.T) {
	o := Options{
		Durability: SyncEveryWrite,
		Checkpoint: CheckpointConfig{TruncateAfterCheckpoint: true},
	}
	err := o.Validate()
	if err == nil {
		t.Fatal("expected error when TruncateAfterCheckpoint without Dir")
	}
	if !strings.Contains(err.Error(), "TruncateAfterCheckpoint") {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestOptions_Validate_invalidDurability(t *testing.T) {
	o := Options{Durability: Durability(99)}
	if err := o.Validate(); err == nil {
		t.Fatal("expected error for invalid Durability")
	}
}

func TestOptions_CheckpointConfigured(t *testing.T) {
	if (Options{Checkpoint: CheckpointConfig{}}).CheckpointConfigured() {
		t.Fatal("empty dir should not be configured")
	}
	if !(Options{Checkpoint: CheckpointConfig{Dir: "/x"}}).CheckpointConfigured() {
		t.Fatal("non-empty dir should be configured")
	}
}
