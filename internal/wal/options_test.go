package wal

import (
	"strings"
	"testing"
)

func TestOptions_Validate_truncateRequiresDir(t *testing.T) {
	o := Options{
		Durability:       SyncEveryWrite,
		CheckpointPolicy: CheckpointPolicy{TruncateAfterCheckpoint: true},
	}
	err := o.Validate()
	if err == nil {
		t.Fatal("expected error when TruncateAfterCheckpoint without CheckpointDir")
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
	if (Options{}).CheckpointConfigured() {
		t.Fatal("empty CheckpointDir should not be configured")
	}
	if !(Options{CheckpointDir: "/x"}).CheckpointConfigured() {
		t.Fatal("non-empty CheckpointDir should be configured")
	}
}
