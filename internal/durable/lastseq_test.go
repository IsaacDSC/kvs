package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cfg"
)

func TestMain(m *testing.M) {
	if err := cfg.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "cfg.Load: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestLoadLastSeq_missingFile(t *testing.T) {
	dir := t.TempDir()
	seq, err := LoadLastSeq(dir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("got seq %d, want 0", seq)
	}
}

func TestSaveLastSeq_roundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := SaveLastSeq(dir, 42); err != nil {
		t.Fatal(err)
	}
	seq, err := LoadLastSeq(dir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 {
		t.Fatalf("got %d, want 42", seq)
	}
}

func TestLoadLastSeq_corruptFile(t *testing.T) {
	conf := cfg.Get()

	dir := t.TempDir()
	path := filepath.Join(dir, conf.CheckpointFileName)
	if err := os.WriteFile(path, []byte("not-cbor"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadLastSeq(dir)
	if err == nil {
		t.Fatal("expected error for corrupt checkpoint")
	}
}
