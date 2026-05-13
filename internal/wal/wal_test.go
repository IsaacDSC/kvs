package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
)

func TestLoad_replaysOnlyAfterCheckpointSeq(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")

	opts := Options{Durability: SyncEveryWrite, Checkpoint: CheckpointConfig{Dir: ckptDir}}
	w, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}

	tab := "t"
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := w.Set(ctx, tab, item.Entity{Key: k, Value: k}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := durable.SaveLastSeq(ckptDir, 2); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	db := memdb.NewDB(memdb.Options{})
	if err := w2.Load(ctx, db); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get(ctx, tab, "k1"); err == nil {
		t.Fatal("expected k1 missing after partial replay")
	}
	if _, err := db.Get(ctx, tab, "k2"); err == nil {
		t.Fatal("expected k2 missing after partial replay")
	}
	got, err := db.Get(ctx, tab, "k3")
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "k3" {
		t.Fatalf("k3: %+v", got)
	}
	if w2.seq != 3 {
		t.Fatalf("w.seq after load: got %d want 3", w2.seq)
	}
}

func TestLoad_fullReplayWhenNoCheckpointDir(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	w, err := New(walPath, Options{Durability: SyncEveryWrite}, codec)
	if err != nil {
		t.Fatal(err)
	}
	tab := "t"
	if err := w.Set(ctx, tab, item.Entity{Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, Options{Durability: SyncEveryWrite}, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	db := memdb.NewDB(memdb.Options{})
	if err := w2.Load(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, tab, "a"); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpoint_requiresDir(t *testing.T) {
	w, err := New(filepath.Join(t.TempDir(), "x.wal"), Options{Durability: SyncEveryWrite}, code.NewCBOR())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Checkpoint(); err == nil {
		t.Fatal("expected error when checkpoint dir is empty")
	}
}

func TestCheckpoint_truncateLeavesSeqForAppend(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")
	opts := Options{
		Durability: SyncEveryWrite,
		Checkpoint: CheckpointConfig{Dir: ckptDir, TruncateAfterCheckpoint: true},
	}
	w, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(ctx, "t", item.Entity{Key: "x", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 0 {
		t.Fatalf("wal size after truncate: got %d want 0", st.Size())
	}
	seq, err := durable.LoadLastSeq(ckptDir)
	if err != nil || seq != 1 {
		t.Fatalf("checkpoint seq: %v %d", err, seq)
	}
	if w.seq != 1 {
		t.Fatalf("w.seq: got %d want 1", w.seq)
	}
	if err := w.Set(ctx, "t", item.Entity{Key: "y", Value: 2}); err != nil {
		t.Fatal(err)
	}
	if w.seq != 2 {
		t.Fatalf("after append seq: got %d want 2", w.seq)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	db := memdb.NewDB(memdb.Options{})
	if err := w2.Load(ctx, db); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(ctx, "t", "y")
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "y" {
		t.Fatalf("got %+v", got)
	}
}
