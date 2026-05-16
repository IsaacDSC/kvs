package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
)

func TestMain(m *testing.M) {
	if err := cfg.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "cfg.Load: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestLoad_replaysOnlyAfterCheckpointSeq(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")

	opts := Options{
		Durability:      SyncEveryWrite,
		CheckpointDir:   ckptDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
	}
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

// Two backends: prefix targets get a full replay; the last target gets tail-only replay (checkpoint).
func TestLoad_twoBackends_fullThenTail(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")

	opts := Options{
		Durability:      SyncEveryWrite,
		CheckpointDir:   ckptDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
	}
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

	memFull := memdb.NewDB(memdb.Options{})
	memTail := memdb.NewDB(memdb.Options{})
	if err := w2.Load(ctx, memFull, memTail); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := memFull.Get(ctx, tab, k); err != nil {
			t.Fatalf("memFull missing %s: %v", k, err)
		}
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := memTail.Get(ctx, tab, k); err == nil {
			t.Fatalf("memTail should not have %s after tail-only replay", k)
		}
	}
	if _, err := memTail.Get(ctx, tab, "k3"); err != nil {
		t.Fatalf("memTail missing k3: %v", err)
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
		Durability:       SyncEveryWrite,
		CheckpointDir:    ckptDir,
		CheckpointPolicy: CheckpointPolicy{TruncateAfterCheckpoint: true},
		CheckpointStore:  durable.NewFileCheckpointStore(),
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

func TestLoad_snapshotHydratesThenTailOnTwoBackends(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")

	opts := Options{
		Durability:      SyncEveryWrite,
		CheckpointDir:   ckptDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
	}
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

	snapMem := memdb.NewDB(memdb.Options{})
	if err := snapMem.CreateTable(tab); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2"} {
		if err := snapMem.Set(ctx, tab, item.Entity{Key: k, Value: k}); err != nil {
			t.Fatal(err)
		}
	}
	if err := durable.SaveCheckpointMemdb(ckptDir, snapMem, 2); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	mem := memdb.NewDB(memdb.Options{})
	tail := memdb.NewDB(memdb.Options{})
	if err := w2.Load(ctx, mem, tail); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := mem.Get(ctx, tab, k); err != nil {
			t.Fatalf("mem missing %s: %v", k, err)
		}
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := tail.Get(ctx, tab, k); err == nil {
			t.Fatalf("tail should not have %s", k)
		}
	}
	if _, err := tail.Get(ctx, tab, "k3"); err != nil {
		t.Fatalf("tail missing k3: %v", err)
	}
}

func TestLoad_repairTruncatedTailWithPartialReplay(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")
	opts := Options{
		Durability:      SyncEveryWrite,
		CheckpointDir:   ckptDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
	}
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

	b, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 8 {
		t.Fatal("wal too small")
	}
	if err := os.WriteFile(walPath, b[:len(b)-5], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RepairTruncatesTail(walPath); err != nil {
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
	if _, err := db.Get(ctx, tab, "k3"); err == nil {
		t.Fatal("expected incomplete k3 record dropped after repair")
	}
}

func TestCheckpoint_everyNWrites(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")
	opts := Options{
		Durability:       SyncEveryWrite,
		CheckpointDir:    ckptDir,
		CheckpointPolicy: CheckpointPolicy{EveryNWrites: 2},
		CheckpointStore:  durable.NewFileCheckpointStore(),
	}
	w, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Set(ctx, "t", item.Entity{Key: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Set(ctx, "t", item.Entity{Key: "b", Value: 2}); err != nil {
		t.Fatal(err)
	}
	seq, err := durable.LoadLastSeq(ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Fatalf("LoadLastSeq after every-2-writes checkpoint: got %d want 2", seq)
	}
}

func TestCheckpoint_maxWalBytes(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	ckptDir := filepath.Join(dir, "ckpt")
	opts := Options{
		Durability:       SyncEveryWrite,
		CheckpointDir:    ckptDir,
		CheckpointPolicy: CheckpointPolicy{MaxWalBytes: 64},
		CheckpointStore:  durable.NewFileCheckpointStore(),
	}
	w, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Set(ctx, "t", item.Entity{Key: "a", Value: make([]byte, 128)}); err != nil {
		t.Fatal(err)
	}
	seq, err := durable.LoadLastSeq(ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("LoadLastSeq after size-triggered checkpoint: got %d want 1", seq)
	}
}
