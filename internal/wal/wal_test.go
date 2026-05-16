package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/item"
)

func TestMain(m *testing.M) {
	if err := cfg.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "cfg.Load: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// seedPersistedPrefix materialises WAL entries with Seq <= checkpoint on fsdb before tail replay tests.
func seedPersistedPrefix(ctx context.Context, fsRoot string, tab string, keys []string) (*fsdb.Db, error) {
	d := fsdb.NewDb(fsRoot)
	if err := d.CreateTable(tab); err != nil {
		return nil, err
	}
	for _, k := range keys {
		if err := d.Set(ctx, tab, item.Entity{Key: k, Value: k}); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func TestLoad_requiresExactlyOneOperations(t *testing.T) {
	ctx := context.Background()
	codec := code.NewCBOR()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	w2, err := New(walPath, Options{Durability: SyncEveryWrite}, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	a := fsdb.NewDb(filepath.Join(dir, "a"))
	b := fsdb.NewDb(filepath.Join(dir, "b"))
	if err := w2.Load(ctx, a, b); err == nil {
		t.Fatal("expected error for multiple ops targets")
	}
	if err := w2.Load(ctx); err == nil {
		t.Fatal("expected error for zero ops targets")
	} else if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("unexpected error: %v", err)
	}
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

	fsRoot := filepath.Join(dir, "fsdb")
	dd, err := seedPersistedPrefix(ctx, fsRoot, tab, []string{"k1", "k2"})
	if err != nil {
		t.Fatal(err)
	}

	if err := w2.Load(ctx, dd); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := dd.Get(ctx, tab, k); err != nil {
			t.Fatalf("missing %s: %v", k, err)
		}
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

	dbInst := fsdb.NewDb(filepath.Join(dir, "fs"))
	if err := w2.Load(ctx, dbInst); err != nil {
		t.Fatal(err)
	}
	if _, err := dbInst.Get(ctx, tab, "a"); err != nil {
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

	dbInst := fsdb.NewDb(filepath.Join(dir, "fs-empty"))
	if err := w2.Load(ctx, dbInst); err != nil {
		t.Fatal(err)
	}
	got, err := dbInst.Get(ctx, "t", "y")
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "y" {
		t.Fatalf("got %+v", got)
	}
}

func TestLoad_snapshotHydratesThenTail(t *testing.T) {
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

	blobs := make(map[string][]byte)
	for _, k := range []string{"k1", "k2"} {
		raw, encErr := codec.Encode(item.Entity{Key: k, Value: k})
		if encErr != nil {
			t.Fatal(encErr)
		}
		blobs[k] = raw
	}
	if err := durable.SaveCheckpointTableBlobs(ckptDir, 2, map[string]map[string][]byte{tab: blobs}); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	dbInst := fsdb.NewDb(filepath.Join(dir, "fs-restore"))
	if err := w2.Load(ctx, dbInst); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := dbInst.Get(ctx, tab, k); err != nil {
			t.Fatalf("missing %s: %v", k, err)
		}
	}
}

func TestLoad_writeBatcherImplementsCheckpointHydrator(t *testing.T) {
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
	for _, k := range []string{"k1"} {
		if err := w.Set(ctx, tab, item.Entity{Key: k, Value: k}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := codec.Encode(item.Entity{Key: "k1", Value: "from-checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.SaveCheckpointTableBlobs(ckptDir, 1, map[string]map[string][]byte{tab: {"k1": raw}}); err != nil {
		t.Fatal(err)
	}

	w2, err := New(walPath, opts, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	inner := fsdb.NewDb(filepath.Join(dir, "fs-inner"))
	batched := fsdb.NewWriteBatcher(inner, fsdb.WriteBatcherOptions{})
	if err := w2.Load(ctx, batched); err != nil {
		t.Fatal(err)
	}
	got, err := batched.Get(ctx, tab, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "from-checkpoint" {
		t.Fatalf("hydrated Value: %#v", got.Value)
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

	fsRoot := filepath.Join(dir, "fsdb-seed")
	dd, err := seedPersistedPrefix(ctx, fsRoot, tab, []string{"k1", "k2"})
	if err != nil {
		t.Fatal(err)
	}

	if err := w2.Load(ctx, dd); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := dd.Get(ctx, tab, k); err != nil {
			t.Fatalf("missing seeded %s: %v", k, err)
		}
	}
	if _, err := dd.Get(ctx, tab, "k3"); err == nil {
		t.Fatal("expected incomplete k3 record dropped after repair")
	} else if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unexpected err for missing k3: %v", err)
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
