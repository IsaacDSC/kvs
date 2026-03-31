package store

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/IsaacDSC/kvs/internal/db"
)

func TestOpenCreatesLayoutAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, WalFileName)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReopenRecoversPuts(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		key := strconv.Itoa(i)
		if err := s.Put("users", db.Item{Key: key, Fk: "g", Value: key + "-v"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	tb := s2.DB().GetOrCreateTable("users")
	for i := 0; i < 20; i++ {
		key := strconv.Itoa(i)
		v, err := tb.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != key+"-v" {
			t.Fatalf("key %s: got %v", key, v)
		}
	}
}

func TestReopenRecoversDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Put("t", db.Item{Key: "a", Fk: "x", Value: "1"})
	_ = s.Put("t", db.Item{Key: "b", Fk: "x", Value: "2"})
	if err := s.Delete("t", "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	tb := s2.DB().GetOrCreateTable("t")
	if _, err := tb.Get("a"); err == nil {
		t.Fatal("a should be gone")
	}
	v, err := tb.Get("b")
	if err != nil || v.(string) != "2" {
		t.Fatalf("b: %v %v", v, err)
	}
}

func TestCorruptTailTruncatedOnOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("t", db.Item{Key: "k", Fk: "f", Value: "v42"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, WalFileName)
	b, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, append(b, 0xde, 0xad, 0xfe), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v, err := s2.DB().GetOrCreateTable("t").Get("k")
	if err != nil || v.(string) != "v42" {
		t.Fatalf("recovery failed: %v %v", v, err)
	}
}

func TestPutEmptyTableName(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Put("", db.Item{Key: "k", Value: 1}); err != ErrEmptyTableName {
		t.Fatalf("got %v", err)
	}
}

func TestSyncEveryWriteAfterSyncCount(t *testing.T) {
	dir := t.TempDir()
	var n atomic.Int32
	s, err := Open(dir, Options{Durability: SyncEveryWrite, AfterSync: func() { n.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 4; i++ {
		if err := s.Put("t", db.Item{Key: strconv.Itoa(i), Fk: "x", Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	if got := n.Load(); got < 4 {
		t.Fatalf("expected at least 4 fsync hooks, got %d", got)
	}
}

func TestBufferedSyncsOnFlushAndClose(t *testing.T) {
	dir := t.TempDir()
	var n atomic.Int32
	s, err := Open(dir, Options{Durability: Buffered, AfterSync: func() { n.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Put("t", db.Item{Key: strconv.Itoa(i), Fk: "x", Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != 0 {
		t.Fatalf("no sync before flush, got %d", n.Load())
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if n.Load() < 1 {
		t.Fatalf("expected sync after flush")
	}
	before := n.Load()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n.Load() <= before {
		t.Fatalf("close should sync again")
	}
}

func TestCheckpointSkipsEarlierWALOnReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Put("tbl", db.Item{Key: strconv.Itoa(i), Fk: "x", Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	if s2.LastReplayApplied != 0 {
		t.Fatalf("after checkpoint reopen should apply 0 wal tail, got %d", s2.LastReplayApplied)
	}
	if err := s2.Put("tbl", db.Item{Key: "extra", Fk: "x", Value: 99}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if s3.LastReplayApplied != 1 {
		t.Fatalf("want 1 wal entry applied, got %d", s3.LastReplayApplied)
	}
	ev, err := s3.DB().GetOrCreateTable("tbl").Get("extra")
	if err != nil {
		t.Fatal(err)
	}
	var extra int
	switch x := ev.(type) {
	case int:
		extra = x
	case int64:
		extra = int(x)
	case uint64:
		extra = int(x)
	default:
		t.Fatalf("unexpected type %T for extra", ev)
	}
	if extra != 99 {
		t.Fatalf("extra: got %d", extra)
	}
}

func TestMultiTableIsolation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Put("a", db.Item{Key: "1", Fk: "x", Value: "a1"})
	_ = s.Put("b", db.Item{Key: "1", Fk: "y", Value: "b1"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	va, _ := s2.DB().GetOrCreateTable("a").Get("1")
	vb, _ := s2.DB().GetOrCreateTable("b").Get("1")
	if va.(string) != "a1" || vb.(string) != "b1" {
		t.Fatalf("va=%v vb=%v", va, vb)
	}
}

func TestDoubleReopenIdempotentState(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, Options{Durability: SyncEveryWrite})
	_ = s.Put("t", db.Item{Key: "k", Fk: "f", Value: "v"})
	_ = s.Close()
	s2, _ := Open(dir, Options{Durability: SyncEveryWrite})
	_ = s2.Close()
	s3, _ := Open(dir, Options{Durability: SyncEveryWrite})
	defer s3.Close()
	v, err := s3.DB().GetOrCreateTable("t").Get("k")
	if err != nil || v.(string) != "v" {
		t.Fatalf("%v %v", v, err)
	}
}
