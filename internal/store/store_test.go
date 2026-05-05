package store

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
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
	tb0 := s.DB().GetOrCreateTable("users")
	for i := 0; i < 20; i++ {
		key := strconv.Itoa(i)
		if err := tb0.Set(item.Entity{Key: key, SK: "g", Value: key + "-v"}); err != nil {
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
		item, err := tb.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if item.Value.(string) != key+"-v" {
			t.Fatalf("key %s: got %v", key, item.Value)
		}
	}
}

func TestReopenRecoversDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tbd := s.DB().GetOrCreateTable("t")
	_ = tbd.Set(item.Entity{Key: "a", SK: "x", Value: "1"})
	_ = tbd.Set(item.Entity{Key: "b", SK: "x", Value: "2"})
	if err := tbd.Delete("a"); err != nil {
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
	item, err := tb.Get("b")
	if err != nil || item.Value.(string) != "2" {
		t.Fatalf("b: %v %v", item.Value, err)
	}
}

func TestOptimisticPutPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tbw := s.DB().GetOrCreateTable("t")
	if err := tbw.Set(item.Entity{Key: "k", SK: "f", Value: "before"}); err != nil {
		t.Fatal(err)
	}
	item, err := s.DB().GetOrCreateTable("t").Get("k")
	if err != nil {
		t.Fatal(err)
	}
	item.Value = "after"
	tb := s.DB().GetOrCreateTable("t")
	res := tb.OptimisticPut(context.Background(), item, "v1")
	if err := res.Err(); err != nil {
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
	got, err := s2.DB().GetOrCreateTable("t").Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value.(string) != "after" || got.Version != "v1" {
		t.Fatalf("got %+v", got)
	}
}

func TestSessionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tbp := s.DB().GetOrCreateTable("t")
	if err := tbp.Set(item.Entity{Key: "k", SK: "f", Value: "a"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tb := s.DB().GetOrCreateTable("t")
	err = tb.NewSession(ctx, func(tx *memdb.Tx) error {
		it, err := tx.Get("k")
		if err != nil {
			return err
		}
		it.Value = "b"
		return tx.Set(it)
	})
	if err != nil {
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
	got, err := s2.DB().GetOrCreateTable("t").Get("k")
	if err != nil || got.Value.(string) != "b" {
		t.Fatalf("got %v err %v", got.Value, err)
	}
}

func TestOptimisticDeletePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tbo := s.DB().GetOrCreateTable("t")
	if err := tbo.Set(item.Entity{Key: "k", SK: "f", Value: "x", Version: "v1"}); err != nil {
		t.Fatal(err)
	}
	item := item.Entity{Key: "k", SK: "f", Value: "x", Version: "v1"}
	tb := s.DB().GetOrCreateTable("t")
	res := tb.OptimisticDelete(context.Background(), item, "v1")
	if err := res.Err(); err != nil {
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
	if _, err := s2.DB().GetOrCreateTable("t").Get("k"); err == nil {
		t.Fatal("key should be deleted after replay")
	}
}

func TestCorruptTailTruncatedOnOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tbc := s.DB().GetOrCreateTable("t")
	if err := tbc.Set(item.Entity{Key: "k", SK: "f", Value: "v42"}); err != nil {
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
	item, err := s2.DB().GetOrCreateTable("t").Get("k")
	if err != nil || item.Value.(string) != "v42" {
		t.Fatalf("recovery failed: %v %v", item.Value, err)
	}
}

func TestPutEmptyTableName(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := (&storeDurable{s: s}).Put("", item.Entity{Key: "k", Value: 1}); err != ErrEmptyTableName {
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
	tbs := s.DB().GetOrCreateTable("t")
	for i := 0; i < 4; i++ {
		if err := tbs.Set(item.Entity{Key: strconv.Itoa(i), SK: "x", Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	if got := n.Load(); got < 4 {
		t.Fatalf("expected at least 4 fsync hooks, got %d", got)
	}
}

func TestBufferedCommitTransactionFlushes(t *testing.T) {
	dir := t.TempDir()
	var n atomic.Int32
	s, err := Open(dir, Options{Durability: Buffered, AfterSync: func() { n.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tb := s.DB().GetOrCreateTable("t")
	err = tb.NewSession(context.Background(), func(tx *memdb.Tx) error {
		return tx.Set(item.Entity{Key: "k", SK: "x", Value: 1})
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.Load() < 1 {
		t.Fatalf("expected Flush after transactional commit, syncs=%d", n.Load())
	}
}

func TestBufferedSyncsOnFlushAndClose(t *testing.T) {
	dir := t.TempDir()
	var n atomic.Int32
	s, err := Open(dir, Options{Durability: Buffered, AfterSync: func() { n.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	tbf := s.DB().GetOrCreateTable("t")
	for i := 0; i < 3; i++ {
		if err := tbf.Set(item.Entity{Key: strconv.Itoa(i), SK: "x", Value: i}); err != nil {
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
	tblW := s.DB().GetOrCreateTable("tbl")
	for i := 0; i < 10; i++ {
		if err := tblW.Set(item.Entity{Key: strconv.Itoa(i), SK: "x", Value: i}); err != nil {
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
	tbl2 := s2.DB().GetOrCreateTable("tbl")
	if err := tbl2.Set(item.Entity{Key: "extra", SK: "x", Value: 99}); err != nil {
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
	switch x := ev.Value.(type) {
	case int:
		extra = x
	case int64:
		extra = int(x)
	case uint64:
		extra = int(x)
	default:
		t.Fatalf("unexpected type %T for extra", ev.Value)
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
	_ = s.DB().GetOrCreateTable("a").Set(item.Entity{Key: "1", SK: "x", Value: "a1"})
	_ = s.DB().GetOrCreateTable("b").Set(item.Entity{Key: "1", SK: "y", Value: "b1"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	aItem, _ := s2.DB().GetOrCreateTable("a").Get("1")
	bItem, _ := s2.DB().GetOrCreateTable("b").Get("1")
	if aItem.Value.(string) != "a1" || bItem.Value.(string) != "b1" {
		t.Fatalf("va=%v vb=%v", aItem.Value, bItem.Value)
	}
}

func TestCommitTransactionReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	tb := s.DB().GetOrCreateTable("t")
	err = tb.NewSession(context.Background(), func(tx *memdb.Tx) error {
		if err := tx.Set(item.Entity{Key: "a", SK: "x", Value: "1"}); err != nil {
			return err
		}
		return tx.Set(item.Entity{Key: "b", SK: "x", Value: "2"})
	})
	if err != nil {
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
	tb2 := s2.DB().GetOrCreateTable("t")
	a, err := tb2.Get("a")
	if err != nil || a.Value.(string) != "1" {
		t.Fatalf("a: %v %v", a, err)
	}
	b, err := tb2.Get("b")
	if err != nil || b.Value.(string) != "2" {
		t.Fatalf("b: %v %v", b, err)
	}
}

func TestReplayDiscardsIncompleteTransaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DB().GetOrCreateTable("t").Set(item.Entity{Key: "base", SK: "x", Value: "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, WalFileName)
	txid := EncodeTxID(1)
	begin := Entry{Seq: 2, Op: OpBegin, Table: "t", ValueBytes: txid}
	b0, err := begin.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	val, err := code.Encode(item.Entity{Key: "orphan", SK: "x", Value: "bad"})
	if err != nil {
		t.Fatal(err)
	}
	put := Entry{Seq: 3, Op: OpPut, Table: "t", Key: "orphan", Fk: "x", ValueBytes: val}
	b1, err := put.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, append(append(readFile(t, walPath), b0...), b1...), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Durability: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	tb := s2.DB().GetOrCreateTable("t")
	if _, err := tb.Get("orphan"); err == nil {
		t.Fatal("orphan should not be visible without COMMIT")
	}
	base, err := tb.Get("base")
	if err != nil || base.Value.(string) != "ok" {
		t.Fatalf("base: %v %v", base, err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDoubleReopenIdempotentState(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, Options{Durability: SyncEveryWrite})
	_ = s.DB().GetOrCreateTable("t").Set(item.Entity{Key: "k", SK: "f", Value: "v"})
	_ = s.Close()
	s2, _ := Open(dir, Options{Durability: SyncEveryWrite})
	_ = s2.Close()
	s3, _ := Open(dir, Options{Durability: SyncEveryWrite})
	defer s3.Close()
	item, err := s3.DB().GetOrCreateTable("t").Get("k")
	if err != nil || item.Value.(string) != "v" {
		t.Fatalf("%v %v", item.Value, err)
	}
}
