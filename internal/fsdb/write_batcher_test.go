package fsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

// mockDB is a minimal in-memory db.DB for batcher tests.
type mockDB struct {
	mu   sync.Mutex
	keys map[string]item.Entity
	ops  []string
}

func newMockDB() *mockDB {
	return &mockDB{keys: make(map[string]item.Entity)}
}

func (m *mockDB) k(table, key string) string {
	return table + "\x00" + key
}

func (m *mockDB) CreateTable(string) error { return nil }

func (m *mockDB) Set(_ context.Context, table string, entity item.Entity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[m.k(table, entity.Key)] = entity
	m.ops = append(m.ops, fmt.Sprintf("set:%s:%s", table, entity.Key))
	return nil
}

func (m *mockDB) BulkSet(_ context.Context, table string, entities []item.Entity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entity := range entities {
		m.keys[m.k(table, entity.Key)] = entity
		m.ops = append(m.ops, fmt.Sprintf("set:%s:%s", table, entity.Key))
	}
	return nil
}

func (m *mockDB) Get(_ context.Context, table, key string) (item.Entity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keys[m.k(table, key)]
	if !ok {
		return item.Entity{}, db.ErrNotFound
	}
	return e, nil
}

func (m *mockDB) GetBySk(ctx context.Context, table, sk string) ([]item.Entity, error) {
	return nil, db.ErrNotFoundSk
}

func (m *mockDB) Del(_ context.Context, table, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.k(table, key)
	if _, ok := m.keys[k]; !ok {
		return db.ErrNotFound
	}
	delete(m.keys, k)
	m.ops = append(m.ops, fmt.Sprintf("del:%s:%s", table, key))
	return nil
}

func (m *mockDB) BulkDel(ctx context.Context, table string, keys []string) error {
	for _, key := range keys {
		if err := m.Del(ctx, table, key); err != nil && !errors.Is(err, db.ErrNotFound) {
			return err
		}
	}
	return nil
}

func (m *mockDB) opSummary() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.ops))
	copy(out, m.ops)
	return out
}

func (m *mockDB) setCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.ops {
		if len(o) >= 4 && o[:4] == "set:" {
			n++
		}
	}
	return n
}

func (m *mockDB) delCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.ops {
		if len(o) >= 4 && o[:4] == "del:" {
			n++
		}
	}
	return n
}

func TestWriteBatcher_mergeSameKeyDeferWrites(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	for i := 0; i < 10; i++ {
		if err := b.Set(ctx, "t", item.Entity{Key: "k", Value: fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if inner.setCount() != 1 {
		t.Fatalf("expected 1 inner Set, got %d ops=%v", inner.setCount(), inner.opSummary())
	}
}

func TestWriteBatcher_setDelSetInterleave(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	_ = b.Set(ctx, "t", item.Entity{Key: "k", Value: "a"})
	_ = b.Set(ctx, "t", item.Entity{Key: "k", Value: "b"})
	_ = b.Del(ctx, "t", "k")
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Coalesced Del on key never written to inner: idempotent delete, no recorded inner ops.
	if len(inner.opSummary()) != 0 {
		t.Fatalf("expected no inner ops (Del was no-op on empty store), got %v", inner.opSummary())
	}

	inner2 := newMockDB()
	b2 := NewWriteBatcher(inner2, WriteBatcherOptions{DeferWrites: true})
	_ = b2.Del(ctx, "t", "k")
	_ = b2.Set(ctx, "t", item.Entity{Key: "k", Value: "x"})
	if err := b2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if inner2.setCount() != 1 || inner2.delCount() != 0 {
		t.Fatalf("Del then Set should end as Set, ops=%v", inner2.opSummary())
	}
}

// BulkDel coalesces with pending Sets (LWW tombstone) and tolerates keys the inner
// store never had — the flush treats ErrNotFound as success.
func TestWriteBatcher_bulkDelCoalescesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	_ = inner.Set(ctx, "t", item.Entity{Key: "k1", Value: "v"}) // pre-existing on disk
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	_ = b.Set(ctx, "t", item.Entity{Key: "k2", Value: "x"}) // pending, never reaches inner
	if err := b.BulkDel(ctx, "t", []string{"k1", "k2", "ghost"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// k1: real delete; k2: Set coalesced into a no-op delete; ghost: idempotent no-op.
	if inner.delCount() != 1 {
		t.Fatalf("want 1 inner Del, ops=%v", inner.opSummary())
	}
	if inner.setCount() != 1 {
		t.Fatalf("pending Set k2 must be coalesced away, ops=%v", inner.opSummary())
	}
	if _, err := b.Get(ctx, "t", "k1"); err == nil {
		t.Fatal("k1 must be gone after flush")
	}
}

// Without DeferWrites, BulkDel flushes before returning (Option B passthrough).
func TestWriteBatcher_bulkDelSyncModeFlushesImmediately(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	_ = inner.Set(ctx, "t", item.Entity{Key: "k", Value: 1})
	b := NewWriteBatcher(inner, WriteBatcherOptions{}) // DeferWrites false

	if err := b.BulkDel(ctx, "t", []string{"k"}); err != nil {
		t.Fatal(err)
	}
	if inner.delCount() != 1 {
		t.Fatalf("sync mode should flush the delete, ops=%v", inner.opSummary())
	}
}

// BulkDel counts toward the dirty-keys limit and triggers the deferred flush.
func TestWriteBatcher_bulkDelTriggersMaxKeysFlush(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{
		DeferWrites:   true,
		MaxDirtyKeys:  3,
		MaxDirtyBytes: 1 << 30,
	})

	_ = b.Set(ctx, "t", item.Entity{Key: "k1", Value: 1})
	if len(inner.opSummary()) != 0 {
		t.Fatalf("unexpected early flush: %v", inner.opSummary())
	}
	if err := b.BulkDel(ctx, "t", []string{"k2", "k3"}); err != nil {
		t.Fatal(err)
	}
	// 3 dirty merge keys reached: everything flushed (k1 set; k2/k3 no-op deletes).
	if inner.setCount() != 1 {
		t.Fatalf("expected flush of pending Set, ops=%v", inner.opSummary())
	}
	keys, _ := b.PendingDirty()
	if keys != 0 {
		t.Fatalf("pending keys = %d, want 0 after limit-triggered flush", keys)
	}
}

func TestWriteBatcher_flushDeterministicOrder(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	_ = b.Set(ctx, "tb", item.Entity{Key: "a", Value: 1})
	_ = b.Set(ctx, "ta", item.Entity{Key: "b", Value: 2})
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ops := inner.opSummary()
	want := []string{"set:ta:b", "set:tb:a"}
	if len(ops) != len(want) {
		t.Fatalf("ops=%v", ops)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("op[%d] want %q got %q full=%v", i, want[i], ops[i], ops)
		}
	}
}

func TestWriteBatcher_maxKeysTriggersFlush(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{
		DeferWrites:   true,
		MaxDirtyKeys:  3,
		MaxDirtyBytes: 1 << 30,
	})

	_ = b.Set(ctx, "t", item.Entity{Key: "k1", Value: 1})
	_ = b.Set(ctx, "t", item.Entity{Key: "k2", Value: 2})
	if len(inner.opSummary()) != 0 {
		t.Fatalf("unexpected early flush: %v", inner.opSummary())
	}
	_ = b.Set(ctx, "t", item.Entity{Key: "k3", Value: 3})
	if len(inner.opSummary()) != 3 {
		t.Fatalf("expected flush of 3 keys, got %v", inner.opSummary())
	}
}

func TestWriteBatcher_syncModeFlushesEachCall(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{}) // DeferWrites false

	if err := b.Set(ctx, "t", item.Entity{Key: "k", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(ctx, "t", item.Entity{Key: "k", Value: 2}); err != nil {
		t.Fatal(err)
	}
	if inner.setCount() != 2 {
		t.Fatalf("sync mode should pass through each Set, got %v", inner.opSummary())
	}
}

func TestWriteBatcher_GetReadThrough(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	if err := b.Set(ctx, "t", item.Entity{Key: "k", Value: "hidden"}); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(ctx, "t", "k")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "hidden" {
		t.Fatalf("read-through got %#v", got.Value)
	}
	_, err = b.Get(ctx, "t", "missing")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteBatcher_concurrentSameKeyOneInnerSet(t *testing.T) {
	ctx := context.Background()
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{DeferWrites: true})

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(v int) {
			defer wg.Done()
			_ = b.Set(ctx, "t", item.Entity{Key: "k", Value: v})
		}(i)
	}
	wg.Wait()
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if inner.setCount() != 1 {
		t.Fatalf("want 1 coalesced Set, got %d ops=%v", inner.setCount(), inner.opSummary())
	}
}

func TestWriteBatcher_periodicStop(t *testing.T) {
	inner := newMockDB()
	b := NewWriteBatcher(inner, WriteBatcherOptions{
		DeferWrites:   true,
		FlushInterval: 20 * time.Millisecond,
	})
	time.Sleep(45 * time.Millisecond)
	b.Stop()
}
