package db

import (
	"context"
	"errors"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cache"
	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/internal/item"
)

// ─── test doubles ────────────────────────────────────────────────────────────

type memStore struct {
	tables map[string]map[string]item.Entity
}

func newMemStore() *memStore {
	return &memStore{tables: map[string]map[string]item.Entity{}}
}

func (m *memStore) CreateTable(name string) error {
	if _, ok := m.tables[name]; !ok {
		m.tables[name] = map[string]item.Entity{}
	}
	return nil
}

func (m *memStore) Set(_ context.Context, table string, entity item.Entity) error {
	if _, ok := m.tables[table]; !ok {
		m.tables[table] = map[string]item.Entity{}
	}
	m.tables[table][entity.Key] = entity
	return nil
}

func (m *memStore) Get(_ context.Context, table, key string) (item.Entity, error) {
	if t, ok := m.tables[table]; ok {
		if ent, ok2 := t[key]; ok2 {
			return ent, nil
		}
	}
	return item.Entity{}, ErrNotFound
}

func (m *memStore) GetBySk(context.Context, string, string) ([]item.Entity, error) {
	return nil, nil
}

func (m *memStore) Del(_ context.Context, table, key string) error {
	if t, ok := m.tables[table]; ok {
		delete(t, key)
	}
	return nil
}

type memLog struct {
	sets    int
	deletes int
}

func (l *memLog) Set(_ context.Context, _ string, _ item.Entity) error {
	l.sets++
	return nil
}

func (l *memLog) Delete(_ context.Context, _ string, _ string) error {
	l.deletes++
	return nil
}

func (l *memLog) Load(_ context.Context, _ ...commands.Operations) error { return nil }
func (l *memLog) Close() error                                            { return nil }

// ─── tests ───────────────────────────────────────────────────────────────────

// ApplyReplicated must apply unconditionally even when the store already has the
// target version — this is the restart catch-up scenario where the Raft in-memory
// log is re-delivered in full after WAL replay already advanced the state.
func TestApplyReplicated_appliesEvenWhenVersionAlreadyPresent(t *testing.T) {
	store := newMemStore()
	_ = store.CreateTable("t")
	_ = store.Set(context.Background(), "t", item.Entity{Key: "k", SK: "f", Value: "v", Version: "2"})

	log := &memLog{}
	adapter := New(store, log, nil)

	it := dto.Item{
		Key:     "k",
		SK:      "f",
		Value:   map[string]any{"x": 1},
		Version: &dto.Version{OldVersion: "1", PromoteVersion: "2"},
	}
	if err := adapter.ApplyReplicated(context.Background(), "t", it); err != nil {
		t.Fatalf("ApplyReplicated: unexpected error: %v", err)
	}
	if log.sets != 1 {
		t.Fatalf("expected 1 WAL append, got %d", log.sets)
	}
	got, _ := store.Get(context.Background(), "t", "k")
	if got.Version != "2" {
		t.Fatalf("version=%q want %q", got.Version, "2")
	}
}

// ApplyReplicated must also apply when the version is behind (catch-up replaying
// an old entry before later entries bring it forward again).
func TestApplyReplicated_appliesOldEntryDuringCatchUp(t *testing.T) {
	store := newMemStore()
	_ = store.CreateTable("t")
	_ = store.Set(context.Background(), "t", item.Entity{Key: "k", Version: "3"})

	log := &memLog{}
	adapter := New(store, log, nil)

	// Replaying an older entry that sets version=1 — correct Raft behaviour.
	it := dto.Item{Key: "k", Value: map[string]any{"x": 1}}
	entity := it.Entity()
	entity.Version = "1"

	if err := adapter.ApplyReplicated(context.Background(), "t", it); err != nil {
		t.Fatalf("ApplyReplicated: unexpected error: %v", err)
	}
	if log.sets != 1 {
		t.Fatalf("expected 1 WAL append, got %d", log.sets)
	}
}

// ApplyReplicatedDelete must evict a previously cached key so reads do not return ghosts.
func TestApplyReplicatedDelete_evictsCache(t *testing.T) {
	store := newMemStore()
	_ = store.CreateTable("t")
	_ = store.Set(context.Background(), "t", item.Entity{Key: "k", Value: "v"})

	cc := cache.New[item.Entity](8, 0)
	adapter := New(store, &memLog{}, cc)

	if _, err := adapter.Get(context.Background(), "t", "k"); err != nil {
		t.Fatalf("Get warm cache: %v", err)
	}
	if err := adapter.ApplyReplicatedDelete(context.Background(), "t", dto.DeleteItem{Key: "k"}); err != nil {
		t.Fatalf("ApplyReplicatedDelete: %v", err)
	}
	if _, err := adapter.Get(context.Background(), "t", "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: want ErrNotFound, got %v", err)
	}
}

// The normal client path (Set) must still reject stale optimistic-lock versions.
func TestSet_rejectsStaleOptimisticLock(t *testing.T) {
	store := newMemStore()
	_ = store.CreateTable("t")
	_ = store.Set(context.Background(), "t", item.Entity{Key: "k", Version: "2"})

	log := &memLog{}
	adapter := New(store, log, nil)

	it := dto.Item{
		Key:     "k",
		Value:   map[string]any{"x": 1},
		Version: &dto.Version{OldVersion: "1", PromoteVersion: "3"},
	}
	err := adapter.Set(context.Background(), "t", it)
	if !errors.Is(err, ErrNotCompatibleVersion) {
		t.Fatalf("Set: want ErrNotCompatibleVersion, got %v", err)
	}
	if log.sets != 0 {
		t.Fatalf("WAL must not be written on validation failure, got %d sets", log.sets)
	}
}
