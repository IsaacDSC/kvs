package memdb

import (
	"fmt"
	"log"
	"sync"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

type table struct {
	mu sync.Mutex // Reads also mutate LRU state, so the table uses one exclusive lock.

	data         map[string][]byte
	secondaryKey secondaryKey
	lru          lruTracker
}

// newTable builds an empty table. LRU tracking is allocated only when maxEntries > 0 (unlimited cache skips list bookkeeping).
func newTable(maxEntries int) *table {
	return &table{
		data:         make(map[string][]byte),
		secondaryKey: make(map[string]keySet),
		lru:          newLRUTracker(maxEntries),
	}
}

func (t *table) set(entity item.Entity) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	b, err := code.Encode(entity)
	if err != nil {
		return fmt.Errorf("memdb.set error on encoding entity: %w", err)
	}

	// On overwrite, keep SecondaryKey in sync with the stored blob: if the primary key's SK
	// changes (or is cleared), remove this key from the old SK set before indexing the new SK.
	if oldB, ok := t.data[entity.Key]; ok {
		t.secondaryKey.removePreviousIfChanged(entity.Key, oldB, entity.SK)
	}

	t.data[entity.Key] = b

	t.secondaryKey.add(entity.Key, entity.SK)

	t.lru.markRecentlyUsed(entity.Key)

	for t.lru.maxExceeded(len(t.data)) {
		key, ok := t.lru.leastRecentlyUsed()
		if !ok {
			break
		}
		b, ok := t.data[key]
		if !ok {
			t.lru.remove(key)
			continue
		}
		var evicted item.Entity
		if err := code.Decode(b, &evicted); err == nil {
			t.secondaryKey.remove(key, evicted.SK)
		}
		delete(t.data, key)
		t.lru.remove(key)
	}

	return nil
}

func (t *table) get(key string) (item.Entity, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	b, ok := t.data[key]
	if !ok {
		return item.Entity{}, db.ErrNotFound
	}

	var entity item.Entity
	if err := code.Decode(b, &entity); err != nil {
		return item.Entity{}, fmt.Errorf("memdb.get error on decoding entity: %w", err)
	}

	t.lru.markRecentlyUsed(key)

	return entity, nil
}

func (t *table) getBySecondaryKey(sk string) ([]item.Entity, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	keys := t.secondaryKey[sk]
	if len(keys) == 0 {
		return nil, fmt.Errorf("memdb.get by secondary key error on getting entities: secondary key %s not found", sk)
	}

	entities := make([]item.Entity, 0, len(keys))
	for _, key := range keys {
		b, ok := t.data[key]
		if !ok {
			return nil, fmt.Errorf("memdb.get by secondary key error on getting entities: inconsistent index for key %q", key)
		}
		var entity item.Entity
		if err := code.Decode(b, &entity); err != nil {
			return nil, fmt.Errorf("memdb.get by secondary key error on getting entities: %w", err)
		}
		entities = append(entities, entity)
	}

	for _, e := range entities {
		t.lru.markRecentlyUsed(e.Key)
	}

	return entities, nil
}

func (t *table) delete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.deleteEntryLocked(key)
}

func (t *table) deleteEntryLocked(key string) error {
	b, ok := t.data[key]
	if !ok {
		// Idempotent: absent key already matches post-delete state. Required for WAL replay
		// when LRU eviction removed the entry before a later log delete is applied.
		log.Println("[*] - WARN - error not found key for delete item in table.deleteEntryLocked")
		return nil
	}

	var entity item.Entity
	if err := code.Decode(b, &entity); err != nil {
		return fmt.Errorf("memdb.delete error on decoding entity: %w", err)
	}

	delete(t.data, key)
	t.lru.remove(key)

	t.secondaryKey.remove(key, entity.SK)

	return nil
}
