package memdb

import (
	"fmt"
	"slices"
	"sync"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/item"
)

type Table struct {
	mu           sync.RWMutex
	Data         map[string][]byte
	SecondaryKey map[string]Set
}

type Set []string

func (s Set) Add(key string) Set {
	if slices.Contains(s, key) {
		return s
	}
	return append(s, key)
}

func (s Set) Remove(key string) Set {
	return slices.DeleteFunc(s, func(s string) bool {
		return s == key
	})
}

func (t *Table) Set(entity item.Entity) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	b, err := code.Encode(entity)
	if err != nil {
		return fmt.Errorf("memdb.set error on encoding entity: %w", err)
	}

	t.Data[entity.Key] = b

	if entity.SK != "" {
		set := t.SecondaryKey[entity.SK]
		t.SecondaryKey[entity.SK] = set.Add(entity.Key)
	}

	return nil
}

func (t *Table) Get(key string) (item.Entity, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	b, ok := t.Data[key]
	if !ok {
		return item.Entity{}, fmt.Errorf("memdb.get error on getting entity: key %s not found", key)
	}

	var entity item.Entity
	if err := code.DecodeItem(b, &entity); err != nil {
		return item.Entity{}, fmt.Errorf("memdb.get error on decoding entity: %w", err)
	}

	return entity, nil
}

func (t *Table) GetBySecondaryKey(sk string) ([]item.Entity, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	keys := t.SecondaryKey[sk]
	if len(keys) == 0 {
		return nil, fmt.Errorf("memdb.get by secondary key error on getting entities: secondary key %s not found", sk)
	}

	entities := make([]item.Entity, 0, len(keys))
	for _, key := range keys {
		entity, err := t.Get(key)
		if err != nil {
			return nil, fmt.Errorf("memdb.get by secondary key error on getting entities: %w", err)
		}
		entities = append(entities, entity)
	}

	return entities, nil
}

func (t *Table) Delete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.Data[key]
	if !ok {
		return fmt.Errorf("memdb.delete error on deleting entity: key %s not found", key)
	}

	var entity item.Entity
	if err := code.DecodeItem(b, &entity); err != nil {
		return fmt.Errorf("memdb.delete error on decoding entity: %w", err)
	}

	delete(t.Data, key)

	if entity.SK != "" {
		set := t.SecondaryKey[entity.SK]
		if len(set) == 1 {
			delete(t.SecondaryKey, entity.SK)
		} else {
			t.SecondaryKey[entity.SK] = set.Remove(key)
		}
	}

	return nil
}
