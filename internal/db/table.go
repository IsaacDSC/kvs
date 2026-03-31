package db

import "sync"

type VirtualTable struct {
	Data map[string]any
	Fk   map[string][]string
}

type Table struct {
	mu sync.RWMutex
	VirtualTable
	Session map[int]VirtualTable
}

type Item struct {
	Key   string
	Fk    string
	Value any
}

func (t *Table) Set(item Item) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addLocked(item)
}

func (t *Table) addLocked(item Item) {
	t.Data[item.Key] = item.Value
	for _, k := range t.Fk[item.Fk] {
		if k == item.Key {
			return
		}
	}
	t.Fk[item.Fk] = append(t.Fk[item.Fk], item.Key)
}

func (t *Table) Get(key string) (any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := t.getLocked(key)
	if result == nil {
		return nil, ErrKeyNotFound
	}
	return result, nil
}

func (t *Table) getLocked(key string) any {
	return t.Data[key]
}

func (t *Table) GetByFk(fk string) ([]Item, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	items := t.getByFkLocked(fk)
	if len(items) == 0 || items == nil {
		return nil, ErrKeyNotFound
	}
	return items, nil
}

func (t *Table) getByFkLocked(fk string) []Item {
	items := make([]Item, 0)
	for _, key := range t.Fk[fk] {
		items = append(items, Item{
			Key:   key,
			Value: t.Data[key],
			Fk:    fk,
		})
	}
	return items
}

func (t *Table) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteLocked(key)
}

func (t *Table) deleteLocked(key string) {
	delete(t.Data, key)

	for fk, keys := range t.Fk {
		out := keys[:0]
		for _, k := range keys {
			if k != key {
				out = append(out, k)
			}
		}
		if len(out) == 0 {
			delete(t.Fk, fk)
		} else {
			t.Fk[fk] = out
		}
	}
}
