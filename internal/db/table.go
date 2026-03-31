package db

import (
	"errors"
	"sync"

	"github.com/IsaacDSC/kvs/internal/code"
)

// VirtualTable stores values as CBOR bytes; the public Table API still uses any.
type VirtualTable struct {
	Data map[string][]byte
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

func (t *Table) Set(item Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addLocked(item)
}

func (t *Table) addLocked(item Item) error {
	b, err := code.Encode(item.Value)
	if err != nil {
		return errors.Join(ErrEncodeValue, err)
	}
	t.Data[item.Key] = b
	for _, k := range t.Fk[item.Fk] {
		if k == item.Key {
			return nil
		}
	}
	t.Fk[item.Fk] = append(t.Fk[item.Fk], item.Key)
	return nil
}

func (t *Table) Get(key string) (any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.getLocked(key)
}

func (t *Table) getLocked(key string) (any, error) {
	b, ok := t.Data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	v, err := code.Decode(b)
	if err != nil {
		return nil, errors.Join(ErrDecodeValue, err)
	}
	return v, nil
}

func (t *Table) GetByFk(fk string) ([]Item, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	items, err := t.getByFkLocked(fk)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrKeyNotFound
	}
	return items, nil
}

func (t *Table) getByFkLocked(fk string) ([]Item, error) {
	keys := t.Fk[fk]
	items := make([]Item, 0, len(keys))
	for _, key := range keys {
		b, ok := t.Data[key]
		if !ok {
			continue
		}
		v, err := code.Decode(b)
		if err != nil {
			return nil, errors.Join(ErrDecodeValue, err)
		}
		items = append(items, Item{
			Key:   key,
			Value: v,
			Fk:    fk,
		})
	}
	return items, nil
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
