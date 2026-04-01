package db

import (
	"errors"
	"sync"

	"github.com/fxamacker/cbor/v2"

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
	name    string
	durable DurableWriter

	sessionOnce sync.Once
	sessionSem  chan struct{}
}

type Item struct {
	Key     string
	Fk      string
	Value   any
	Version string
}

func (t *Table) initSessionLock() {
	t.sessionOnce.Do(func() {
		t.sessionSem = make(chan struct{}, 1)
		t.sessionSem <- struct{}{}
	})
}

// Set inserts or updates item. If the DB is opened via store.Open, this records a WAL entry.
func (t *Table) Set(item Item) error {
	if t.durable != nil && t.name != "" {
		return t.durable.Put(t.name, item)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addLocked(item)
}

// ApplyPut updates memory only; used when replaying the WAL or after a WAL append inside the store.
func (t *Table) ApplyPut(item Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addLocked(item)
}

func (t *Table) addLocked(item Item) error {
	b, err := code.Encode(item)
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

func (t *Table) Get(key string) (Item, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.getLocked(key)
}

func (t *Table) getLocked(key string) (Item, error) {
	b, ok := t.Data[key]
	if !ok {
		return Item{}, ErrKeyNotFound
	}
	var item Item
	if err := cbor.Unmarshal(b, &item); err != nil {
		return Item{}, errors.Join(ErrDecodeValue, err)
	}
	return item, nil
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
		var item Item
		if err := cbor.Unmarshal(b, &item); err != nil {
			return nil, errors.Join(ErrDecodeValue, err)
		}
		items = append(items, item)
	}
	return items, nil
}

// Delete removes key. If the DB is opened via store.Open, this records a WAL delete.
func (t *Table) Delete(key string) error {
	if t.durable != nil && t.name != "" {
		return t.durable.Delete(t.name, key)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteLocked(key)
	return nil
}

// ApplyDelete removes key from memory only; used when replaying the WAL or after a WAL append inside the store.
func (t *Table) ApplyDelete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteLocked(key)
	return nil
}

// ExportMaps returns deep copies of Data and Fk for checkpointing.
func (t *Table) ExportMaps() (data map[string][]byte, fk map[string][]string) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	data = make(map[string][]byte, len(t.Data))
	for k, v := range t.Data {
		vv := make([]byte, len(v))
		copy(vv, v)
		data[k] = vv
	}
	fk = make(map[string][]string, len(t.Fk))
	for k, v := range t.Fk {
		sl := make([]string, len(v))
		copy(sl, v)
		fk[k] = sl
	}
	return data, fk
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
