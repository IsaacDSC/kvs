package fsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

/* TODO:
- [ ] Adicionar testes
- [ ] Adicionar erros para saber caso o arquivo não exista
- [ ] Adicionar erros por operação (leitura, escrita, etc.)
- [ ] Validar se o melhor seria utilizar json para enconding e decoding ao invés de cbor ou outro formato.

*/

type Db struct {
	defaultDir string
	tbmu       TbMutex
}

func NewDb(defaultDir string) *Db {
	tables := make(map[string]*sync.RWMutex)
	return &Db{defaultDir: defaultDir, tbmu: tables}
}

func (d *Db) CreateTable(name string) error {
	path := filepath.Join(d.defaultDir, name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(path, "key"), 0o755); err != nil {
		return fmt.Errorf("create table keys dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(path, "sk"), 0o755); err != nil {
		return fmt.Errorf("create table sk dir: %w", err)
	}
	d.tbmu[name] = &sync.RWMutex{}
	return nil
}

func (d *Db) Set(ctx context.Context, table string, data item.Entity) error {
	d.tbmu.Lock(table)
	defer d.tbmu.Unlock(table)

	return d.setLocked(table, data)
}

// bulkWriteConcurrency bounds how many key files BulkSet writes at once, capping
// in-flight syscalls / open descriptors while still parallelizing the I/O.
const bulkWriteConcurrency = 16

// BulkSet writes multiple entities under a single table lock. Key files are written
// concurrently (each key is an independent file), then the shared SK index files are
// consolidated — each written exactly once. It is not atomic: on failure, entities
// already written remain persisted.
func (d *Db) BulkSet(ctx context.Context, table string, entities []item.Entity) error {
	d.tbmu.Lock(table)
	defer d.tbmu.Unlock(table)

	// Collapse duplicate keys (last wins): two goroutines writing the same key file
	// would race, and last-write-wins matches the old sequential behavior.
	entities = dedupeByKey(entities)

	if err := d.writeKeyFilesConcurrent(ctx, table, entities); err != nil {
		return err
	}

	return d.bulkUpdateSk(table, entities)
}

// writeKeyFilesConcurrent fans the per-key writes across a bounded goroutine pool and
// returns the first error, cancelling the remaining dispatches.
func (d *Db) writeKeyFilesConcurrent(ctx context.Context, table string, entities []item.Entity) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, bulkWriteConcurrency)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error

dispatch:
	for i, data := range entities {
		select {
		case <-ctx.Done():
			break dispatch // an earlier write failed; stop scheduling more.
		case sem <- struct{}{}:
			wg.Add(1)
			go func(i int, data item.Entity) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := d.writeKeyFile(table, data); err != nil {
					once.Do(func() {
						firstErr = fmt.Errorf("bulk set entity %d (key %q): %w", i, data.Key, err)
						cancel()
					})
				}
			}(i, data)
		}
	}

	wg.Wait()
	return firstErr
}

// bulkUpdateSk groups every entity by SK and rewrites each SK index file once, instead
// of a read-modify-write per item. SK files are shared across entities, so this runs
// after the parallel key writes — never concurrently for the same sk.
func (d *Db) bulkUpdateSk(table string, entities []item.Entity) error {
	order := make([]string, 0)
	bySK := make(map[string][]string)
	for _, e := range entities {
		if e.SK == "" {
			continue
		}
		if _, seen := bySK[e.SK]; !seen {
			order = append(order, e.SK)
		}
		bySK[e.SK] = append(bySK[e.SK], e.Key)
	}

	for _, sk := range order {
		if err := d.appendKeysToSk(table, sk, bySK[sk]...); err != nil {
			return err
		}
	}

	return nil
}

// dedupeByKey returns entities with duplicate keys collapsed to their last occurrence,
// preserving first-seen order.
func dedupeByKey(entities []item.Entity) []item.Entity {
	idx := make(map[string]int, len(entities))
	out := make([]item.Entity, 0, len(entities))
	for _, e := range entities {
		if i, ok := idx[e.Key]; ok {
			out[i] = e
			continue
		}
		idx[e.Key] = len(out)
		out = append(out, e)
	}
	return out
}

// setLocked performs a single Set without acquiring the table lock; callers must hold it.
func (d *Db) setLocked(table string, data item.Entity) error {
	if err := d.writeKeyFile(table, data); err != nil {
		return err
	}

	if data.SK != "" {
		return d.appendKeysToSk(table, data.SK, data.Key)
	}

	return nil
}

// writeKeyFile persists one entity to its key file. Key files are independent (one file
// per key), so concurrent callers writing distinct keys never race.
func (d *Db) writeKeyFile(table string, data item.Entity) error {
	// Entity.Value may be map[interface{}]interface{} after CBOR decoding; encoding/json rejects that.
	data.Value = jsonSafeAny(data.Value)

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	path := filepath.Join(d.defaultDir, table, "key", data.Key)
	if err := os.WriteFile(path, b, 0644); err != nil {
		if writeBlockedByMissingPath(err) {
			return db.ErrTableNotFound
		}
		return fmt.Errorf("write put key file: %w", err)
	}

	return nil
}

// appendKeysToSk merges keys into the SK index file, reading and rewriting it once. An
// SK file is shared by every entity with the same SK, so it must never be written
// concurrently for the same sk.
func (d *Db) appendKeysToSk(table, sk string, newKeys ...string) error {
	keys, err := d.getKeys(table, sk)
	if errors.Is(err, db.ErrNotFoundSk) || os.IsNotExist(err) {
		keys = []string{}
		err = nil
	}
	if err != nil {
		return fmt.Errorf("get keys: %w", err)
	}

	changed := false
	for _, k := range newKeys {
		if !slices.Contains(keys, k) {
			keys = append(keys, k)
			changed = true
		}
	}
	if !changed {
		return nil
	}

	kb, err := json.Marshal(keys)
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}

	path := filepath.Join(d.defaultDir, table, "sk", sk)
	if err := os.WriteFile(path, kb, 0644); err != nil {
		if writeBlockedByMissingPath(err) {
			return db.ErrTableNotFound
		}
		return fmt.Errorf("write put sk file: %w", err)
	}

	return nil
}

// writeBlockedByMissingPath reports errors typical of Put without CreateTable:
// missing parent dir (ENOENT) or a path segment that exists but is not a directory (ENOTDIR).
func writeBlockedByMissingPath(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR)
}

// jsonSafeAny converts values that encoding/json cannot marshal (e.g. map[interface{}]interface{}
// produced by CBOR-decoding nested maps) into JSON-friendly shapes.
func jsonSafeAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, val := range x {
			m[k] = jsonSafeAny(val)
		}
		return m
	case map[interface{}]interface{}:
		m := make(map[string]any, len(x))
		for k, val := range x {
			var ks string
			if s, ok := k.(string); ok {
				ks = s
			} else {
				ks = fmt.Sprint(k)
			}
			m[ks] = jsonSafeAny(val)
		}
		return m
	case []interface{}:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = jsonSafeAny(val)
		}
		return out
	default:
		return v
	}
}

func (d *Db) Get(ctx context.Context, table string, key string) (item.Entity, error) {
	d.tbmu.RLock(table)
	defer d.tbmu.RUnlock(table)

	path := filepath.Join(d.defaultDir, table, "key", key)
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR) {
		return item.Entity{}, db.ErrNotFound
	}

	if err != nil {
		return item.Entity{}, fmt.Errorf("read get key file: %w", err)
	}

	var result item.Entity
	if err := json.Unmarshal(b, &result); err != nil {
		return item.Entity{}, fmt.Errorf("unmarshal value: %w", err)
	}

	return result, nil

}

func (d *Db) GetBySk(ctx context.Context, table string, sk string) ([]item.Entity, error) {
	d.tbmu.RLock(table)
	defer d.tbmu.RUnlock(table)

	keys, err := d.getKeys(table, sk)
	if err != nil {
		return nil, fmt.Errorf("get keys: %w", err)
	}

	var results []item.Entity
	for _, key := range keys {
		result, err := d.Get(ctx, table, key)
		if err != nil {
			return nil, fmt.Errorf("get by key: %w", err)
		}

		results = append(results, result)
	}

	return results, nil
}

func (d *Db) Del(ctx context.Context, table string, key string) error {
	d.tbmu.Lock(table)
	defer d.tbmu.Unlock(table)

	data, err := d.get(ctx, table, key)
	if err != nil {
		return db.ErrNotFound
	}

	path := filepath.Join(d.defaultDir, table, "key", key)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove key file: %w", err)
	}

	if data.SK != "" {
		keys, err := d.getKeys(table, data.SK)
		if err != nil {
			return db.ErrNotFoundSk
		}

		keys = slices.DeleteFunc(keys, func(k string) bool {
			return k == key
		})

		path = filepath.Join(d.defaultDir, table, "sk", data.SK)

		if len(keys) == 0 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove sk file: %w", err)
			}
			return nil
		}

		kb, err := json.Marshal(keys)
		if err != nil {
			return fmt.Errorf("marshal keys: %w", err)
		}

		if err := os.WriteFile(path, kb, 0644); err != nil {
			return fmt.Errorf("write put sk file: %w", err)
		}
	}

	return nil
}

func (d *Db) getKeys(table string, sk string) ([]string, error) {
	path := filepath.Join(d.defaultDir, table, "sk", sk)
	keysBytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, db.ErrNotFoundSk
		}
		return nil, fmt.Errorf("read get by sk file: %w", err)
	}

	var keys []string
	if err := json.Unmarshal(keysBytes, &keys); err != nil {
		return nil, fmt.Errorf("unmarshal keys: %w", err)
	}

	return keys, nil
}

func (d *Db) get(_ context.Context, table string, key string) (item.Entity, error) {
	path := filepath.Join(d.defaultDir, table, "key", key)
	b, err := os.ReadFile(path)
	if err != nil {
		return item.Entity{}, fmt.Errorf("read get key file: %w", err)
	}

	var result item.Entity
	if err := json.Unmarshal(b, &result); err != nil {
		return item.Entity{}, fmt.Errorf("unmarshal value: %w", err)
	}

	return result, nil
}
