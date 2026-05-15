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

type TbMutex map[string]*sync.RWMutex

func (m TbMutex) Lock(table string) {
	mu, ok := m[table]
	if !ok {
		mu = new(sync.RWMutex)
		m[table] = mu
	}
	mu.Lock()
}

func (m TbMutex) RLock(table string) {
	mu, ok := m[table]
	if !ok {
		mu = new(sync.RWMutex)
		m[table] = mu
	}
	mu.RLock()
}

func (m TbMutex) Unlock(table string) {
	m[table].Unlock()
}

func (m TbMutex) RUnlock(table string) {
	m[table].RUnlock()
}

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

	// Entity.Value may be map[interface{}]interface{} after CBOR decoding; encoding/json rejects that.
	data.Value = jsonSafeAny(data.Value)

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	path := filepath.Join(d.defaultDir, table, "key", data.Key)
	if err := os.WriteFile(path, b, 0644); err != nil {
		return fmt.Errorf("write put key file: %w", err)
	}

	if data.SK != "" {
		path = filepath.Join(d.defaultDir, table, "sk", data.SK)

		keys, err := d.getKeys(table, data.SK)
		if errors.Is(err, db.ErrNotFoundSk) || os.IsNotExist(err) {
			keys = []string{}
			err = nil
		}
		if err != nil {
			return fmt.Errorf("get keys: %w", err)
		}

		if !slices.Contains(keys, data.Key) {
			keys = append(keys, data.Key)
			kb, err := json.Marshal(keys)
			if err != nil {
				return fmt.Errorf("marshal keys: %w", err)
			}
			if err := os.WriteFile(path, kb, 0644); err != nil {
				return fmt.Errorf("write put sk file: %w", err)
			}
		}
	}

	return nil
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
