package fsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Db struct {
	defaultDir string
	tables     map[string]*sync.RWMutex
}

func NewDb(defaultDir string) *Db {
	return &Db{defaultDir: defaultDir}
}

func (db *Db) CreateTable(name string) error {
	path := filepath.Join(db.defaultDir, name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	db.tables[name] = &sync.RWMutex{}
	return nil
}

func (db *Db) Put(table string, key string, value []byte) error {
	db.tables[table].Lock()
	defer db.tables[table].Unlock()

	path := filepath.Join(db.defaultDir, table, "key", key)
	if err := os.WriteFile(path, value, 0644); err != nil {
		return fmt.Errorf("write put key file: %w", err)
	}

	// for _, fk := range  {}

	return nil
}

func (db *Db) Get(table string, key string) ([]byte, error) {
	db.tables[table].RLock()
	defer db.tables[table].RUnlock()

	path := filepath.Join(db.defaultDir, table, "key", key)
	keys, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read get key file: %w", err)
	}

	return keys, nil
}

func (db *Db) GetByFk(table string, fk string) ([]byte, error) {
	db.tables[table].RLock()
	defer db.tables[table].RUnlock()

	path := filepath.Join(db.defaultDir, table, "fk", fk)
	keys, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read get by fk file: %w", err)
	}

	return keys, nil
}

func (db *Db) Del(table string, key string) error {
	db.tables[table].Lock()
	defer db.tables[table].Unlock()

	path := filepath.Join(db.defaultDir, table, "key", key)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove key file: %w", err)
	}

	return nil
}
