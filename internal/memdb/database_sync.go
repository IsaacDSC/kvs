package memdb

import (
	"fmt"
	"sync"
)

type DB struct {
	Tables map[string]*Table
	mu     sync.RWMutex
}

func NewDB() *DB {
	return &DB{
		Tables: make(map[string]*Table),
	}
}

func (db *DB) CreateTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.Tables[name] = &Table{
		Data:         make(map[string][]byte),
		SecondaryKey: make(map[string]Set),
		mu:           sync.RWMutex{},
	}

	return nil
}

func (db *DB) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	table, ok := db.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return table, nil
}
