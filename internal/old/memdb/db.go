package memdb

import (
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/item"
)

type DB struct {
	Tables  map[string]*Table
	Lock    sync.RWMutex
	durable DurableWriter
}

func NewDB() *DB {
	return &DB{
		Tables: make(map[string]*Table),
	}
}

func (db *DB) Set(tableName string, entity item.Entity) error {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s not found", tableName)
	}

	return table.Set(entity)
}

// SetDurable wires all tables (existing and future) so Set/Delete go through w.
// Call after WAL replay so replay applies memory via ApplyPut/ApplyDelete only.
func (db *DB) SetDurable(w DurableWriter) {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	db.durable = w
	for name, t := range db.Tables {
		t.name = name
		t.durable = w
	}
}

func (db *DB) CreateTable(name string) *Table {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	t := newTable()
	t.name = name
	t.durable = db.durable
	db.Tables[name] = t
	return t
}

// GetOrCreateTable returns the table named name, creating an empty one if needed.
func (db *DB) GetOrCreateTable(name string) *Table {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	if t, ok := db.Tables[name]; ok {
		return t
	}
	t := newTable()
	t.name = name
	t.durable = db.durable
	db.Tables[name] = t
	return t
}

func newTable() *Table {
	return &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
		mu:      sync.RWMutex{},
	}
}

// NewTableFromSnapshot restores a table from checkpoint bytes (no durable writer).
func NewTableFromSnapshot(data map[string][]byte, fk map[string][]string) *Table {
	t := newTable()
	t.Data = data
	t.Fk = fk
	return t
}
