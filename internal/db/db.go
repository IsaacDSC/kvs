package db

import "sync"

type DB struct {
	Tables map[string]*Table
	Lock   sync.RWMutex
}

func NewDB() *DB {
	return &DB{
		Tables: make(map[string]*Table),
	}
}

func (db *DB) CreateTable(name string) *Table {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	db.Tables[name] = newTable()
	return db.Tables[name]
}

// GetOrCreateTable returns the table named name, creating an empty one if needed.
func (db *DB) GetOrCreateTable(name string) *Table {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	if t, ok := db.Tables[name]; ok {
		return t
	}
	t := newTable()
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
