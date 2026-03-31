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
	db.Tables[name] = &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string]any),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
		mu:      sync.RWMutex{},
	}
	return db.Tables[name]
}
