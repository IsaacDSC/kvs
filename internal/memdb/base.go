package memdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

// Options configures the in-memory cache. Zero value means unlimited entries per table.
type Options struct {
	// MaxEntriesPerTable is the maximum number of primary keys cached per table.
	// 0 disables the limit and LRU bookkeeping.
	MaxEntriesPerTable int
}

type DB struct {
	tables map[string]*table
	opts   Options
	mu     sync.RWMutex
}

// NewDB creates an in-memory database. Use Options.MaxEntriesPerTable to bound cache size with LRU eviction per table.
func NewDB(opts Options) *DB {
	return &DB{
		tables: make(map[string]*table),
		opts:   opts,
	}
}

func (d *DB) CreateTable(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.tables[name]; ok {
		return nil
	}

	d.tables[name] = newTable(d.opts.MaxEntriesPerTable)

	return nil
}

func (d *DB) Set(_ context.Context, tableName string, entity item.Entity) error {
	table, err := d.table(tableName)
	if err != nil {
		return err
	}
	return table.set(entity)
}

func (d *DB) Del(_ context.Context, tableName, key string) error {
	table, err := d.table(tableName)
	if err != nil {
		return err
	}
	return table.delete(key)
}

func (d *DB) Get(_ context.Context, tableName, key string) (item.Entity, error) {
	table, err := d.table(tableName)
	if err != nil {
		return item.Entity{}, err
	}

	return table.get(key)
}

func (d *DB) GetBySk(_ context.Context, tableName, secondaryKey string) ([]item.Entity, error) {
	table, err := d.table(tableName)
	if err != nil {
		return nil, err
	}
	return table.getBySecondaryKey(secondaryKey)
}

func (d *DB) table(name string) (*table, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	table, ok := d.tables[name]
	if !ok {
		return nil, fmt.Errorf("%w : %s", db.ErrTableNotFound, name)
	}
	return table, nil
}
