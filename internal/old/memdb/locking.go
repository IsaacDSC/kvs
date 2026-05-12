package memdb

import (
	"context"
	"errors"

	"github.com/IsaacDSC/kvs/internal/item"
)

type OptimisticLocking struct {
	table *Table
}

type OptimisticResult struct {
	item item.Entity
	err  error
}

func MakeOptimisticResult(item item.Entity, err error) OptimisticResult {
	return OptimisticResult{item: item, err: err}
}

func (or OptimisticResult) Err() error {
	return or.err
}

func (or OptimisticResult) GetLastVersion() (item.Entity, error) {
	if errors.Is(or.err, ErrInvalidVersion) {
		return or.item, nil
	}

	return item.Entity{}, or.err
}

func (t *Table) NewOptimisticLocking(ctx context.Context) *OptimisticLocking {
	return &OptimisticLocking{table: t}
}

// OptimisticPut applies optimistic version rules and writes via Table.Set (WAL when the DB is durable).
func (t *Table) OptimisticPut(ctx context.Context, item item.Entity, candidate string) OptimisticResult {
	return t.NewOptimisticLocking(ctx).Set(item, candidate)
}

// OptimisticDelete applies optimistic version rules and deletes via Table.Delete (WAL when the DB is durable).
func (t *Table) OptimisticDelete(ctx context.Context, item item.Entity, candidate string) OptimisticResult {
	return t.NewOptimisticLocking(ctx).Del(item, candidate)
}

func (ol OptimisticLocking) Set(item item.Entity, candidate string) OptimisticResult {
	dbItem, err := ol.table.Get(item.Key)
	if err != nil {
		return OptimisticResult{err: err}
	}

	if dbItem.Version == "" {
		item.Version = candidate
		err := ol.table.Set(item)
		return OptimisticResult{item: item, err: err}
	}

	if dbItem.Version != item.Version {
		return OptimisticResult{item: dbItem, err: ErrInvalidVersion}
	}

	item.Version = candidate
	if err := ol.table.Set(item); err != nil {
		return OptimisticResult{err: err}
	}

	return OptimisticResult{item: item}
}

func (ol OptimisticLocking) Del(item item.Entity, candidate string) OptimisticResult {
	dbItem, err := ol.table.Get(item.Key)
	if err != nil {
		return OptimisticResult{err: err}
	}

	if dbItem.Version == "" {
		if err := ol.table.Delete(item.Key); err != nil {
			return OptimisticResult{err: err}
		}
		return OptimisticResult{item: item, err: nil}
	}

	if dbItem.Version != item.Version {
		return OptimisticResult{item: dbItem, err: ErrInvalidVersion}
	}

	if err := ol.table.Delete(item.Key); err != nil {
		return OptimisticResult{err: err}
	}

	return OptimisticResult{item: item, err: nil}
}
