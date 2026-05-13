package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/item"
)

type DB interface {
	CreateTable(tableName string) error
	Set(ctx context.Context, tableName string, entity item.Entity) error
	Get(ctx context.Context, tableName string, key string) (item.Entity, error)
	GetBySk(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error)
	Del(ctx context.Context, tableName string, key string) error
}

type LogDb interface {
	Set(ctx context.Context, tableName string, entity item.Entity) error
	Delete(ctx context.Context, tableName string, key string) error
	Load(ctx context.Context, operations ...commands.Operations) error
	Close() error
}

type Adapter struct {
	mu sync.Mutex

	memdb DB
	fsdb  DB
	logdb LogDb
}

func New(memdb DB, fsdb DB, logDb LogDb) *Adapter {
	return &Adapter{
		memdb: memdb,
		fsdb:  fsdb,
		logdb: logDb,
	}

}

func (f *Adapter) CreateTable(table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.fsdb.CreateTable(table); err != nil {
		return err
	}

	if err := f.memdb.CreateTable(table); err != nil {
		return err
	}

	return nil
}

func (f *Adapter) Set(ctx context.Context, tableName string, entity item.Entity) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Set(ctx, tableName, entity); err != nil {
		return fmt.Errorf("db: wal set: %w", err)
	}

	// TODO: passar para ser async
	if err := f.fsdb.Set(ctx, tableName, entity); err != nil {
		return fmt.Errorf("db: put entity: %w", err)
	}

	return f.memdb.Set(ctx, tableName, entity)
}

/*
Get reads a value from the database.

Case not found in memmory search, it will be searched in the filesystem.
*/
func (f *Adapter) Get(ctx context.Context, tableName string, key string) (item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	it, err := f.memdb.Get(ctx, tableName, key)
	if err != nil {
		it, err = f.fsdb.Get(ctx, tableName, key)
		if err != nil {
			return item.Entity{}, err
		}
	}

	return it, nil
}

func (f *Adapter) GetBySecondaryKey(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	its, err := f.memdb.GetBySk(ctx, tableName, secondaryKey)
	if err != nil {
		its, err = f.fsdb.GetBySk(ctx, tableName, secondaryKey)
		if err != nil {
			return nil, err
		}
	}

	return its, nil
}

func (f *Adapter) Delete(ctx context.Context, tableName string, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Delete(ctx, tableName, key); err != nil {
		return fmt.Errorf("db: wal delete: %w", err)
	}

	// TODO: passar para ser async
	if err := f.fsdb.Del(ctx, tableName, key); err != nil {
		return fmt.Errorf("db: delete entity: %w", err)
	}

	return f.memdb.Del(ctx, tableName, key)
}

func (f *Adapter) Load(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Load(ctx, f.fsdb, f.memdb); err != nil {
		return fmt.Errorf("db: load wal: %w", err)
	}

	return nil
}

func (f *Adapter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logdb == nil {
		return nil
	}
	return f.logdb.Close()
}
