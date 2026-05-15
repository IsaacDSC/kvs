package db

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
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

func (f *Adapter) Set(ctx context.Context, tableName string, it dto.Item) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entity := it.Entity()

	if err := f.validateConsistency(ctx, tableName, it); err != nil {
		return err
	}

	if err := f.logdb.Set(ctx, tableName, entity); err != nil {
		return fmt.Errorf("db: wal set: %w", err)
	}

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

	it, err := f.getEntityMemThenFS(ctx, tableName, key)
	if err != nil {
		return item.Entity{}, err
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

func (f *Adapter) Delete(ctx context.Context, tableName string, it dto.DeleteItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := it.Item()

	if err := f.validateConsistency(ctx, tableName, e); err != nil {
		return err
	}

	if err := f.logdb.Delete(ctx, tableName, e.Key); err != nil {
		return fmt.Errorf("db: wal delete: %w", err)
	}

	if err := f.fsdb.Del(ctx, tableName, e.Key); err != nil {
		return fmt.Errorf("db: delete entity: %w", err)
	}

	return f.memdb.Del(ctx, tableName, e.Key)
}

func (f *Adapter) Load(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Load(ctx, f.memdb, f.fsdb); err != nil {
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
	if flusher, ok := f.fsdb.(interface {
		Flush(ctx context.Context) error
	}); ok {
		_ = flusher.Flush(context.Background())
	}
	return f.logdb.Close()

}

// getEntityMemThenFS reads from memdb and falls back to fsdb on error. Caller must hold f.mu.
func (f *Adapter) getEntityMemThenFS(ctx context.Context, tableName, key string) (item.Entity, error) {
	it, err := f.memdb.Get(ctx, tableName, key)
	if err != nil {
		return f.fsdb.Get(ctx, tableName, key)
	}
	return it, nil
}

// validateConsistency check if using optimisticLock then compare db.version with it.OldVersion
func (f *Adapter) validateConsistency(ctx context.Context, tableName string, it dto.Item) error {
	if it.Version != nil { // remover code duplicado
		itdb, err := f.getEntityMemThenFS(ctx, tableName, it.Key)
		if err != nil && !errors.Is(ErrNotFound, err) {
			return fmt.Errorf("error on optimistic set :%w", err)
		}

		// validate if db version is equal old version received
		if itdb.Version != it.Version.OldVersion {
			return ErrNotCompatibleVersion
		}

	}

	return nil
}
