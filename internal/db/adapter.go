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
	mu    sync.Mutex
	store DB
	logdb LogDb
}

func New(store DB, logDb LogDb) *Adapter {
	return &Adapter{
		store: store,
		logdb: logDb,
	}

}

func (f *Adapter) CreateTable(table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.store.CreateTable(table)
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

	if err := f.store.Set(ctx, tableName, entity); err != nil {
		return fmt.Errorf("db: put entity: %w", err)
	}
	return nil
}

// Get reads a value from persistent storage (filesystem-backed store behind this adapter).
func (f *Adapter) Get(ctx context.Context, tableName string, key string) (item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.store.Get(ctx, tableName, key)
}

func (f *Adapter) GetBySecondaryKey(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.store.GetBySk(ctx, tableName, secondaryKey)
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

	if err := f.store.Del(ctx, tableName, e.Key); err != nil {
		return fmt.Errorf("db: delete entity: %w", err)
	}

	return nil
}

func (f *Adapter) Load(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Load(ctx, f.store); err != nil {
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
	if flusher, ok := f.store.(interface {
		Flush(ctx context.Context) error
	}); ok {
		_ = flusher.Flush(context.Background())
	}
	return f.logdb.Close()

}

// validateConsistency check if using optimisticLock then compare db.version with it.OldVersion
func (f *Adapter) validateConsistency(ctx context.Context, tableName string, it dto.Item) error {
	if it.Version != nil { // remover code duplicado
		itdb, err := f.store.Get(ctx, tableName, it.Key)
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
