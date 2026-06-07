package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/internal/item"
)

type Cache[T item.Entity] interface {
	Once(key string, fn func() (T, error)) (T, error)
	DelIfOk(key string, fn func() error) error
	SaveIfOk(key string, value T, fn func() error) error
	// Del evicts key if present; a missing key is a no-op and never an error.
	Del(key string)
}

type DB interface {
	CreateTable(tableName string) error
	Set(ctx context.Context, tableName string, entity item.Entity) error
	BulkSet(ctx context.Context, tableName string, entities []item.Entity) error
	BulkDel(ctx context.Context, tableName string, keys []string) error
	Get(ctx context.Context, tableName string, key string) (item.Entity, error)
	GetBySk(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error)
	Del(ctx context.Context, tableName string, key string) error
}

type LogDb interface {
	Set(ctx context.Context, tableName string, entity item.Entity) error
	BulkSet(ctx context.Context, tableName string, entities []item.Entity) error
	BulkDelete(ctx context.Context, tableName string, keys []string) error
	Delete(ctx context.Context, tableName string, key string) error
	Load(ctx context.Context, operations ...commands.Operations) error
	Close() error
}

type Adapter struct {
	mu    sync.Mutex
	store DB
	logdb LogDb
	cache Cache[item.Entity]
}

func New(store DB, logDb LogDb, cache Cache[item.Entity]) *Adapter {
	return &Adapter{
		store: store,
		logdb: logDb,
		cache: cache,
	}
}

func (f *Adapter) CreateTable(table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.store.CreateTable(table)
}

func (f *Adapter) BulkSet(ctx context.Context, tableName string, its dto.Items) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entities := its.Entities()

	if err := f.logdb.BulkSet(ctx, tableName, entities); err != nil {
		return fmt.Errorf("db: wal set: %w", err)
	}

	if err := f.store.BulkSet(ctx, tableName, entities); err != nil {
		return fmt.Errorf("db: put entity: %w", err)
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

	key := f.key(entity.Key)
	return f.cache.SaveIfOk(key, entity, func() error {
		if err := f.store.Set(ctx, tableName, entity); err != nil {
			return fmt.Errorf("db: put entity: %w", err)
		}

		return nil
	})
}

// Get reads a value from persistent storage (filesystem-backed store behind this adapter).
func (f *Adapter) Get(ctx context.Context, tableName string, key string) (item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.cache.Once(f.key(key), func() (item.Entity, error) {
		return f.store.Get(ctx, tableName, key)
	})

}

func (f *Adapter) GetBySecondaryKey(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.store.GetBySk(ctx, tableName, secondaryKey)
}

// ApplyReplicated applies a committed Raft entry unconditionally on a follower.
//
// Unlike Set, this bypasses validateConsistency: optimistic-lock preconditions are
// a client-side business rule that the leader already enforced before proposing to
// Raft. Re-running that check on followers produces false positives — particularly
// after a restart, when the Raft log (in-memory only) is re-delivered in full by the
// leader while the WAL has already advanced the store to the final state.
//
// TODO: eliminate the catch-up replay entirely by persisting the Raft log so
// lastApplied is correctly restored on restart (see raft.Node.log TODO comment).
func (f *Adapter) ApplyReplicated(ctx context.Context, tableName string, it dto.Item) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entity := it.Entity()
	if err := f.logdb.Set(ctx, tableName, entity); err != nil {
		return fmt.Errorf("db: wal set (replicated): %w", err)
	}

	key := f.key(entity.Key)
	return f.cache.SaveIfOk(key, entity, func() error {
		if err := f.store.Set(ctx, tableName, entity); err != nil {
			return fmt.Errorf("db: put entity: %w", err)
		}

		return nil
	})

}

// ApplyReplicatedBulk applies a committed Raft bulk-put entry on a follower. Like
// ApplyReplicated, it bypasses validateConsistency (the leader already enforced any
// preconditions before proposing) and writes the whole batch through the WAL then the store.
func (f *Adapter) ApplyReplicatedBulk(ctx context.Context, tableName string, its dto.Items) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entities := its.Entities()

	if err := f.logdb.BulkSet(ctx, tableName, entities); err != nil {
		return fmt.Errorf("db: wal bulk set (replicated): %w", err)
	}

	if err := f.store.BulkSet(ctx, tableName, entities); err != nil {
		return fmt.Errorf("db: bulk put entity (replicated): %w", err)
	}

	return nil
}

// BulkDel deletes the whole batch through the WAL, then the store, then evicts the
// deleted keys from the in-memory cache. There is no optimistic-lock validation on
// the bulk path; missing keys are skipped by the store (idempotent delete).
func (f *Adapter) BulkDel(ctx context.Context, tableName string, its dto.DeleteItems) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.bulkDelLocked(ctx, tableName, its, "")
}

// ApplyReplicatedBulkDelete applies a committed Raft bulk-delete entry on a follower.
// See ApplyReplicated for the rationale (no consistency validation on replay).
func (f *Adapter) ApplyReplicatedBulkDelete(ctx context.Context, tableName string, its dto.DeleteItems) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.bulkDelLocked(ctx, tableName, its, " (replicated)")
}

// bulkDelLocked is the shared body of BulkDel and ApplyReplicatedBulkDelete: the bulk
// path never validates versions, so leader and follower run the same sequence
// (log → disk → memory); suffix only distinguishes the error context.
func (f *Adapter) bulkDelLocked(ctx context.Context, tableName string, its dto.DeleteItems, suffix string) error {
	keys := its.Keys()

	if err := f.logdb.BulkDelete(ctx, tableName, keys); err != nil {
		return fmt.Errorf("db: wal bulk delete%s: %w", suffix, err)
	}

	if err := f.store.BulkDel(ctx, tableName, keys); err != nil {
		return fmt.Errorf("db: bulk delete entity%s: %w", suffix, err)
	}

	// One store call for N keys: evict each from the memdb after the disk delete
	// succeeded so reads never serve a deleted item. Eviction never fails the batch.
	for _, k := range keys {
		f.cache.Del(f.key(k))
	}

	return nil
}

// ApplyReplicatedDelete applies a committed Raft delete unconditionally on a follower.
// See ApplyReplicated for the rationale.
func (f *Adapter) ApplyReplicatedDelete(ctx context.Context, tableName string, it dto.DeleteItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.logdb.Delete(ctx, tableName, it.Key); err != nil {
		return fmt.Errorf("db: wal delete (replicated): %w", err)
	}
	return f.cache.DelIfOk(f.key(it.Key), func() error {
		if err := f.store.Del(ctx, tableName, it.Key); err != nil {
			return fmt.Errorf("db: delete entity (replicated): %w", err)
		}
		return nil
	})
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

	return f.cache.DelIfOk(f.key(it.Key), func() error {
		if err := f.store.Del(ctx, tableName, e.Key); err != nil {
			return fmt.Errorf("db: delete entity: %w", err)
		}

		return nil
	})
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

func (f *Adapter) key(args ...string) string {
	keys := []string{"kvs:adapter:cache:mem"}
	keys = append(keys, args...)
	return strings.Join(keys, ":")
}
