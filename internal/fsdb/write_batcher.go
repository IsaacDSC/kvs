package fsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

// WriteBatcherOptions configures coalescing and flush triggers for filesystem writes.
// See WriteBatcher for durability semantics.
type WriteBatcherOptions struct {
	// MaxDirtyKeys triggers a flush when the number of distinct merge keys in the buffer
	// reaches this value (only when DeferWrites is true). Zero defaults to 4096.
	MaxDirtyKeys int
	// MaxDirtyBytes triggers a flush when the estimated byte size of buffered values reaches
	// this threshold (only when DeferWrites is true). Zero defaults to 32 MiB.
	MaxDirtyBytes int64
	// FlushInterval, when > 0 and DeferWrites is true, runs periodic FlushAll in the background.
	FlushInterval time.Duration
	// DeferWrites enables Option A style batching: Set/Del return after updating the in-memory
	// LWW map; data reaches the inner store on flush (limits, timer, explicit Flush, or read
	// paths that require a consistent view). When false (default), every Set/Del flushes the
	// full buffer before returning (Option B relative to fsdb), which keeps WAL checkpoint
	// ordering safe without extra hooks in logdb.
	DeferWrites bool
}

type mergeKey struct {
	table string
	key   string
}

type batchOpKind int

const (
	batchOpSet batchOpKind = iota
	batchOpDel
)

type batchOp struct {
	kind   batchOpKind
	entity item.Entity // valid when kind == batchOpSet
}

// WriteBatcher sits between callers and the concrete fsdb implementation, coalescing writes
// by (table, primary key) with last-write-wins semantics. Flushes apply at most one Set or
// one Del per merge key to the inner DB, in deterministic merge_key order.
//
// Durability (fsdb only, see specs/reducao-pressao-filesystem-batch-merge.md):
//   - DeferWrites == false: Option B — each Set/Del returns after all pending entries have
//     been flushed to the inner store (safe with WAL LastSeq checkpoint assumptions).
//   - DeferWrites == true: Option A — return after enqueue; callers must invoke Flush before
//     any operation that assumes fsdb reflects WAL through a sequence (e.g. periodic checkpoint).
type WriteBatcher struct {
	inner    db.DB
	opts     WriteBatcherOptions
	mu       sync.Mutex
	pend     map[mergeKey]batchOp
	bytesN   int64
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewWriteBatcher wraps inner (typically *Db). opts are copied and defaulted by the constructor.
func NewWriteBatcher(inner db.DB, opts WriteBatcherOptions) *WriteBatcher {
	b := &WriteBatcher{
		inner: inner,
		opts:  opts.normalized(),
		pend:  make(map[mergeKey]batchOp),
	}
	if b.opts.DeferWrites && b.opts.FlushInterval > 0 {
		b.stopCh = make(chan struct{})
		b.doneCh = make(chan struct{})
		go b.loopPeriodicFlush()
	}
	return b
}

func (o WriteBatcherOptions) normalized() WriteBatcherOptions {
	if o.MaxDirtyKeys <= 0 {
		o.MaxDirtyKeys = 4096
	}
	if o.MaxDirtyBytes <= 0 {
		o.MaxDirtyBytes = 32 << 20
	}
	return o
}

func (b *WriteBatcher) loopPeriodicFlush() {
	defer close(b.doneCh)
	t := time.NewTicker(b.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = b.Flush(context.Background())
		case <-b.stopCh:
			return
		}
	}
}

// DirtyFlushThresholds returns the configured MaxDirtyKeys and MaxDirtyBytes
// (after normalization in [NewWriteBatcher]). Used by periodic flush tasks to
// align time- and size-driven triggers with the batcher.
func (b *WriteBatcher) DirtyFlushThresholds() (maxKeys int, maxBytes int64) {
	return b.opts.MaxDirtyKeys, b.opts.MaxDirtyBytes
}

// PendingDirty returns the number of distinct merge keys and the estimated
// byte size of the coalescing buffer. It acquires the batcher mutex briefly.
func (b *WriteBatcher) PendingDirty() (keys int, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pend), b.bytesN
}

// Stop ends the periodic flusher goroutine, if any, and waits for it to exit.
func (b *WriteBatcher) Stop() {
	b.mu.Lock()
	done := b.doneCh
	stop := b.stopCh
	b.mu.Unlock()
	if stop == nil {
		return
	}
	b.stopOnce.Do(func() { close(stop) })
	if done != nil {
		<-done
	}
}

// Flush persists all pending coalesced operations to the inner DB (sorted by merge key).
func (b *WriteBatcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushAllLocked(ctx)
}

func (b *WriteBatcher) flushAllLocked(ctx context.Context) error {
	if len(b.pend) == 0 {
		return nil
	}
	keys := make([]mergeKey, 0, len(b.pend))
	for k := range b.pend {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].table != keys[j].table {
			return keys[i].table < keys[j].table
		}
		return keys[i].key < keys[j].key
	})
	for _, mk := range keys {
		op := b.pend[mk]
		if err := b.applyOneLocked(ctx, mk, op); err != nil {
			return err
		}
		delete(b.pend, mk)
	}
	b.bytesN = 0
	return nil
}

func (b *WriteBatcher) applyOneLocked(ctx context.Context, mk mergeKey, op batchOp) error {
	switch op.kind {
	case batchOpSet:
		if err := b.inner.Set(ctx, mk.table, op.entity); err != nil {
			return fmt.Errorf("fsdb write batcher: set %q/%q: %w", mk.table, mk.key, err)
		}
	case batchOpDel:
		if err := b.inner.Del(ctx, mk.table, mk.key); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return nil
			}
			return fmt.Errorf("fsdb write batcher: del %q/%q: %w", mk.table, mk.key, err)
		}
	default:
		return fmt.Errorf("fsdb write batcher: unknown op kind %d", op.kind)
	}
	return nil
}

func (b *WriteBatcher) opBytes(op batchOp) int64 {
	switch op.kind {
	case batchOpSet:
		raw, err := json.Marshal(op.entity)
		if err != nil {
			return int64(len(op.entity.Key) + len(op.entity.SK) + 64)
		}
		return int64(len(raw))
	case batchOpDel:
		return 16
	default:
		return 0
	}
}

// mergeKeyBytes is a rough contribution for map size accounting.
func mergeKeyBytes(mk mergeKey) int64 {
	return int64(len(mk.table) + len(mk.key))
}

func (b *WriteBatcher) upsertLocked(mk mergeKey, next batchOp) {
	old, had := b.pend[mk]
	if had {
		b.bytesN -= b.opBytes(old)
	} else {
		b.bytesN += mergeKeyBytes(mk)
	}
	// LWW: each new op replaces the coalesced state for this merge key.
	b.pend[mk] = next
	b.bytesN += b.opBytes(next)
}

func (b *WriteBatcher) overLimitsLocked() bool {
	if len(b.pend) >= b.opts.MaxDirtyKeys {
		return true
	}
	if b.bytesN >= b.opts.MaxDirtyBytes {
		return true
	}
	return false
}

func (b *WriteBatcher) CreateTable(name string) error {
	return b.inner.CreateTable(name)
}

func (b *WriteBatcher) Set(ctx context.Context, tableName string, entity item.Entity) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	mk := mergeKey{table: tableName, key: entity.Key}
	b.upsertLocked(mk, batchOp{kind: batchOpSet, entity: entity})

	if b.opts.DeferWrites {
		if b.overLimitsLocked() {
			if err := b.flushAllLocked(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	return b.flushAllLocked(ctx)
}

func (b *WriteBatcher) BulkSet(ctx context.Context, tableName string, entities []item.Entity) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, entity := range entities {
		mk := mergeKey{table: tableName, key: entity.Key}
		b.upsertLocked(mk, batchOp{kind: batchOpSet, entity: entity})
	}

	if b.opts.DeferWrites {
		if b.overLimitsLocked() {
			if err := b.flushAllLocked(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	return b.flushAllLocked(ctx)
}

func (b *WriteBatcher) Del(ctx context.Context, tableName string, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	mk := mergeKey{table: tableName, key: key}
	b.upsertLocked(mk, batchOp{kind: batchOpDel})

	if b.opts.DeferWrites {
		if b.overLimitsLocked() {
			if err := b.flushAllLocked(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	return b.flushAllLocked(ctx)
}

func (b *WriteBatcher) Get(ctx context.Context, tableName string, key string) (item.Entity, error) {
	b.mu.Lock()
	if b.opts.DeferWrites {
		mk := mergeKey{table: tableName, key: key}
		if op, ok := b.pend[mk]; ok {
			switch op.kind {
			case batchOpSet:
				e := op.entity
				b.mu.Unlock()
				return e, nil
			case batchOpDel:
				b.mu.Unlock()
				return item.Entity{}, db.ErrNotFound
			}
		}
	}
	b.mu.Unlock()
	return b.inner.Get(ctx, tableName, key)
}

func (b *WriteBatcher) GetBySk(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error) {
	// Conservative: secondary index reads need a materialized table view.
	if err := b.Flush(ctx); err != nil {
		return nil, err
	}
	return b.inner.GetBySk(ctx, tableName, secondaryKey)
}

// ReplaceWithCheckpointBlobs clears buffered writes and restores the inner store from WAL checkpoint blobs.
func (b *WriteBatcher) ReplaceWithCheckpointBlobs(ctx context.Context, tables map[string]map[string][]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pend = make(map[mergeKey]batchOp)
	b.bytesN = 0

	h, ok := b.inner.(db.CheckpointBlobHydrator)
	if !ok {
		return fmt.Errorf("fsdb write batcher: inner %T does not implement db.CheckpointBlobHydrator", b.inner)
	}
	return h.ReplaceWithCheckpointBlobs(ctx, tables)
}

var _ db.CheckpointBlobHydrator = (*WriteBatcher)(nil)
