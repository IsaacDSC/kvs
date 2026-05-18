package setup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/IsaacDSC/kvs/internal/cache"
	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/tasks"
	"github.com/IsaacDSC/kvs/internal/wal"
)

// KVStore is the full KV persistence stack: filesystem DB + write batcher + WAL + adapter.
type KVStore struct {
	adapter   *db.Adapter
	batchedFS *fsdb.WriteBatcher
}

// OpenKV creates the KV persistence stack from flags and runtime config, replays
// the WAL to restore state, and starts background maintenance goroutines
// (periodic checkpoint, periodic FS flush) tied to ctx.
// Close must be called at shutdown regardless of ctx cancellation.
func OpenKV(ctx context.Context, flags cfg.NodeFlags, conf cfg.Config, codec wal.Codec, logger *slog.Logger) (*KVStore, error) {
	if err := os.MkdirAll(filepath.Dir(flags.WALPath), 0o755); err != nil {
		return nil, fmt.Errorf("setup: kv wal dir: %w", err)
	}
	if err := os.MkdirAll(flags.FsDefaultDir, 0o755); err != nil {
		return nil, fmt.Errorf("setup: kv fs dir: %w", err)
	}

	rawFS := fsdb.NewDb(flags.FsDefaultDir)
	batchOpts := fsdb.WriteBatcherOptions{}
	if conf.FSDeferWrites {
		batchOpts.DeferWrites = true
	}
	batchedFS := fsdb.NewWriteBatcher(rawFS, batchOpts)

	kvWAL, err := wal.New(flags.WALPath, wal.Options{
		Durability:      wal.SyncEveryWrite,
		CheckpointDir:   flags.CheckpointDefaultDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
		BeforeCheckpoint: func(ckptCtx context.Context) error {
			return batchedFS.Flush(ckptCtx)
		},
	}, codec)
	if err != nil {
		batchedFS.Stop()
		return nil, fmt.Errorf("setup: kv wal open: %w", err)
	}

	cc := cache.New[item.Entity](conf.CacheMaxEntries, conf.CacheTTL)
	adapter := db.New(batchedFS, kvWAL, cc)

	if err := adapter.Load(ctx); err != nil {
		_ = adapter.Close()
		batchedFS.Stop()
		return nil, fmt.Errorf("setup: kv wal replay: %w", err)
	}

	if conf.CheckpointInterval > 0 {
		go tasks.RunPeriodicWALCheckpoint(ctx, logger, kvWAL, conf.CheckpointInterval)
	}

	if conf.FSDeferWrites && conf.FSFlushInterval > 0 {
		maxKeys, maxBytes := batchedFS.DirtyFlushThresholds()
		go tasks.RunPeriodicFSFlush(ctx, logger, batchedFS, tasks.FSPeriodicFlushLimits{
			Interval:         conf.FSFlushInterval,
			MaxPendingKeys:   maxKeys,
			MaxPendingBytes:  maxBytes,
			PendingPollEvery: conf.FSPeriodicPoll,
			PerFlushTimeout:  conf.FSFlushOpTimeout,
		}, batchedFS)
	}

	return &KVStore{adapter: adapter, batchedFS: batchedFS}, nil
}

// DB returns the database adapter for use by API handlers and the applied-entry loop.
func (s *KVStore) DB() *db.Adapter {
	return s.adapter
}

// Close flushes pending writes to the filesystem and closes the WAL.
func (s *KVStore) Close() error {
	err := s.adapter.Close()
	s.batchedFS.Stop()
	return err
}
