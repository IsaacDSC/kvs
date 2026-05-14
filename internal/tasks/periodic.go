package tasks

import (
	"context"
	"log/slog"
	"time"

	"github.com/IsaacDSC/kvs/internal/wal"
)

// RunPeriodicWALCheckpoint calls w.Checkpoint on every tick until ctx is cancelled.
func RunPeriodicWALCheckpoint(ctx context.Context, logger *slog.Logger, w *wal.WAL, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := w.Checkpoint(); err != nil {
				logger.Error("periodic wal checkpoint failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// FSFlusher is satisfied by types that periodically persist buffered data (e.g. fsdb.WriteBatcher).
type FSFlusher interface {
	Flush(ctx context.Context) error
}

// RunPeriodicFSFlush calls fs.Flush on every tick until ctx is cancelled.
func RunPeriodicFSFlush(ctx context.Context, logger *slog.Logger, fs FSFlusher, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := fs.Flush(context.Background()); err != nil {
				logger.Error("periodic fsdb batch flush failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
