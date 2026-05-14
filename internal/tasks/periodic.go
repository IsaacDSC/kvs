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

// FSPendingSizer is implemented by flush targets that expose dirty-buffer stats
// (e.g. *fsdb.WriteBatcher) so a periodic task can flush on size/memory pressure
// between wall-clock ticks, aligned with the batcher's own limits.
type FSPendingSizer interface {
	PendingDirty() (keys int, bytes int64)
}

// FSPeriodicFlushLimits groups triggers for the node-level periodic fs flush loop:
//   - Interval: wall-clock tick (required, >0).
//   - MaxPendingKeys / MaxPendingBytes: when PendingPollEvery > 0 and sizer is non-nil,
//     a flush runs if pending keys or bytes reach these thresholds (typically same as
//     the WriteBatcher coalescing limits).
//   - PendingPollEvery: how often to check pending size; zero disables size-driven flushes
//     from this loop (the batcher still flushes on Set/Del when over limits).
//   - PerFlushTimeout: optional upper bound for each Flush call (uses ctx cancellation).
type FSPeriodicFlushLimits struct {
	Interval         time.Duration
	MaxPendingKeys   int
	MaxPendingBytes  int64
	PendingPollEvery time.Duration
	PerFlushTimeout  time.Duration
}

func shouldPeriodicFlushByPending(keys int, bytes int64, lim FSPeriodicFlushLimits) bool {
	if lim.MaxPendingKeys > 0 && keys >= lim.MaxPendingKeys {
		return true
	}
	if lim.MaxPendingBytes > 0 && bytes >= lim.MaxPendingBytes {
		return true
	}
	return false
}

func runFSFlushOnce(ctx context.Context, logger *slog.Logger, fs FSFlusher, lim FSPeriodicFlushLimits) {
	flushCtx := ctx
	cancel := func() {}
	if lim.PerFlushTimeout > 0 {
		flushCtx, cancel = context.WithTimeout(ctx, lim.PerFlushTimeout)
	}
	defer cancel()
	if err := fs.Flush(flushCtx); err != nil {
		logger.Error("periodic fsdb batch flush failed", "error", err)
	}
}

// RunPeriodicFSFlush calls fs.Flush on Interval ticks and, when sizer and poll are set,
// whenever pending dirty keys/bytes hit MaxPendingKeys / MaxPendingBytes, until ctx is cancelled.
func RunPeriodicFSFlush(ctx context.Context, logger *slog.Logger, fs FSFlusher, lim FSPeriodicFlushLimits, sizer FSPendingSizer) {
	if lim.Interval <= 0 {
		return
	}
	t := time.NewTicker(lim.Interval)
	defer t.Stop()

	// TODO: levar depois para ser por evento
	var poll *time.Ticker
	if sizer != nil && lim.PendingPollEvery > 0 && (lim.MaxPendingKeys > 0 || lim.MaxPendingBytes > 0) {
		poll = time.NewTicker(lim.PendingPollEvery)
		defer poll.Stop()
	}

	for {
		if poll == nil {
			select {
			case <-t.C:
				runFSFlushOnce(ctx, logger, fs, lim)
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case <-t.C:
			runFSFlushOnce(ctx, logger, fs, lim)
		case <-poll.C:
			keys, bytes := sizer.PendingDirty()
			if shouldPeriodicFlushByPending(keys, bytes, lim) {
				runFSFlushOnce(ctx, logger, fs, lim)
			}
		case <-ctx.Done():
			return
		}
	}
}
