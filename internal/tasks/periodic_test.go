package tasks

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type fakePeriodicFS struct {
	flushCalls atomic.Int32
	pendKeys   atomic.Int32
	pendBytes  atomic.Int64
}

func (f *fakePeriodicFS) Flush(context.Context) error {
	f.flushCalls.Add(1)
	return nil
}

func (f *fakePeriodicFS) PendingDirty() (int, int64) {
	return int(f.pendKeys.Load()), f.pendBytes.Load()
}

func TestRunPeriodicFSFlush_sizeTriggerBeforeInterval(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs := &fakePeriodicFS{}
	fs.pendKeys.Store(100)
	fs.pendBytes.Store(1024)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunPeriodicFSFlush(ctx, logger, fs, FSPeriodicFlushLimits{
		Interval:         time.Hour,
		MaxPendingKeys:   10,
		MaxPendingBytes:  0,
		PendingPollEvery: 15 * time.Millisecond,
	}, fs)

	time.Sleep(80 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if n := fs.flushCalls.Load(); n < 1 {
		t.Fatalf("expected at least one flush from pending size trigger, got %d", n)
	}
}

func TestRunPeriodicFSFlush_intervalOnlyNoSizer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs := &fakePeriodicFS{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunPeriodicFSFlush(ctx, logger, fs, FSPeriodicFlushLimits{
		Interval:         40 * time.Millisecond,
		MaxPendingKeys:   0,
		MaxPendingBytes:  0,
		PendingPollEvery: 0,
	}, nil)

	time.Sleep(95 * time.Millisecond)
	cancel()

	if n := fs.flushCalls.Load(); n < 1 {
		t.Fatalf("expected interval flush, got %d", n)
	}
}

func TestRunPeriodicFSFlush_zeroIntervalNoop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs := &fakePeriodicFS{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		RunPeriodicFSFlush(ctx, logger, fs, FSPeriodicFlushLimits{Interval: 0}, fs)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("expected immediate return when Interval <= 0")
	}
	cancel()

	if fs.flushCalls.Load() != 0 {
		t.Fatalf("expected no flushes, got %d", fs.flushCalls.Load())
	}
}
