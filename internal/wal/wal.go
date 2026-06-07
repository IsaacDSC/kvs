package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// WAL is an append-only log file.
type WAL struct {
	path  string
	f     *os.File
	opts  Options
	bufw  *bufio.Writer
	seq   uint64
	codec Codec
	mu    sync.Mutex

	writesSinceCkpt uint64
	bytesSinceCkpt  int64
}

func New(path string, opts Options, codec Codec) (*WAL, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, f: f, opts: opts, codec: codec}
	if opts.Durability == Buffered {
		w.bufw = bufio.NewWriter(f)
	}
	return w, nil
}

func (w *WAL) Set(ctx context.Context, tableName string, entity item.Entity) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	b, err := w.codec.Encode(entity)
	if err != nil {
		return fmt.Errorf("wal: encode: %w", err)
	}

	seq := w.seq + 1
	if err := w.appendLocked(Entry{
		Seq:        seq,
		Op:         OpSet,
		Table:      tableName,
		Key:        entity.Key,
		Fk:         entity.SK,
		ValueBytes: b,
	}); err != nil {
		return fmt.Errorf("wal: append: %w", err)
	}
	w.seq = seq
	return w.maybeAutoCheckpointLocked()
}

// BulkSet appends one OpSet entry per entity under a single lock, assigning
// monotonically increasing sequence numbers, then evaluates the auto-checkpoint
// policy once. On append failure, w.seq reflects the entries already written.
func (w *WAL) BulkSet(ctx context.Context, tableName string, entities []item.Entity) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.seq
	for _, entity := range entities {
		b, err := w.codec.Encode(entity)
		if err != nil {
			w.seq = seq
			return fmt.Errorf("wal: encode: %w", err)
		}

		seq++
		if err := w.appendLocked(Entry{
			Seq:        seq,
			Op:         OpSet,
			Table:      tableName,
			Key:        entity.Key,
			Fk:         entity.SK,
			ValueBytes: b,
		}); err != nil {
			w.seq = seq - 1
			return fmt.Errorf("wal: append: %w", err)
		}
		w.seq = seq
	}

	return w.maybeAutoCheckpointLocked()
}

func (w *WAL) Delete(ctx context.Context, tableName string, key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.seq + 1
	if err := w.appendLocked(Entry{
		Seq:   seq,
		Op:    OpDel,
		Table: tableName,
		Key:   key,
	}); err != nil {
		return fmt.Errorf("db: wal append: %w", err)
	}
	w.seq = seq
	return w.maybeAutoCheckpointLocked()
}

// Load recovers fsdb-compatible state after a checkpoint metadata write.
//
// When CheckpointDir is set, LastSeq defines the WAL prefix already materialised on durable
// storage; only entries with Seq > LastSeq are replayed onto the single [commands.Operations]
// target (typically *fsdb.Db or [fsdb.WriteBatcher]). If checkpoint.cbor embeds optional table blobs,
// the target must implement [db.CheckpointBlobHydrator] — those blobs are applied first, then
// the tail replay runs.
//
// After successful recovery, when checkpoint is configured, Load invokes [WAL.Checkpoint]
// so LastSeq on disk advances to the replayed Seq (after [Options.BeforeCheckpoint] flushes batchers).
func (w *WAL) Load(ctx context.Context, operations ...commands.Operations) error {
	if len(operations) != 1 {
		return fmt.Errorf("wal: load requires exactly one operations target, got %d", len(operations))
	}
	opsTarget := operations[0]

	w.mu.Lock()
	defer w.mu.Unlock()

	decodeEntity := func(e Entry) (item.Entity, error) {
		var it item.Entity
		if err := w.codec.Decode(e.ValueBytes, &it); err == nil && it.Key != "" {
			return it, nil
		}
		var v any
		if err := w.codec.Decode(e.ValueBytes, &v); err != nil {
			return item.Entity{}, err
		}
		return item.Entity{Key: e.Key, SK: e.Fk, Value: v}, nil
	}

	applyEntry := func(ops commands.Operations, e Entry) error {
		if err := ops.CreateTable(e.Table); err != nil {
			return fmt.Errorf("WAL.Load.db: ensure table %q: %w", e.Table, err)
		}
		switch e.Op {
		case OpSet:
			it, err := decodeEntity(e)
			if err != nil {
				return fmt.Errorf("WAL.Load.db: decode wal put: %w", err)
			}
			if err := ops.Set(ctx, e.Table, it); err != nil {
				return fmt.Errorf("WAL.Load.db: set: %w", err)
			}
			return nil
		case OpDel:
			if err := ops.Del(ctx, e.Table, e.Key); err != nil {
				return fmt.Errorf("WAL.Load.db: del: %w", err)
			}
			return nil
		case OpBegin, OpCommit:
			// Transactions are supported by WAL format, but Facade doesn't use them yet.
			// For now, ignore them (a tx would be appended by a different component).
			return nil
		default:
			return fmt.Errorf("WAL.Load.db: unknown wal op %d", e.Op)
		}
	}

	var lastSeq uint64
	var tableBlobs map[string]map[string][]byte
	if w.opts.CheckpointConfigured() {
		var ckerr error
		lastSeq, tableBlobs, ckerr = w.opts.CheckpointStore.ReadCheckpointTables(w.opts.CheckpointDir)
		if ckerr != nil {
			return fmt.Errorf("wal: load checkpoint: %w", ckerr)
		}
	}
	hasSnap := len(tableBlobs) > 0
	if hasSnap {
		if err := w.hydrateCheckpointIfNeeded(ctx, opsTarget, tableBlobs); err != nil {
			return err
		}
	}

	maxSeq, err := w.replaySince(lastSeq, func(e Entry) error {
		return applyEntry(opsTarget, e)
	})
	if err != nil {
		return fmt.Errorf("db: replay wal: %w", err)
	}
	w.seq = maxUint64(lastSeq, maxSeq)

	// Persist LastSeq after recovery so the next boot skips replayed prefix; the tail store
	// was updated for Seq > lastSeq, and Seq <= lastSeq is assumed already materialized there.
	if w.opts.CheckpointConfigured() {
		if err := w.checkpointLocked(); err != nil {
			return fmt.Errorf("wal: post-load checkpoint: %w", err)
		}
		w.writesSinceCkpt = 0
		w.bytesSinceCkpt = 0
	}
	return nil
}

func (w *WAL) hydrateCheckpointIfNeeded(ctx context.Context, first commands.Operations, blobs map[string]map[string][]byte) error {
	if len(blobs) == 0 {
		return nil
	}
	h, ok := first.(db.CheckpointBlobHydrator)
	if !ok {
		return fmt.Errorf("wal: checkpoint has table data but %T does not implement db.CheckpointBlobHydrator", first)
	}
	if err := h.ReplaceWithCheckpointBlobs(ctx, blobs); err != nil {
		return fmt.Errorf("wal: apply checkpoint snapshot: %w", err)
	}
	return nil
}

// Checkpoint flushes the WAL, runs BeforeCheckpoint (so e.g. fsdb reflects all writes through
// the current w.seq), then atomically persists LastSeq = w.seq.
//
// Ordering: durable on-disk state and BeforeCheckpoint completion must cover every Seq <= w.seq
// before SaveLastSeq; otherwise recovery could skip WAL entries that were never materialized.
// After SaveLastSeq, if TruncateAfterCheckpoint is set, the WAL file is truncated; new appends
// continue with monotonically increasing Seq.
//
// Load invokes Checkpoint automatically when checkpoint is configured and replay succeeded.
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.checkpointLocked()
}

func (w *WAL) checkpointLocked() error {
	if !w.opts.CheckpointConfigured() {
		return errors.New("wal: checkpoint dir not configured")
	}
	if err := w.flushLocked(); err != nil {
		return fmt.Errorf("wal: checkpoint flush: %w", err)
	}
	if w.opts.BeforeCheckpoint != nil {
		if err := w.opts.BeforeCheckpoint(context.Background()); err != nil {
			return fmt.Errorf("wal: before checkpoint: %w", err)
		}
	}
	if err := w.opts.CheckpointStore.SaveLastSeq(w.opts.CheckpointDir, w.seq); err != nil {
		return fmt.Errorf("wal: checkpoint save seq: %w", err)
	}
	if w.opts.CheckpointPolicy.TruncateAfterCheckpoint {
		if err := w.truncate(0); err != nil {
			return fmt.Errorf("wal: checkpoint truncate: %w", err)
		}
		if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}
	w.writesSinceCkpt = 0
	w.bytesSinceCkpt = 0
	return nil
}

func (w *WAL) maybeAutoCheckpointLocked() error {
	if !w.opts.CheckpointConfigured() {
		return nil
	}
	trigger := false
	if w.opts.CheckpointPolicy.EveryNWrites > 0 && w.writesSinceCkpt >= w.opts.CheckpointPolicy.EveryNWrites {
		trigger = true
	}
	if w.opts.CheckpointPolicy.MaxWalBytes > 0 && w.bytesSinceCkpt >= w.opts.CheckpointPolicy.MaxWalBytes {
		trigger = true
	}
	if !trigger {
		return nil
	}
	return w.checkpointLocked()
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (w *WAL) syncAndHook() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	if w.opts.AfterSync != nil {
		w.opts.AfterSync()
	}
	return nil
}

// Open creates (if missing) and opens a WAL file for replay and append.
func Open(path string, opts Options) (*WAL, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, f: f, opts: opts}
	if opts.Durability == Buffered {
		w.bufw = bufio.NewWriter(f)
	}
	return w, nil
}

// Append adds one frame to the log. The caller must not assume w.seq is updated; callers that
// maintain sequence (e.g. Set) assign w.seq explicitly. Append counts toward automatic checkpoint thresholds.
func (w *WAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(e)
}

func (w *WAL) appendLocked(e Entry) error {
	rec, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if w.bufw != nil {
		if _, err := w.bufw.Write(rec); err != nil {
			return err
		}
		if w.opts.Durability == SyncEveryWrite {
			if err := w.bufw.Flush(); err != nil {
				return err
			}
			if err := w.syncAndHook(); err != nil {
				return err
			}
		}
	} else {
		if _, err := w.f.Write(rec); err != nil {
			return err
		}
		if w.opts.Durability == SyncEveryWrite {
			if err := w.syncAndHook(); err != nil {
				return err
			}
		}
	}
	w.writesSinceCkpt++
	w.bytesSinceCkpt += int64(len(rec))
	return nil
}

// Flush flushes buffered WAL data and syncs to disk.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *WAL) flushLocked() error {
	if w.bufw != nil {
		if err := w.bufw.Flush(); err != nil {
			return err
		}
	}
	return w.syncAndHook()
}

// Replay reads from the beginning of the file and invokes apply for each valid record.
// Incomplete trailing data is truncated so future appends succeed.
// Returns the maximum Seq seen in the file.
func (w *WAL) Replay(apply func(Entry) error) (maxSeq uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.replaySince(0, apply)
}

// replaySince parses the WAL from the start, updates maxSeq for every valid record, and
// invokes apply only for entries with Seq > skipThroughSeq (checkpoint tail replay).
func (w *WAL) replaySince(skipThroughSeq uint64, apply func(Entry) error) (maxSeq uint64, err error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var goodOff int64
	reader := io.Reader(w.f)

	for {
		var lenBuf [4]byte
		n, readErr := io.ReadFull(reader, lenBuf[:])
		if readErr == io.EOF && n == 0 {
			break
		}
		if readErr == io.EOF || n < 4 {
			if truncErr := w.truncate(goodOff); truncErr != nil {
				return maxSeq, truncErr
			}
			break
		}
		if readErr != nil {
			return maxSeq, readErr
		}

		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		if payloadLen > 1<<28 {
			if truncErr := w.truncate(goodOff); truncErr != nil {
				return maxSeq, truncErr
			}
			break
		}
		frame := make([]byte, 4+payloadLen+4)
		copy(frame[:4], lenBuf[:])
		if _, err := io.ReadFull(reader, frame[4:]); err != nil {
			if truncErr := w.truncate(goodOff); truncErr != nil {
				return maxSeq, truncErr
			}
			break
		}

		var e Entry
		if err := e.UnmarshalBinary(frame); err != nil {
			if truncErr := w.truncate(goodOff); truncErr != nil {
				return maxSeq, truncErr
			}
			break
		}
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
		if e.Seq > skipThroughSeq {
			if err := apply(e); err != nil {
				return maxSeq, err
			}
		}

		goodOff += int64(len(frame))
	}

	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return maxSeq, err
	}
	return maxSeq, nil
}

func (w *WAL) truncate(off int64) error {
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// RepairTruncatesTail reads path and truncates the file to the last complete record (same rules as Replay).
func RepairTruncatesTail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var goodOff int64
	r := io.Reader(f)
	for {
		var lenBuf [4]byte
		n, readErr := io.ReadFull(r, lenBuf[:])
		if readErr == io.EOF && n == 0 {
			break
		}
		if readErr == io.EOF || n < 4 {
			return f.Truncate(goodOff)
		}
		if readErr != nil {
			return readErr
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		if payloadLen > 1<<28 {
			return f.Truncate(goodOff)
		}
		crcPayload := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(r, crcPayload); err != nil {
			return f.Truncate(goodOff)
		}
		payload := crcPayload[:payloadLen]
		got := binary.BigEndian.Uint32(crcPayload[payloadLen:])
		if crc32.ChecksumIEEE(payload) != got {
			return f.Truncate(goodOff)
		}
		if len(payload) < 15 {
			return f.Truncate(goodOff)
		}
		goodOff += int64(4 + payloadLen + 4)
	}
	return nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.bufw != nil {
		if err := w.bufw.Flush(); err != nil {
			_ = w.f.Close()
			return err
		}
	}
	if err := w.syncAndHook(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
