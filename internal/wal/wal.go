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

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/durable"
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
	b, err := w.codec.Encode(entity)
	if err != nil {
		return fmt.Errorf("wal: encode: %w", err)
	}

	seq := w.seq + 1
	if err := w.Append(Entry{
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
	return nil
}

func (w *WAL) Delete(ctx context.Context, tableName string, key string) error {
	seq := w.seq + 1
	if err := w.Append(Entry{
		Seq:   seq,
		Op:    OpDel,
		Table: tableName,
		Key:   key,
	}); err != nil {
		return fmt.Errorf("db: wal append: %w", err)
	}
	w.seq = seq
	return nil

}

func (w *WAL) Load(ctx context.Context, operations ...commands.Operations) error {
	if len(operations) == 0 {
		return errors.New("wal: load requires at least one operations")
	}

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

	// applyAll func aply in list operations received (fsdb and memdb)
	applyAll := func(targets []commands.Operations, e Entry) error {
		for _, ops := range targets {
			if err := applyEntry(ops, e); err != nil {
				return err
			}
		}
		return nil
	}

	var lastSeq uint64
	if w.opts.CheckpointConfigured() {
		var err error
		lastSeq, err = durable.LoadLastSeq(w.opts.Checkpoint.Dir)
		if err != nil {
			return fmt.Errorf("wal: load checkpoint seq: %w", err)
		}
	}

	var maxSeq uint64
	var err error
	switch len(operations) {
	case 1:
		maxSeq, err = w.replaySince(lastSeq, func(e Entry) error {
			return applyAll(operations, e)
		})
	default:
		// Volatile stores (e.g. memdb) start empty: they need the full WAL. The durable tail
		// (typically fsdb) only replays Seq > lastSeq — prefix is assumed on disk.
		prefix := operations[:len(operations)-1]
		tail := operations[len(operations)-1]
		maxSeq, err = w.replaySince(0, func(e Entry) error {
			return applyAll(prefix, e)
		})
		if err != nil {
			return fmt.Errorf("db: replay wal (full): %w", err)
		}
		maxTail, errTail := w.replaySince(lastSeq, func(e Entry) error {
			return applyEntry(tail, e)
		})
		if errTail != nil {
			return fmt.Errorf("db: replay wal (tail): %w", errTail)
		}
		if maxTail > maxSeq {
			maxSeq = maxTail
		}
	}
	if err != nil {
		return fmt.Errorf("db: replay wal: %w", err)
	}
	w.seq = maxUint64(lastSeq, maxSeq)

	// Persist LastSeq after recovery so the next boot skips replayed prefix; the tail store
	// was updated for Seq > lastSeq, and Seq <= lastSeq is assumed already materialized there.
	if w.opts.CheckpointConfigured() {
		if err := w.Checkpoint(); err != nil {
			return fmt.Errorf("wal: post-load checkpoint: %w", err)
		}
	}
	return nil
}

// Checkpoint persists LastSeq to the configured checkpoint directory after flushing the WAL.
// Load invokes Checkpoint automatically when checkpoint is configured and replay succeeded.
// When Options.BeforeCheckpoint is set (e.g. flush a deferred fsdb batcher), it runs after WAL
// flush and before SaveLastSeq. For other callers: ensure durable store already reflects all
// mutations through w.seq before calling when BeforeCheckpoint is nil.
func (w *WAL) Checkpoint() error {
	if !w.opts.CheckpointConfigured() {
		return errors.New("wal: checkpoint dir not configured")
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("wal: checkpoint flush: %w", err)
	}
	if w.opts.BeforeCheckpoint != nil {
		if err := w.opts.BeforeCheckpoint(context.Background()); err != nil {
			return fmt.Errorf("wal: before checkpoint: %w", err)
		}
	}
	if err := durable.SaveLastSeq(w.opts.Checkpoint.Dir, w.seq); err != nil {
		return fmt.Errorf("wal: checkpoint save seq: %w", err)
	}
	if w.opts.Checkpoint.TruncateAfterCheckpoint {
		if err := w.truncate(0); err != nil {
			return fmt.Errorf("wal: checkpoint truncate: %w", err)
		}
		if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}
	return nil
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

// Append adds one frame to the
func (w *WAL) Append(e Entry) error {
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
			return w.syncAndHook()
		}
		return nil
	}
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if w.opts.Durability == SyncEveryWrite {
		return w.syncAndHook()
	}
	return nil
}

// Flush flushes buffered WAL data and syncs to disk.
func (w *WAL) Flush() error {
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
