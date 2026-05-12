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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f, opts: opts, codec: codec}, nil
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

	apply := func(e Entry) error {
		for _, ops := range operations {
			if err := ops.CreateTable(e.Table); err != nil {
				return fmt.Errorf("WAL.Load.db: ensure table %q: %w", e.Table, err)
			}
		}

		switch e.Op {
		case OpSet:
			it, err := decodeEntity(e)
			if err != nil {
				return fmt.Errorf("WAL.Load.db: decode wal put: %w", err)
			}

			for _, ops := range operations {
				if err := ops.Set(ctx, e.Table, it); err != nil {
					return fmt.Errorf("WAL.Load.db: set: %w", err)
				}
			}

			return nil

		case OpDel:
			for _, ops := range operations {
				if err := ops.Del(ctx, e.Table, e.Key); err != nil {
					return fmt.Errorf("WAL.Load.db: del: %w", err)
				}
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
	maxSeq, err := w.Replay(apply)
	if err != nil {
		return fmt.Errorf("db: replay wal: %w", err)
	}
	w.seq = maxSeq
	return nil
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
		if err := apply(e); err != nil {
			return maxSeq, err
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
