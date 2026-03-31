package store

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

const WalFileName = "data.wal"

// WAL is an append-only log file.
type WAL struct {
	path string
	f    *os.File
	opts Options
	bufw *bufio.Writer
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

func openWAL(path string, opts Options) (*WAL, error) {
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

	var goodOff int64 = 0
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
