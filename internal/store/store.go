package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/fxamacker/cbor/v2"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
)

var ErrEmptyTableName = errors.New("store: empty table name")

var _ db.DurableWriter = (*Store)(nil)

// Store provides durable Put/Delete backed by a WAL under dir.
type Store struct {
	mu   sync.Mutex
	dir  string
	db   *db.DB
	wal  *WAL
	opts Options

	nextSeq uint64

	// LastReplayApplied is the number of WAL entries applied during the last Open (seq > checkpoint LastSeq).
	LastReplayApplied int
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// decodePutItem restores a Put payload. New records are CBOR of db.Item; older WAL
// records store only Value (CBOR of any), with Key/Fk taken from the frame.
func decodePutItem(e Entry) (db.Item, error) {
	var item db.Item
	if err := cbor.Unmarshal(e.ValueBytes, &item); err == nil && item.Key != "" {
		if item.Key != e.Key || item.Fk != e.Fk {
			return db.Item{}, fmt.Errorf("store: wal key/fk does not match item")
		}
		return item, nil
	}
	var v any
	if err := cbor.Unmarshal(e.ValueBytes, &v); err != nil {
		return db.Item{}, err
	}
	return db.Item{Key: e.Key, Fk: e.Fk, Value: v}, nil
}

func applyEntry(database *db.DB, e Entry) error {
	t := database.GetOrCreateTable(e.Table)
	switch e.Op {
	case OpPut:
		item, err := decodePutItem(e)
		if err != nil {
			return err
		}
		return t.ApplyPut(item)
	case OpDel:
		return t.ApplyDelete(e.Key)
	default:
		return errors.New("store: unknown wal op")
	}
}

// Open creates dir if needed, loads optional checkpoint, replays WAL, and opens the log for append.
func Open(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	database := db.NewDB()
	cpSeq, err := loadCheckpoint(dir, database)
	if err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, WalFileName)
	_ = RepairTruncatesTail(walPath)
	wal, err := openWAL(walPath, opts)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, db: database, wal: wal, opts: opts}
	applied := 0
	maxSeq, err := wal.Replay(func(e Entry) error {
		if e.Seq <= cpSeq {
			return nil
		}
		applied++
		return applyEntry(database, e)
	})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	s.LastReplayApplied = applied
	s.nextSeq = maxUint64(cpSeq, maxSeq)
	database.SetDurable(s)
	return s, nil
}

func (s *Store) DB() *db.DB { return s.db }

// putLocked appends a Put to the WAL and updates memory. s.mu must be held.
func (s *Store) putLocked(table string, item db.Item) error {
	if table == "" {
		return ErrEmptyTableName
	}
	b, err := code.Encode(item)
	if err != nil {
		return err
	}
	seq := s.nextSeq + 1
	e := Entry{Seq: seq, Op: OpPut, Table: table, Key: item.Key, Fk: item.Fk, ValueBytes: b}
	if err := s.wal.Append(e); err != nil {
		return err
	}
	s.nextSeq = seq
	tb := s.db.GetOrCreateTable(table)
	return tb.ApplyPut(item)
}

// Put appends to the WAL then updates memory.
func (s *Store) Put(table string, item db.Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(table, item)
}

// deleteLocked appends a delete to the WAL and updates memory. s.mu must be held.
func (s *Store) deleteLocked(table, key string) error {
	if table == "" {
		return ErrEmptyTableName
	}
	seq := s.nextSeq + 1
	e := Entry{Seq: seq, Op: OpDel, Table: table, Key: key}
	if err := s.wal.Append(e); err != nil {
		return err
	}
	s.nextSeq = seq
	tb := s.db.GetOrCreateTable(table)
	return tb.ApplyDelete(key)
}

// Delete appends a delete record then updates memory.
func (s *Store) Delete(table, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(table, key)
}

// Checkpoint writes a snapshot including current nextSeq as LastSeq.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return saveCheckpoint(s.dir, s.db, s.nextSeq)
}

// Flush durably writes buffered WAL data.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Flush()
}

// Close flushes and closes the WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}
