package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
)

var ErrEmptyTableName = errors.New("store: empty table name")

var _ db.DurableWriter = (*Store)(nil)
var _ db.TransactionCommitter = (*Store)(nil)

// Store provides durable Put/Delete backed by a WAL under dir.
type Store struct {
	mu   sync.Mutex
	dir  string
	db   *db.DB
	wal  *WAL
	opts Options

	nextSeq  uint64
	nextTxID uint64

	// LastReplayApplied is the number of WAL frames replayed during the last Open (seq > checkpoint LastSeq).
	LastReplayApplied int
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
	rs := &replayState{db: database, cpSeq: cpSeq}
	maxSeq, err := wal.Replay(rs.apply)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	s.LastReplayApplied = rs.applied
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

// CommitTransaction writes BEGIN → muts → COMMIT to the WAL, then applies muts in memory.
// It holds s.mu for the whole operation. On Buffered durability, flushes at the end.
func (s *Store) CommitTransaction(table string, muts []db.TxMutation) error {
	if table == "" {
		return ErrEmptyTableName
	}
	for i, m := range muts {
		if m.Put != nil && m.DelKey != "" {
			return fmt.Errorf("store: tx mutation %d: both put and delete", i)
		}
		if m.Put == nil && m.DelKey == "" {
			return fmt.Errorf("store: tx mutation %d: empty", i)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextTxID++
	txid := s.nextTxID
	txBytes := EncodeTxID(txid)

	appendFrame := func(e Entry) error {
		if err := s.wal.Append(e); err != nil {
			return err
		}
		s.nextSeq = e.Seq
		return nil
	}

	seq := s.nextSeq + 1
	begin := Entry{Seq: seq, Op: OpBegin, Table: table, ValueBytes: txBytes}
	if err := appendFrame(begin); err != nil {
		return err
	}

	for _, m := range muts {
		seq = s.nextSeq + 1
		if m.Put != nil {
			b, err := code.Encode(*m.Put)
			if err != nil {
				return err
			}
			e := Entry{
				Seq: seq, Op: OpPut, Table: table,
				Key: m.Put.Key, Fk: m.Put.Fk, ValueBytes: b,
			}
			if err := appendFrame(e); err != nil {
				return err
			}
		} else {
			e := Entry{Seq: seq, Op: OpDel, Table: table, Key: m.DelKey}
			if err := appendFrame(e); err != nil {
				return err
			}
		}
	}

	seq = s.nextSeq + 1
	commit := Entry{Seq: seq, Op: OpCommit, Table: table, ValueBytes: txBytes}
	if err := appendFrame(commit); err != nil {
		return err
	}

	tb := s.db.GetOrCreateTable(table)
	for _, m := range muts {
		if m.Put != nil {
			if err := tb.ApplyPut(*m.Put); err != nil {
				return err
			}
		} else {
			if err := tb.ApplyDelete(m.DelKey); err != nil {
				return err
			}
		}
	}

	if s.opts.Durability == Buffered {
		return s.wal.Flush()
	}
	return nil
}
