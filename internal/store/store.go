package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/old/memdb"
	"github.com/IsaacDSC/kvs/internal/wal"
)

var ErrEmptyTableName = errors.New("store: empty table name")

// Store provides a durable key-value store backed by a WAL under dir; mutations go through Table (memdb/db façade).
type Store struct {
	mu   sync.Mutex
	dir  string
	db   *memdb.DB
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
	database := memdb.NewDB()
	cpSeq, err := durable.LoadCheckpoint(dir, database)
	if err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, WalFileName)
	_ = RepairTruncatesTail(walPath)
	log, err := openWAL(walPath, opts)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, db: database, wal: log, opts: opts}
	replayer := wal.NewMemDBReplayer(database, cpSeq)
	maxSeq, err := log.Replay(replayer.Apply)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	s.LastReplayApplied = replayer.Applied()
	s.nextSeq = maxUint64(cpSeq, maxSeq)
	database.SetDurable(&storeDurable{s: s})
	return s, nil
}

func (s *Store) DB() *memdb.DB { return s.db }

// AppDB returns a façade over the in-memory database (forward-only; see internal/db).
func (s *Store) AppDB() *db.OldDb { return db.Wrap(s.db) }

// putLocked appends a Put to the WAL and updates memory. s.mu must be held.
func (s *Store) putLocked(table string, item item.Entity) error {
	if table == "" {
		return ErrEmptyTableName
	}
	b, err := code.Encode(item)
	if err != nil {
		return err
	}
	seq := s.nextSeq + 1
	e := Entry{Seq: seq, Op: OpPut, Table: table, Key: item.Key, Fk: item.SK, ValueBytes: b}
	if err := s.wal.Append(e); err != nil {
		return err
	}
	s.nextSeq = seq
	tb := s.db.GetOrCreateTable(table)
	return tb.ApplyPut(item)
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

// Checkpoint writes a snapshot including current nextSeq as LastSeq.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return durable.SaveCheckpoint(s.dir, s.db, s.nextSeq)
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

func validateTxMuts(muts []memdb.TxMutation) error {
	for i, m := range muts {
		if m.Put != nil && m.DelKey != "" {
			return fmt.Errorf("store: tx mutation %d: both put and delete", i)
		}
		if m.Put == nil && m.DelKey == "" {
			return fmt.Errorf("store: tx mutation %d: empty", i)
		}
	}
	return nil
}

// commitTransaction writes BEGIN → muts → COMMIT to the WAL, then applies muts in memory.
// It holds s.mu for the WAL/memory phase. On Buffered durability, flushes at the end.
func (s *Store) commitTransaction(table string, muts []memdb.TxMutation) error {
	if table == "" {
		return ErrEmptyTableName
	}
	if err := validateTxMuts(muts); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitTransactionLocked(table, muts)
}

// commitTransactionLocked is the body of commitTransaction; s.mu must be held.
func (s *Store) commitTransactionLocked(table string, muts []memdb.TxMutation) error {
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
				Key: m.Put.Key, Fk: m.Put.SK, ValueBytes: b,
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
