package wal

import (
	"errors"
	"fmt"

	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/old/memdb"
	"github.com/fxamacker/cbor/v2"
)

// MemDBReplayer applies WAL entries into memdb with transaction semantics.
type MemDBReplayer struct {
	db    *memdb.DB
	cpSeq uint64

	open      bool
	pending   []Entry
	pendTable string
	pendTxID  uint64

	applied int
}

// NewMemDBReplayer creates a replayer that skips entries up to checkpointSeq.
func NewMemDBReplayer(db *memdb.DB, checkpointSeq uint64) *MemDBReplayer {
	return &MemDBReplayer{
		db:    db,
		cpSeq: checkpointSeq,
	}
}

// Apply applies one WAL entry.
func (r *MemDBReplayer) Apply(e Entry) error {
	if e.Seq <= r.cpSeq {
		return nil
	}
	r.applied++

	switch e.Op {
	case OpBegin:
		txid, err := TxIDFromValueBytes(e.ValueBytes)
		if err != nil {
			return err
		}
		r.open = false
		r.pending = r.pending[:0]
		r.open = true
		r.pendTable = e.Table
		r.pendTxID = txid
		return nil

	case OpCommit:
		txid, err := TxIDFromValueBytes(e.ValueBytes)
		if err != nil {
			return err
		}
		if !r.open || e.Table != r.pendTable || txid != r.pendTxID {
			return errors.New("wal: orphan commit or tx mismatch")
		}
		for _, pe := range r.pending {
			if err := applyEntry(r.db, pe); err != nil {
				return err
			}
		}
		r.open = false
		r.pending = r.pending[:0]
		r.pendTable = ""
		r.pendTxID = 0
		return nil

	case OpSet, OpDel:
		if r.open && e.Table == r.pendTable {
			r.pending = append(r.pending, e)
			return nil
		}
		if r.open && e.Table != r.pendTable {
			return fmt.Errorf("wal: put/del table %q inside tx for %q", e.Table, r.pendTable)
		}
		return applyEntry(r.db, e)

	default:
		return fmt.Errorf("wal: unknown op %d", e.Op)
	}
}

// Applied returns how many entries were considered after checkpoint filtering.
func (r *MemDBReplayer) Applied() int {
	return r.applied
}

// decodePutItem restores a Put payload. New records are CBOR of item.Entity; older WAL
// records store only Value (CBOR of any), with Key/Fk taken from the frame.
func decodePutItem(e Entry) (item.Entity, error) {
	var i item.Entity
	if err := cbor.Unmarshal(e.ValueBytes, &i); err == nil && i.Key != "" {
		if i.Key != e.Key || i.SK != e.Fk {
			return item.Entity{}, errors.New("wal: key/fk mismatch with payload")
		}
		return i, nil
	}
	var v any
	if err := cbor.Unmarshal(e.ValueBytes, &v); err != nil {
		return item.Entity{}, err
	}
	return item.Entity{Key: e.Key, SK: e.Fk, Value: v}, nil
}

func applyEntry(database *memdb.DB, e Entry) error {
	t := database.GetOrCreateTable(e.Table)
	switch e.Op {
	case OpSet:
		it, err := decodePutItem(e)
		if err != nil {
			return err
		}
		return t.ApplyPut(it)
	case OpDel:
		return t.ApplyDelete(e.Key)
	default:
		return errors.New("wal: unknown op")
	}
}
