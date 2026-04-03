package store

import (
	"fmt"

	"github.com/IsaacDSC/kvs/internal/memdb"
)

// replayState applies WAL entries: immediate Put/Del, or buffered groups between Begin and Commit.
type replayState struct {
	db    *memdb.DB
	cpSeq uint64

	open      bool
	pending   []Entry
	pendTable string
	pendTxID  uint64

	applied int
}

func (rs *replayState) apply(e Entry) error {
	if e.Seq <= rs.cpSeq {
		return nil
	}
	rs.applied++

	switch e.Op {
	case OpBegin:
		txid, err := TxIDFromValueBytes(e.ValueBytes)
		if err != nil {
			return err
		}
		// New transaction: drop incomplete previous group.
		rs.open = false
		rs.pending = rs.pending[:0]
		rs.open = true
		rs.pendTable = e.Table
		rs.pendTxID = txid
		return nil

	case OpCommit:
		txid, err := TxIDFromValueBytes(e.ValueBytes)
		if err != nil {
			return err
		}
		if !rs.open || e.Table != rs.pendTable || txid != rs.pendTxID {
			return fmt.Errorf("store: orphan commit or tx mismatch")
		}
		for _, pe := range rs.pending {
			if err := applyEntry(rs.db, pe); err != nil {
				return err
			}
		}
		rs.open = false
		rs.pending = rs.pending[:0]
		rs.pendTable = ""
		rs.pendTxID = 0
		return nil

	case OpPut, OpDel:
		if rs.open && e.Table == rs.pendTable {
			rs.pending = append(rs.pending, e)
			return nil
		}
		if rs.open && e.Table != rs.pendTable {
			return fmt.Errorf("store: put/del table %q inside tx for %q", e.Table, rs.pendTable)
		}
		return applyEntry(rs.db, e)

	default:
		return fmt.Errorf("store: unknown wal op %d", e.Op)
	}
}
