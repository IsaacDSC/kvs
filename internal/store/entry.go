package store

import "github.com/IsaacDSC/kvs/internal/wal"

// Op is the mutation kind stored in the WAL.
type Op = wal.Op

const (
	OpPut    = wal.OpPut
	OpDel    = wal.OpDel
	OpBegin  = wal.OpBegin
	OpCommit = wal.OpCommit
)

// Entry is one logical record in the WAL.
type Entry = wal.Entry

var (
	ErrCorruptRecord = wal.ErrCorruptRecord
	ErrTruncated     = wal.ErrTruncated
)

// TxIDFromValueBytes returns the txid stored in Begin/Commit ValueBytes.
func TxIDFromValueBytes(b []byte) (uint64, error) {
	return wal.TxIDFromValueBytes(b)
}

// EncodeTxID encodes a txid for Entry.ValueBytes on Begin/Commit.
func EncodeTxID(id uint64) []byte {
	return wal.EncodeTxID(id)
}
