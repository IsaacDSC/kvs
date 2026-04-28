package store

import (
	"errors"
	"fmt"

	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/fxamacker/cbor/v2"
)

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// decodePutItem restores a Put payload. New records are CBOR of item.Entity; older WAL
// records store only Value (CBOR of any), with Key/Fk taken from the frame.
func decodePutItem(e Entry) (item.Entity, error) {
	var i item.Entity
	if err := cbor.Unmarshal(e.ValueBytes, &i); err == nil && i.Key != "" {
		if i.Key != e.Key || i.Fk != e.Fk {
			return item.Entity{}, fmt.Errorf("store: wal key/fk does not match item")
		}
		return i, nil
	}
	var v any
	if err := cbor.Unmarshal(e.ValueBytes, &v); err != nil {
		return item.Entity{}, err
	}
	return item.Entity{Key: e.Key, Fk: e.Fk, Value: v}, nil
}

func applyEntry(database *memdb.DB, e Entry) error {
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
