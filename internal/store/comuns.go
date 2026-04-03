package store

import (
	"errors"
	"fmt"

	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/fxamacker/cbor/v2"
)

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// decodePutItem restores a Put payload. New records are CBOR of memdb.Item; older WAL
// records store only Value (CBOR of any), with Key/Fk taken from the frame.
func decodePutItem(e Entry) (memdb.Item, error) {
	var item memdb.Item
	if err := cbor.Unmarshal(e.ValueBytes, &item); err == nil && item.Key != "" {
		if item.Key != e.Key || item.Fk != e.Fk {
			return memdb.Item{}, fmt.Errorf("store: wal key/fk does not match item")
		}
		return item, nil
	}
	var v any
	if err := cbor.Unmarshal(e.ValueBytes, &v); err != nil {
		return memdb.Item{}, err
	}
	return memdb.Item{Key: e.Key, Fk: e.Fk, Value: v}, nil
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
