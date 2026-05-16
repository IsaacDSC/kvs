package memdb

import (
	"context"
	"fmt"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/item"
)

// ExportCheckpointBlobs returns a deep copy of each table's encoded rows (primary key -> CBOR blob).
func (d *DB) ExportCheckpointBlobs() map[string]map[string][]byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]map[string][]byte, len(d.tables))
	for name, t := range d.tables {
		out[name] = t.cloneDataMapLocked()
	}
	return out
}

func (t *table) cloneDataMapLocked() map[string][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string][]byte, len(t.data))
	for k, v := range t.data {
		vv := make([]byte, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

// ReplaceWithCheckpointBlobs replaces all tables from checkpoint blobs and rebuilds indexes via Set.
func (d *DB) ReplaceWithCheckpointBlobs(ctx context.Context, tables map[string]map[string][]byte) error {
	d.mu.Lock()
	d.tables = make(map[string]*table)
	d.mu.Unlock()

	for name, blobs := range tables {
		if err := d.CreateTable(name); err != nil {
			return err
		}
		for _, b := range blobs {
			var e item.Entity
			if err := code.Decode(b, &e); err != nil {
				return fmt.Errorf("memdb: restore checkpoint table %q: %w", name, err)
			}
			if err := d.Set(ctx, name, e); err != nil {
				return err
			}
		}
	}
	return nil
}
