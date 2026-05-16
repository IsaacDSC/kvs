package fsdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

var _ db.CheckpointBlobHydrator = (*Db)(nil)

// ReplaceWithCheckpointBlobs rebuilds disk tables from WAL checkpoint blobs (primary key → CBOR entity).
func (d *Db) ReplaceWithCheckpointBlobs(ctx context.Context, tables map[string]map[string][]byte) error {
	for name := range tables {
		d.tbmu.Lock(name)
		path := filepath.Join(d.defaultDir, name)
		if err := os.RemoveAll(path); err != nil {
			d.tbmu.Unlock(name)
			return fmt.Errorf("fsdb: wipe checkpoint table %q: %w", name, err)
		}
		d.tbmu.Unlock(name)
	}

	for name, blobs := range tables {
		if err := d.CreateTable(name); err != nil {
			return fmt.Errorf("fsdb: recreate table %q: %w", name, err)
		}
		for _, b := range blobs {
			var e item.Entity
			if err := code.Decode(b, &e); err != nil {
				return fmt.Errorf("fsdb: decode checkpoint row in table %q: %w", name, err)
			}
			if err := d.Set(ctx, name, e); err != nil {
				return fmt.Errorf("fsdb: apply checkpoint row in table %q: %w", name, err)
			}
		}
	}
	return nil
}
