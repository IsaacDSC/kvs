package fsdb

import (
	"context"
	"errors"
	"testing"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

func TestDb_Set_returnsErrTableNotFoundWhenTableDirMissing(t *testing.T) {
	t.Parallel()
	d := NewDb(t.TempDir())
	err := d.Set(context.Background(), "never_created", item.Entity{
		Key:   "fordel",
		SK:    "familia",
		Value: map[string]any{"fordel": "fordelvalue"},
	})
	if !errors.Is(err, db.ErrTableNotFound) {
		t.Fatalf("Set: got %v want ErrTableNotFound", err)
	}
}
