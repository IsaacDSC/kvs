package fsdb

import (
	"context"
	"errors"
	"testing"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
)

// BulkDel removes every existing key of the batch, ignores missing and duplicate
// keys, and consolidates each SK index once — removing emptied index files.
func TestDb_BulkDel_removesKeysAndConsolidatesSk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := NewDb(t.TempDir())
	if err := d.CreateTable("t"); err != nil {
		t.Fatal(err)
	}

	seed := []item.Entity{
		{Key: "k1", SK: "s", Value: map[string]any{"v": 1}},
		{Key: "k2", SK: "s", Value: map[string]any{"v": 2}},
		{Key: "k3", SK: "x", Value: map[string]any{"v": 3}},
		{Key: "k4", Value: map[string]any{"v": 4}},
	}
	if err := d.BulkSet(ctx, "t", seed); err != nil {
		t.Fatal(err)
	}

	// Duplicate k3 and a missing key must not fail the batch.
	if err := d.BulkDel(ctx, "t", []string{"k1", "k3", "k3", "missing", "k4"}); err != nil {
		t.Fatalf("BulkDel: %v", err)
	}

	for _, k := range []string{"k1", "k3", "k4"} {
		if _, err := d.Get(ctx, "t", k); !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("Get %s: want ErrNotFound, got %v", k, err)
		}
	}

	// SK "s" still indexes the surviving k2.
	got, err := d.GetBySk(ctx, "t", "s")
	if err != nil {
		t.Fatalf("GetBySk s: %v", err)
	}
	if len(got) != 1 || got[0].Key != "k2" {
		t.Fatalf("GetBySk s = %#v, want only k2", got)
	}

	// SK "x" lost its last key: the index file must be gone.
	if _, err := d.GetBySk(ctx, "t", "x"); !errors.Is(err, db.ErrNotFoundSk) {
		t.Fatalf("GetBySk x: want ErrNotFoundSk, got %v", err)
	}
}

// BulkDel on an entirely missing batch is a no-op, not an error (idempotent retry).
func TestDb_BulkDel_missingKeysAreIgnored(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := NewDb(t.TempDir())
	if err := d.CreateTable("t"); err != nil {
		t.Fatal(err)
	}
	if err := d.BulkDel(ctx, "t", []string{"a", "b"}); err != nil {
		t.Fatalf("BulkDel on empty table: %v", err)
	}
}

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
