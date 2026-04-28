package memdb

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/IsaacDSC/kvs/internal/item"
)

func newTestTable() *Table {
	return &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
}

func TestOptimisticPut_ConcurrentStaleVersionRejected(t *testing.T) {
	tb := newTestTable()
	ctx := context.Background()

	// Seed item with a known version.
	if err := tb.Set(item.Entity{Key: "k", Fk: "f", Value: "base", Version: "v1"}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var fastErr error
	go func() {
		defer wg.Done()
		<-start
		// Fast writer updates from v1 -> v2.
		res := tb.OptimisticPut(ctx, item.Entity{Key: "k", Fk: "f", Value: "fast", Version: "v1"}, "v2")
		fastErr = res.Err()
	}()

	var slowRes OptimisticResult
	go func() {
		defer wg.Done()
		<-start
		// Slow writer uses a stale version (v1) after fast writer wins.
		// We force ordering by waiting until we observe v2 in the table.
		for {
			it, err := tb.Get("k")
			if err != nil {
				continue
			}
			if it.Version == "v2" {
				break
			}
		}
		slowRes = tb.OptimisticPut(ctx, item.Entity{Key: "k", Fk: "f", Value: "slow", Version: "v1"}, "v3")
	}()

	close(start)
	wg.Wait()

	if fastErr != nil {
		t.Fatalf("fast writer failed: %v", fastErr)
	}
	if !errors.Is(slowRes.Err(), ErrInvalidVersion) {
		t.Fatalf("slow writer: want ErrInvalidVersion, got %v", slowRes.Err())
	}

	last, err := slowRes.GetLastVersion()
	if err != nil {
		t.Fatalf("GetLastVersion: %v", err)
	}
	if last.Version != "v2" {
		t.Fatalf("GetLastVersion.Version=%q want %q", last.Version, "v2")
	}

	// Ensure the slow writer did not overwrite the fast write.
	final, err := tb.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if final.Version != "v2" || final.Value != "fast" {
		t.Fatalf("final=%+v want version=v2 value=fast", final)
	}
}

func TestOptimisticDelete_ConcurrentStaleVersionRejected(t *testing.T) {
	tb := newTestTable()
	ctx := context.Background()

	// Seed item with a known version.
	if err := tb.Set(item.Entity{Key: "k", Fk: "f", Value: "base", Version: "v1"}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var fastErr error
	go func() {
		defer wg.Done()
		<-start
		// Fast writer updates v1 -> v2, making v1 stale.
		res := tb.OptimisticPut(ctx, item.Entity{Key: "k", Fk: "f", Value: "fast", Version: "v1"}, "v2")
		fastErr = res.Err()
	}()

	var slowRes OptimisticResult
	go func() {
		defer wg.Done()
		<-start
		// Delete attempt with stale v1 after the update lands.
		for {
			it, err := tb.Get("k")
			if err != nil {
				continue
			}
			if it.Version == "v2" {
				break
			}
		}
		slowRes = tb.OptimisticDelete(ctx, item.Entity{Key: "k", Version: "v1"}, "v1")
	}()

	close(start)
	wg.Wait()

	if fastErr != nil {
		t.Fatalf("fast writer failed: %v", fastErr)
	}
	if !errors.Is(slowRes.Err(), ErrInvalidVersion) {
		t.Fatalf("stale delete: want ErrInvalidVersion, got %v", slowRes.Err())
	}

	last, err := slowRes.GetLastVersion()
	if err != nil {
		t.Fatalf("GetLastVersion: %v", err)
	}
	if last.Version != "v2" {
		t.Fatalf("GetLastVersion.Version=%q want %q", last.Version, "v2")
	}

	// Ensure the stale delete did not delete the key.
	final, err := tb.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if final.Version != "v2" {
		t.Fatalf("final.Version=%q want %q", final.Version, "v2")
	}
}
