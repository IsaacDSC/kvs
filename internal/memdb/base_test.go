package memdb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
)

func TestDBMethods(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		opts        memdb.Options
		createTable bool
		steps       []dbStep
	}{
		{
			name:        "create table set and get",
			createTable: true,
			steps: []dbStep{
				set("a", "", "1"),
				expectGet("a", "1"),
			},
		},
		{
			name:        "get missing key",
			createTable: true,
			steps: []dbStep{
				expectGetErr("missing", db.ErrNotFound),
			},
		},
		{
			name: "missing table errors",
			steps: []dbStep{
				expectSetErr("a", "", "1", db.ErrTableNotFound),
				expectGetErr("a", db.ErrTableNotFound),
				expectGetBySkErr("sk", db.ErrTableNotFound),
				expectDelErr("a", db.ErrTableNotFound),
			},
		},
		{
			name:        "delete removes key and secondary key entry",
			opts:        memdb.Options{MaxEntriesPerTable: 2},
			createTable: true,
			steps: []dbStep{
				set("a", "sk", "1"),
				set("b", "sk", "2"),
				del("a"),
				expectGetErr("a", db.ErrNotFound),
				expectGetBySk("sk", "b"),
				set("c", "", "3"),
				expectGet("b", "2"),
				expectGet("c", "3"),
			},
		},
		{
			name:        "secondary key lookup returns indexed keys",
			createTable: true,
			steps: []dbStep{
				set("k1", "sk", "v1"),
				set("k2", "sk", "v2"),
				set("other", "other-sk", "v3"),
				expectGetBySk("sk", "k1", "k2"),
			},
		},
		{
			name:        "set moves key between secondary keys",
			opts:        memdb.Options{MaxEntriesPerTable: 10},
			createTable: true,
			steps: []dbStep{
				set("k1", "s1", "a"),
				set("k1", "s2", "b"),
				expectGetBySkErr("s1", errAny),
				expectGetBySk("s2", "k1"),
				expectGet("k1", "b"),
			},
		},
		{
			name:        "no cap does not evict",
			createTable: true,
			steps: append(
				rangeSetSteps(50),
				rangeGetSteps(50)...,
			),
		},
		{
			name:        "cap evicts oldest entry by LRU",
			opts:        memdb.Options{MaxEntriesPerTable: 2},
			createTable: true,
			steps: []dbStep{
				set("a", "", "1"),
				set("b", "", "2"),
				set("c", "", "3"),
				expectGetErr("a", db.ErrNotFound),
				expectGet("b", "2"),
				expectGet("c", "3"),
			},
		},
		{
			name:        "get refreshes LRU",
			opts:        memdb.Options{MaxEntriesPerTable: 2},
			createTable: true,
			steps: []dbStep{
				set("a", "", "1"),
				set("b", "", "2"),
				expectGet("a", "1"),
				set("c", "", "3"),
				expectGetErr("b", db.ErrNotFound),
				expectGet("a", "1"),
				expectGet("c", "3"),
			},
		},
		{
			name:        "eviction keeps secondary key index consistent",
			opts:        memdb.Options{MaxEntriesPerTable: 2},
			createTable: true,
			steps: []dbStep{
				set("k1", "sk", "v1"),
				set("k2", "sk", "v2"),
				set("k3", "", "v3"),
				expectGetErr("k1", db.ErrNotFound),
				expectGetBySk("sk", "k2"),
			},
		},
		{
			name:        "get by secondary key refreshes returned keys",
			opts:        memdb.Options{MaxEntriesPerTable: 3},
			createTable: true,
			steps: []dbStep{
				set("k1", "sk", "v1"),
				set("k2", "sk", "v2"),
				set("x", "", "vx"),
				expectGetBySk("sk", "k1", "k2"),
				set("y", "", "vy"),
				expectGetErr("x", db.ErrNotFound),
				expectGetBySk("sk", "k1", "k2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := memdb.NewDB(tt.opts)
			if tt.createTable {
				if err := d.CreateTable("t"); err != nil {
					t.Fatal(err)
				}
			}
			for _, step := range tt.steps {
				t.Run(step.name, func(t *testing.T) {
					step.run(t, ctx, d)
				})
			}
		})
	}
}

func TestCreateTableIsIdempotent(t *testing.T) {
	ctx := context.Background()
	d := memdb.NewDB(memdb.Options{})

	if err := d.CreateTable("t"); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "t", item.Entity{Key: "a", Value: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := d.CreateTable("t"); err != nil {
		t.Fatal(err)
	}

	entity, err := d.Get(ctx, "t", "a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entity.Value != "1" {
		t.Fatalf("want value %q, got %#v", "1", entity.Value)
	}
}

var errAny = errors.New("any error")

type dbStep struct {
	name string
	run  func(t *testing.T, ctx context.Context, d *memdb.DB)
}

func set(key, sk string, val any) dbStep {
	return dbStep{
		name: fmt.Sprintf("Set(%s)", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			if err := d.Set(ctx, "t", item.Entity{Key: key, SK: sk, Value: val}); err != nil {
				t.Fatalf("Set: %v", err)
			}
		},
	}
}

func expectSetErr(key, sk string, val any, want error) dbStep {
	return dbStep{
		name: fmt.Sprintf("Set(%s) error", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			err := d.Set(ctx, "t", item.Entity{Key: key, SK: sk, Value: val})
			mustErrorIs(t, err, want)
		},
	}
}

func del(key string) dbStep {
	return dbStep{
		name: fmt.Sprintf("Del(%s)", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			if err := d.Del(ctx, "t", key); err != nil {
				t.Fatalf("Del %q: %v", key, err)
			}
		},
	}
}

func expectDelErr(key string, want error) dbStep {
	return dbStep{
		name: fmt.Sprintf("Del(%s) error", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			err := d.Del(ctx, "t", key)
			mustErrorIs(t, err, want)
		},
	}
}

func expectGet(key string, want any) dbStep {
	return dbStep{
		name: fmt.Sprintf("Get(%s)", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			entity, err := d.Get(ctx, "t", key)
			if err != nil {
				t.Fatalf("Get %q: %v", key, err)
			}
			if entity.Value != want {
				t.Fatalf("Get %q: want value %#v, got %#v", key, want, entity.Value)
			}
		},
	}
}

func expectGetErr(key string, want error) dbStep {
	return dbStep{
		name: fmt.Sprintf("Get(%s) error", key),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			_, err := d.Get(ctx, "t", key)
			mustErrorIs(t, err, want)
		},
	}
}

func expectGetBySk(sk string, wantKeys ...string) dbStep {
	return dbStep{
		name: fmt.Sprintf("GetBySk(%s)", sk),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			items, err := d.GetBySk(ctx, "t", sk)
			if err != nil {
				t.Fatalf("GetBySk %q: %v", sk, err)
			}
			mustHaveKeys(t, items, wantKeys...)
		},
	}
}

func expectGetBySkErr(sk string, want error) dbStep {
	return dbStep{
		name: fmt.Sprintf("GetBySk(%s) error", sk),
		run: func(t *testing.T, ctx context.Context, d *memdb.DB) {
			t.Helper()
			_, err := d.GetBySk(ctx, "t", sk)
			mustErrorIs(t, err, want)
		},
	}
}

func rangeSetSteps(n int) []dbStep {
	steps := make([]dbStep, 0, n)
	for i := range n {
		key := string(rune('a' + i))
		steps = append(steps, set(key, "", key))
	}
	return steps
}

func rangeGetSteps(n int) []dbStep {
	steps := make([]dbStep, 0, n)
	for i := range n {
		key := string(rune('a' + i))
		steps = append(steps, expectGet(key, key))
	}
	return steps
}

func mustErrorIs(t *testing.T, err, want error) {
	t.Helper()
	if want == errAny {
		if err == nil {
			t.Fatal("want error, got nil")
		}
		return
	}
	if !errors.Is(err, want) {
		t.Fatalf("want error %v, got %v", want, err)
	}
}

func mustHaveKeys(t *testing.T, items []item.Entity, want ...string) {
	t.Helper()
	got := make(map[string]struct{}, len(items))
	for _, item := range items {
		got[item.Key] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("want keys %v, got items %#v", want, items)
	}
	for _, key := range want {
		if _, ok := got[key]; !ok {
			t.Fatalf("want keys %v, got items %#v", want, items)
		}
	}
}
