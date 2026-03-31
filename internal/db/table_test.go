package db

import (
	"errors"
	"testing"
)

func TestTableSetGet(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	if err := tb.Set(Item{Key: "k1", Fk: "f1", Value: "hello"}); err != nil {
		t.Fatal(err)
	}
	v, err := tb.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := v.(string); s != "hello" {
		t.Fatalf("got %q", s)
	}
	if _, err := tb.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get missing: %v", err)
	}
}

func TestTableGetByFk(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "a", Fk: "grp", Value: "1"})
	_ = tb.Set(Item{Key: "b", Fk: "grp", Value: "2"})
	items, err := tb.GetByFk("grp")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("len=%d", len(items))
	}
	if _, err := tb.GetByFk("none"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetByFk none: %v", err)
	}
}

func TestTableDelete(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "x", Fk: "g", Value: 42})
	tb.Delete("x")
	if _, err := tb.Get("x"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := tb.GetByFk("g"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("fk empty: %v", err)
	}
}

func TestTableExportMapsDeepCopy(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "k", Fk: "f", Value: "x"})
	d, fk := tb.ExportMaps()
	d["k"][0] = 'y'
	v, _ := tb.Get("k")
	if v.(string) != "x" {
		t.Fatalf("ExportMaps mutated table: %v", v)
	}
	if len(fk["f"]) != 1 {
		t.Fatalf("fk: %v", fk)
	}
}

func TestTableDuplicateFkKey(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	if err := tb.Set(Item{Key: "k", Fk: "f", Value: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := tb.Set(Item{Key: "k", Fk: "f", Value: "b"}); err != nil {
		t.Fatal(err)
	}
	items, _ := tb.GetByFk("f")
	if len(items) != 1 {
		t.Fatalf("len=%d want 1", len(items))
	}
}
