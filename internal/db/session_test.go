package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewSessionSuccess(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "k", Fk: "f", Value: "v"})

	ctx := context.Background()
	err := tb.NewSession(ctx, func(tx *Tx) error {
		v, err := tx.Get("k")
		if err != nil {
			return err
		}
		s := v.(string)
		return tx.Set(Item{Key: "k", Fk: "f", Value: s + "!"})
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := tb.Get("k")
	if got != "v!" {
		t.Fatalf("got %v", got)
	}
}

func TestNewSessionTimeout(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := tb.NewSession(ctx, func(tx *Tx) error {
		<-ctx.Done()
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "session timed out") {
		t.Fatalf("want timeout error, got %v", err)
	}
}

func TestNewSessionCallbackError(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	ctx := context.Background()
	want := context.Canceled
	err := tb.NewSession(ctx, func(tx *Tx) error {
		return want
	})
	if err == nil || !strings.Contains(err.Error(), "session error") {
		t.Fatalf("got %v", err)
	}
}
