package memdb

import (
	"context"
	"errors"
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
		item, err := tx.Get("k")
		if err != nil {
			return err
		}
		s := item.Value.(string)
		return tx.Set(Item{Key: "k", Fk: "f", Value: s + "!"})
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := tb.Get("k")
	if got.Value != "v!" {
		t.Fatalf("got %v", got.Value)
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

func TestNewSessionSerializesConcurrentSessions(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session1In := make(chan struct{})
	release1 := make(chan struct{})

	err1Ch := make(chan error, 1)
	go func() {
		err1Ch <- tb.NewSession(ctx, func(tx *Tx) error {
			close(session1In) // signal we acquired the session lock
			<-release1        // hold lock until released
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		t.Fatalf("session1 did not start: %v", ctx.Err())
	case <-session1In:
	}

	session2In := make(chan struct{})
	err2Ch := make(chan error, 1)
	go func() {
		err2Ch <- tb.NewSession(ctx, func(tx *Tx) error {
			close(session2In)
			return nil
		})
	}()

	// session2 must not enter before session1 releases.
	select {
	case <-session2In:
		t.Fatalf("session2 entered while session1 still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release1)

	select {
	case <-ctx.Done():
		t.Fatalf("sessions did not finish: %v", ctx.Err())
	case err := <-err1Ch:
		if err != nil {
			t.Fatalf("session1: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		t.Fatalf("session2 did not finish: %v", ctx.Err())
	case err := <-err2Ch:
		if err != nil {
			t.Fatalf("session2: %v", err)
		}
	}
}

func TestNewSessionMemoryOnlyCommit(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	ctx := context.Background()
	err := tb.NewSession(ctx, func(tx *Tx) error {
		return tx.Set(Item{Key: "k", Fk: "f", Value: "v"})
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tb.Get("k")
	if err != nil || got.Value.(string) != "v" {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestNewSessionRollbackOnError(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "k", Fk: "f", Value: "old"})
	ctx := context.Background()
	want := errors.New("fail")
	err := tb.NewSession(ctx, func(tx *Tx) error {
		if err := tx.Set(Item{Key: "k", Fk: "f", Value: "new"}); err != nil {
			return err
		}
		return want
	})
	if err == nil || !strings.Contains(err.Error(), "session error") {
		t.Fatalf("expected session error, got %v", err)
	}
	got, _ := tb.Get("k")
	if got.Value.(string) != "old" {
		t.Fatalf("value should rollback, got %v", got.Value)
	}
}

func TestNewSessionGetByFkStaging(t *testing.T) {
	tb := &Table{
		VirtualTable: VirtualTable{
			Data: make(map[string][]byte),
			Fk:   make(map[string][]string),
		},
		Session: make(map[int]VirtualTable),
	}
	_ = tb.Set(Item{Key: "a", Fk: "g", Value: "1"})
	ctx := context.Background()
	err := tb.NewSession(ctx, func(tx *Tx) error {
		if err := tx.Set(Item{Key: "b", Fk: "g", Value: "2"}); err != nil {
			return err
		}
		items, err := tx.GetByFk("g")
		if err != nil {
			return err
		}
		if len(items) != 2 {
			t.Fatalf("GetByFk want 2, got %d", len(items))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
