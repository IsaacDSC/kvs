package db

import (
	"context"
	"fmt"
)

// Tx is only valid inside Table.NewSession for the duration of fn.
type Tx struct {
	t *Table
}

func (x *Tx) Set(item Item) error {
	return x.t.Set(item)
}

func (x *Tx) Get(key string) (Item, error) {
	return x.t.Get(key)
}

func (x *Tx) GetByFk(fk string) ([]Item, error) {
	return x.t.GetByFk(fk)
}

func (x *Tx) Delete(key string) error {
	return x.t.Delete(key)
}

// NewSession runs fn under an exclusive per-table session lock so that callers can
// execute multi-step read/modify/write flows without interleaving other sessions.
//
// This lock is distinct from the table RWMutex: Tx.Set/Tx.Delete still call
// Table.Set/Table.Delete (which take the table lock and may append to the WAL).
func (t *Table) NewSession(ctx context.Context, fn func(tx *Tx) error) error {
	t.initSessionLock()

	select {
	case <-ctx.Done():
		return fmt.Errorf("session timed out: %w", ctx.Err())
	case <-t.sessionSem:
		// Acquired session lock.
	}
	defer func() { t.sessionSem <- struct{}{} }()

	if err := fn(&Tx{t: t}); err != nil {
		return fmt.Errorf("session error: %w", err)
	}

	if ctx.Err() != nil {
		return fmt.Errorf("session timed out: %w", ctx.Err())
	}
	return nil
}
