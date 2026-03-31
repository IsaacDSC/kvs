package db

import (
	"context"
	"fmt"
)

// Tx is only valid inside Table.NewSession while the table holds an exclusive lock.
// Use Tx methods from the callback — do not call Table.Add/Get/... on the same table
// concurrently from the same goroutine via Tx and Table (Table methods would deadlock).
type Tx struct {
	t *Table
}

func (x *Tx) Set(item Item) {
	x.t.addLocked(item)
}

func (x *Tx) Get(key string) any {
	return x.t.getLocked(key)
}

func (x *Tx) GetByFk(fk string) []Item {
	return x.t.getByFkLocked(fk)
}

func (x *Tx) Delete(key string) {
	x.t.deleteLocked(key)
}

// NewSession holds an exclusive lock for the duration of fn, blocking all other
// reads and writes on this table until fn returns.
func (t *Table) NewSession(ctx context.Context, fn func(tx *Tx) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	errChan := make(chan error)
	done := make(chan struct{})

	go func() {
		defer close(done)
		if err := fn(&Tx{t: t}); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("session timed out: %w", ctx.Err())
	case <-errChan:
		return fmt.Errorf("session rollback: %w", ctx.Err())
	case <-done:
		return nil
	}

}
