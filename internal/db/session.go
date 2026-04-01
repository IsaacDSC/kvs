package db

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/IsaacDSC/kvs/internal/code"
)

// Tx buffers mutations for Table.NewSession until commit (WAL BEGIN…COMMIT) or rollback.
// Reads resolve by scanning ordered from newest to oldest: the last Put or Del for a key wins; if none, the base table is used.
type Tx struct {
	t       *Table
	ordered []TxMutation
}

func newTx(t *Table) *Tx {
	return &Tx{t: t}
}

func ptrItem(it Item) *Item {
	c := it
	return &c
}

// Set records a put to be applied on successful commit.
func (x *Tx) Set(item Item) error {
	if _, err := code.Encode(item); err != nil {
		return errors.Join(ErrEncodeValue, err)
	}
	x.ordered = append(x.ordered, TxMutation{Put: ptrItem(item)})
	return nil
}

// Delete records a delete to be applied on successful commit.
func (x *Tx) Delete(key string) error {
	x.ordered = append(x.ordered, TxMutation{DelKey: key})
	return nil
}

// Get returns the value visible inside this transaction (replay of ordered on top of the base table).
func (x *Tx) Get(key string) (Item, error) {
	for i := len(x.ordered) - 1; i >= 0; i-- {
		m := x.ordered[i]
		if m.Put != nil && m.Put.Key == key {
			return *m.Put, nil
		}
		if m.DelKey == key {
			return Item{}, ErrKeyNotFound
		}
	}
	return x.t.Get(key)
}

// GetByFk returns items for fk visible inside this transaction.
func (x *Tx) GetByFk(fk string) ([]Item, error) {
	x.t.mu.RLock()
	baseKeys := x.t.Fk[fk]
	x.t.mu.RUnlock()

	candidates := make(map[string]struct{}, len(baseKeys)+8)
	for _, k := range baseKeys {
		candidates[k] = struct{}{}
	}
	for _, m := range x.ordered {
		if m.Put != nil && m.Put.Fk == fk {
			candidates[m.Put.Key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(candidates))
	for k := range candidates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []Item
	for _, k := range keys {
		it, err := x.Get(k)
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if it.Fk == fk {
			out = append(out, it)
		}
	}
	if len(out) == 0 {
		return nil, ErrKeyNotFound
	}
	return out, nil
}

func (t *Table) commitTransaction(tx *Tx) error {
	if len(tx.ordered) == 0 {
		return nil
	}
	muts := append([]TxMutation(nil), tx.ordered...)

	if t.durable != nil {
		tc, ok := t.durable.(TransactionCommitter)
		if !ok {
			return fmt.Errorf("db: transactional session requires CommitTransaction support")
		}
		if t.name == "" {
			return fmt.Errorf("db: table has no name for durable commit")
		}
		return tc.CommitTransaction(t.name, muts)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range muts {
		if m.Put != nil {
			if err := t.addLocked(*m.Put); err != nil {
				return err
			}
		} else {
			t.deleteLocked(m.DelKey)
		}
	}
	return nil
}

// NewSession runs fn under an exclusive per-table session lock. Mutations on *Tx are buffered
// until fn returns successfully; then they commit via WAL BEGIN…COMMIT (when the durable layer
// implements TransactionCommitter) or apply in memory. The lock is held until commit/rollback completes.
// On fn error or ctx cancellation, nothing is persisted.
func (t *Table) NewSession(ctx context.Context, fn func(tx *Tx) error) error {
	t.initSessionLock()

	select {
	case <-ctx.Done():
		return fmt.Errorf("session timed out: %w", ctx.Err())
	case <-t.sessionSem:
	}
	defer func() { t.sessionSem <- struct{}{} }()

	tx := newTx(t)
	if err := fn(tx); err != nil {
		return fmt.Errorf("session error: %w", err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("session timed out: %w", ctx.Err())
	}
	if err := t.commitTransaction(tx); err != nil {
		return fmt.Errorf("transaction commit: %w", err)
	}
	return nil
}
