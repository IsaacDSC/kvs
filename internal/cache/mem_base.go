package cache

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/IsaacDSC/kvs/pkg/datetime"
)

// Cache is an in-memory LRU cache guarded by a mutex.
// limitUnit is the maximum number of entries; values <= 0 disable capacity eviction.
// ttl <= 0 disables time-based expiration.
type Cache[T any] struct {
	mu        sync.Mutex
	ll        *list.List
	items     map[string]*list.Element // key -> list element
	limitUnit int
	ttl       time.Duration
}

type kv[T any] struct {
	key       string
	value     T
	expiresAt time.Time
}

// New returns an empty LRU cache. limitUnit is max capacity (<= 0 = unlimited).
// ttl is the entry lifetime after last access or write (<= 0 = no TTL).
func New[T any](limitUnit int, ttl time.Duration) *Cache[T] {
	return &Cache[T]{
		ll:        list.New(),
		items:     make(map[string]*list.Element),
		limitUnit: limitUnit,
		ttl:       ttl,
	}
}

// PurgeExpired deletes all expired entries. With no TTL configured, it is a no-op.
func (c *Cache[T]) PurgeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ttl <= 0 {
		return
	}

	for e := c.ll.Front(); e != nil; {
		next := e.Next()
		ent := e.Value.(*kv[T])
		if c.expired(ent) {
			delete(c.items, ent.key)
			c.ll.Remove(e)
		}
		e = next
	}
}

// StartCleanupLoop runs PurgeExpired every interval until ctx is canceled.
// interval <= 0 defaults to one second.
func (c *Cache[T]) StartCleanupLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.PurgeExpired()
			}
		}
	}()
}

// SaveIfOk locks the cache, runs fn, and if fn succeeds stores value for key as MRU.
func (c *Cache[T]) SaveIfOk(key string, value T, fn func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maybeSweepExpiredTail()

	if err := fn(); err != nil {
		return err
	}

	c.set(key, value)
	return nil
}

// Once returns the cached value for key and promotes it to MRU. If missing,
// runs fn and stores the result (with eviction when applicable).
func (c *Cache[T]) Once(key string, fn func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maybeSweepExpiredTail()

	if elem, ok := c.items[key]; ok {
		ent := elem.Value.(*kv[T])
		if c.expired(ent) {
			c.removeElement(elem)
		} else {
			c.touch(ent)
			c.ll.MoveToFront(elem)
			return ent.value, nil
		}
	}

	v, err := fn()
	if err != nil {
		var zero T
		return zero, err
	}

	c.set(key, v)
	return v, nil
}

// Del evicts key from the cache if present. A missing key is a no-op: cache
// invalidation is best-effort and never reports an error to the caller.
func (c *Cache[T]) Del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// DelIfOk runs fn while holding the lock; on success it evicts key from the cache.
// On failure it returns the error and leaves the cache unchanged.
func (c *Cache[T]) DelIfOk(key string, fn func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maybeSweepExpiredTail()

	if err := fn(); err != nil {
		return err
	}
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
	return nil
}

func (c *Cache[T]) expired(ent *kv[T]) bool {
	if c.ttl <= 0 {
		return false
	}
	return !datetime.Now().Before(ent.expiresAt)
}

func (c *Cache[T]) touch(ent *kv[T]) {
	if c.ttl <= 0 {
		return
	}
	ent.expiresAt = datetime.Now().Add(c.ttl)
}

func (c *Cache[T]) maybeSweepExpiredTail() {
	if c.ttl <= 0 {
		return
	}
	for {
		back := c.ll.Back()
		if back == nil {
			return
		}
		ent := back.Value.(*kv[T])
		if !c.expired(ent) {
			return
		}
		c.removeElement(back)
	}
}

func (c *Cache[T]) removeElement(elem *list.Element) {
	ent := elem.Value.(*kv[T])
	delete(c.items, ent.key)
	c.ll.Remove(elem)
}

// set stores key as MRU; for a new key, drops expired tail entries then LRU if at capacity.
func (c *Cache[T]) set(key string, value T) {
	if elem, ok := c.items[key]; ok {
		ent := elem.Value.(*kv[T])
		ent.value = value
		c.touch(ent)
		c.ll.MoveToFront(elem)
		return
	}

	c.maybeSweepExpiredTail()

	for c.limitUnit > 0 && c.ll.Len() >= c.limitUnit {
		c.evictLRU()
	}

	ent := &kv[T]{key: key, value: value}
	c.touch(ent)
	c.items[key] = c.ll.PushFront(ent)
}

func (c *Cache[T]) evictLRU() {
	back := c.ll.Back()
	if back == nil {
		return
	}
	k := back.Value.(*kv[T]).key
	delete(c.items, k)
	c.ll.Remove(back)
}
