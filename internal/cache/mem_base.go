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
type Cache struct {
	mu        sync.Mutex
	ll        *list.List
	items     map[string]*list.Element // key -> list element
	limitUnit int
	ttl       time.Duration
}

type kv struct {
	key       string
	value     any
	expiresAt time.Time
}

// New returns an empty LRU cache. limitUnit is max capacity (<= 0 = unlimited).
// ttl is the entry lifetime after last access or write (<= 0 = no TTL).
func New(limitUnit int, ttl time.Duration) *Cache {
	return newCache(limitUnit, ttl)
}

func newCache(limitUnit int, ttl time.Duration) *Cache {
	return &Cache{
		ll:        list.New(),
		items:     make(map[string]*list.Element),
		limitUnit: limitUnit,
		ttl:       ttl,
	}
}

// PurgeExpired deletes all expired entries. With no TTL configured, it is a no-op.
func (c *Cache) PurgeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ttl <= 0 {
		return
	}

	for e := c.ll.Front(); e != nil; {
		next := e.Next()
		ent := e.Value.(*kv)
		if c.expired(ent) {
			delete(c.items, ent.key)
			c.ll.Remove(e)
		}
		e = next
	}
}

// StartCleanupLoop runs PurgeExpired every interval until ctx is canceled.
// interval <= 0 defaults to one second.
func (c *Cache) StartCleanupLoop(ctx context.Context, interval time.Duration) {
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
func (c *Cache) SaveIfOk(key string, value any, fn func() error) error {
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
func (c *Cache) Once(key string, fn func() (any, error)) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maybeSweepExpiredTail()

	if elem, ok := c.items[key]; ok {
		ent := elem.Value.(*kv)
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
		return nil, err
	}

	c.set(key, v)
	return v, nil
}

func (c *Cache) expired(ent *kv) bool {
	if c.ttl <= 0 {
		return false
	}
	return !datetime.Now().Before(ent.expiresAt)
}

func (c *Cache) touch(ent *kv) {
	if c.ttl <= 0 {
		return
	}
	ent.expiresAt = datetime.Now().Add(c.ttl)
}

func (c *Cache) maybeSweepExpiredTail() {
	if c.ttl <= 0 {
		return
	}
	for {
		back := c.ll.Back()
		if back == nil {
			return
		}
		ent := back.Value.(*kv)
		if !c.expired(ent) {
			return
		}
		c.removeElement(back)
	}
}

func (c *Cache) removeElement(elem *list.Element) {
	ent := elem.Value.(*kv)
	delete(c.items, ent.key)
	c.ll.Remove(elem)
}

// set stores key as MRU; for a new key, drops expired tail entries then LRU if at capacity.
func (c *Cache) set(key string, value any) {
	if elem, ok := c.items[key]; ok {
		ent := elem.Value.(*kv)
		ent.value = value
		c.touch(ent)
		c.ll.MoveToFront(elem)
		return
	}

	c.maybeSweepExpiredTail()

	for c.limitUnit > 0 && c.ll.Len() >= c.limitUnit {
		c.evictLRU()
	}

	ent := &kv{key: key, value: value}
	c.touch(ent)
	c.items[key] = c.ll.PushFront(ent)
}

func (c *Cache) evictLRU() {
	back := c.ll.Back()
	if back == nil {
		return
	}
	k := back.Value.(*kv).key
	delete(c.items, k)
	c.ll.Remove(back)
}
