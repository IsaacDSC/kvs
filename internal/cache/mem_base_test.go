package cache_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cache"
)

func TestCacheOnce_LRU(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		step  func(t *testing.T, c *cache.Cache[int])
	}{
		{
			name:  "eviction removes least recently inserted",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "a", 1)
				mustOnceMiss(t, c, "b", 2)
				mustOnceMiss(t, c, "c", 3)
				mustAbsent(t, c, "a")
				mustOnceHit(t, c, "b", 2)
				mustOnceHit(t, c, "c", 3)
			},
		},
		{
			name:  "hit promotes key so another insertion evicts different key",
			limit: 3,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "a", 1)
				mustOnceMiss(t, c, "b", 2)
				mustOnceMiss(t, c, "c", 3)
				mustOnceHit(t, c, "a", 1)
				mustOnceMiss(t, c, "d", 4)
				mustAbsent(t, c, "b")
				mustOnceHit(t, c, "a", 1)
				mustOnceHit(t, c, "c", 3)
				mustOnceHit(t, c, "d", 4)
			},
		},
		{
			name:  "update existing key keeps sibling entries",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "a", 1)
				mustOnceMiss(t, c, "b", 2)
				if err := c.SaveIfOk("a", 10, func() error { return nil }); err != nil {
					t.Fatal(err)
				}
				mustOnceHit(t, c, "a", 10)
				mustOnceHit(t, c, "b", 2)
				mustAbsent(t, c, "ghost")
			},
		},
		{
			name:  "limit zero does not evict",
			limit: 0,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "x", 1)
				mustOnceMiss(t, c, "y", 2)
				mustOnceMiss(t, c, "z", 3)
				mustOnceHit(t, c, "x", 1)
				mustOnceHit(t, c, "y", 2)
				mustOnceHit(t, c, "z", 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.New[int](tt.limit, 0)
			tt.step(t, c)
		})
	}
}

func TestCacheSaveIfOk_LRU(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		step  func(t *testing.T, c *cache.Cache[int])
	}{
		{
			name:  "fn error leaves cache unchanged",
			limit: 1,
			step: func(t *testing.T, c *cache.Cache[int]) {
				err := c.SaveIfOk("k", 1, func() error {
					return errors.New("fail")
				})
				if err == nil {
					t.Fatal("expected error")
				}
				mustAbsent(t, c, "k")
			},
		},
		{
			name:  "success stores and respects eviction",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				if err := c.SaveIfOk("a", 1, func() error { return nil }); err != nil {
					t.Fatal(err)
				}
				if err := c.SaveIfOk("b", 2, func() error { return nil }); err != nil {
					t.Fatal(err)
				}
				if err := c.SaveIfOk("c", 3, func() error { return nil }); err != nil {
					t.Fatal(err)
				}
				mustAbsent(t, c, "a")
				mustOnceHit(t, c, "b", 2)
				mustOnceHit(t, c, "c", 3)
			},
		},
		{
			name:  "update via SaveIfOk refreshes order",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				_ = c.SaveIfOk("a", 1, func() error { return nil })
				_ = c.SaveIfOk("b", 2, func() error { return nil })
				_ = c.SaveIfOk("a", 10, func() error { return nil })
				_ = c.SaveIfOk("c", 3, func() error { return nil })
				mustAbsent(t, c, "b")
				mustOnceHit(t, c, "a", 10)
				mustOnceHit(t, c, "c", 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.New[int](tt.limit, 0)
			tt.step(t, c)
		})
	}
}

func TestCacheDelIfOk(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		step  func(t *testing.T, c *cache.Cache[int])
	}{
		{
			name:  "fn success removes entry from cache",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "k", 1)
				if err := c.DelIfOk("k", func() error { return nil }); err != nil {
					t.Fatalf("DelIfOk: %v", err)
				}
				mustAbsent(t, c, "k")
			},
		},
		{
			name:  "fn error keeps entry in cache",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "k", 1)
				err := c.DelIfOk("k", func() error {
					return errors.New("delete failed")
				})
				if err == nil {
					t.Fatal("expected error")
				}
				mustOnceHit(t, c, "k", 1)
			},
		},
		{
			name:  "fn is invoked",
			limit: 1,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "k", 1)
				var ran bool
				_ = c.DelIfOk("k", func() error {
					ran = true
					return nil
				})
				if !ran {
					t.Fatal("expected fn to run")
				}
			},
		},
		{
			name:  "error on one key does not evict siblings",
			limit: 3,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "a", 1)
				mustOnceMiss(t, c, "b", 2)
				if err := c.DelIfOk("a", func() error { return errors.New("fail") }); err == nil {
					t.Fatal("expected error")
				}
				mustOnceHit(t, c, "a", 1)
				mustOnceHit(t, c, "b", 2)
			},
		},
		{
			name:  "fn error on absent key returns error",
			limit: 1,
			step: func(t *testing.T, c *cache.Cache[int]) {
				if err := c.DelIfOk("ghost", func() error {
					return errors.New("fail")
				}); err == nil {
					t.Fatal("expected error")
				}
				mustAbsent(t, c, "ghost")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.New[int](tt.limit, 0)
			tt.step(t, c)
		})
	}
}

func TestCacheDel(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		step  func(t *testing.T, c *cache.Cache[int])
	}{
		{
			name:  "evicts present key",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "k", 1)
				c.Del("k")
				mustAbsent(t, c, "k")
			},
		},
		{
			name:  "missing key is a no-op",
			limit: 2,
			step: func(t *testing.T, c *cache.Cache[int]) {
				mustOnceMiss(t, c, "a", 1)
				c.Del("ghost")
				mustOnceHit(t, c, "a", 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.New[int](tt.limit, 0)
			tt.step(t, c)
		})
	}
}

func TestCacheConcurrentOnceAndSaveIfOk(t *testing.T) {
	const (
		goroutines = 32
		perG       = 200
	)
	// No capacity limit: isolate mutex/LRU without eviction racing verification.
	c := cache.New[int](0, 0)

	var fnCalls atomic.Int64
	var failures atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("g%d-%d", id, i)

				_, err := c.Once(key, func() (int, error) {
					fnCalls.Add(1)
					return id*10000 + i, nil
				})
				if err != nil {
					failures.Add(1)
					return
				}

				val := id*1000 + i
				if err := c.SaveIfOk(key, val, func() error { return nil }); err != nil {
					failures.Add(1)
					return
				}
				got, err := c.Once(key, func() (int, error) {
					return 0, errors.New("unexpected miss")
				})
				if err != nil {
					failures.Add(1)
					return
				}
				if got != val {
					failures.Add(1)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("worker failures: %d", failures.Load())
	}
	wantCold := int64(goroutines * perG)
	if got := fnCalls.Load(); got != wantCold {
		t.Fatalf("expected %d cold loads (one per unique key), got %d", wantCold, got)
	}
}

func mustOnceMiss(tb testing.TB, c *cache.Cache[int], key string, val int) {
	tb.Helper()
	var ran bool
	got, err := c.Once(key, func() (int, error) {
		ran = true
		return val, nil
	})
	if err != nil {
		tb.Fatalf("Once(%q): %v", key, err)
	}
	if !ran {
		tb.Fatalf("Once(%q): expected cache miss", key)
	}
	if got != val {
		tb.Fatalf("Once(%q): got %v want %v", key, got, val)
	}
}

func mustOnceHit(tb testing.TB, c *cache.Cache[int], key string, want int) {
	tb.Helper()
	var ran bool
	got, err := c.Once(key, func() (int, error) {
		ran = true
		return 0, errors.New("fn must not run on cache hit")
	})
	if err != nil {
		tb.Fatalf("Once(%q): %v", key, err)
	}
	if ran {
		tb.Fatalf("Once(%q): expected cache hit", key)
	}
	if got != want {
		tb.Fatalf("Once(%q): got %v want %v", key, got, want)
	}
}

func mustAbsent(tb testing.TB, c *cache.Cache[int], key string) {
	tb.Helper()
	var ran bool
	_, err := c.Once(key, func() (int, error) {
		ran = true
		return 0, errors.New("probe absent")
	})
	if ran {
		if err == nil {
			tb.Fatalf("Once(%q): expected probe error", key)
		}
		return
	}
	tb.Fatalf("key %q still present in LRU", key)
}
