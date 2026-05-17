package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/kvs/pkg/datetime"
)

// testClock is a thread-safe fake clock for TTL tests without data races.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock(t time.Time) *testClock {
	return &testClock{t: t}
}

func (tc *testClock) Now() time.Time {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.t
}

func (tc *testClock) Set(t time.Time) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.t = t
}

// patchNow makes datetime.Now delegate to tc.Now until undo runs.
func patchNow(tc *testClock) (undo func()) {
	prev := datetime.Now
	datetime.Now = tc.Now
	return func() { datetime.Now = prev }
}

func TestTTL_OnceMissAfterExpiry(t *testing.T) {
	base := time.Unix(1700000000, 0)
	tc := newTestClock(base)
	undo := patchNow(tc)
	defer undo()

	c := New[int](10, time.Minute)

	if _, err := c.Once("k", func() (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}

	tc.Set(base.Add(time.Minute))
	var ran bool
	v, err := c.Once("k", func() (int, error) {
		ran = true
		return 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ran || v != 2 {
		t.Fatalf("expected TTL recomputation; ran=%v v=%v", ran, v)
	}
}

func TestTTL_HitExtendsDeadline(t *testing.T) {
	base := time.Unix(1700000000, 0)
	tc := newTestClock(base)
	undo := patchNow(tc)
	defer undo()

	c := New[string](10, time.Minute)

	if _, err := c.Once("k", func() (string, error) { return "x", nil }); err != nil {
		t.Fatal(err)
	}

	tc.Set(base.Add(59 * time.Second))
	if v, err := c.Once("k", func() (string, error) {
		return "", errors.New("fn must not run on cache hit")
	}); err != nil {
		t.Fatal(err)
	} else if v != "x" {
		t.Fatalf("got %v", v)
	}

	tc.Set(base.Add(2 * time.Minute))
	var ran bool
	if _, err := c.Once("k", func() (string, error) {
		ran = true
		return "y", nil
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("expected miss after sliding TTL window expired")
	}
}

func TestTTL_PurgeExpired(t *testing.T) {
	base := time.Unix(1700000000, 0)
	tc := newTestClock(base)
	undo := patchNow(tc)
	defer undo()

	c := New[int](0, time.Minute)

	_, _ = c.Once("a", func() (int, error) { return 1, nil })
	_, _ = c.Once("b", func() (int, error) { return 2, nil })

	tc.Set(base.Add(time.Hour))
	c.PurgeExpired()

	tc.Set(base.Add(time.Hour))
	var misses int
	for _, key := range []string{"a", "b"} {
		_, _ = c.Once(key, func() (int, error) {
			misses++
			return 0, nil
		})
	}
	if misses != 2 {
		t.Fatalf("expected 2 misses after purge, got %d", misses)
	}
}

func TestTTL_EvictionFreesSlotWhenTailExpired(t *testing.T) {
	base := time.Unix(1700000000, 0)
	tc := newTestClock(base)
	undo := patchNow(tc)
	defer undo()

	c := New[int](2, time.Minute)

	_, _ = c.Once("a", func() (int, error) { return 1, nil })
	_, _ = c.Once("b", func() (int, error) { return 2, nil })

	tc.Set(base.Add(time.Hour))
	if err := c.SaveIfOk("c", 3, func() error { return nil }); err != nil {
		t.Fatal(err)
	}

	var sawA, sawC int
	_, _ = c.Once("a", func() (int, error) {
		sawA++
		return 0, nil
	})
	_, _ = c.Once("c", func() (int, error) {
		sawC++
		return 0, nil
	})
	if sawA != 1 || sawC != 0 {
		t.Fatalf("expected a evicted (miss=%d); c cache hit (miss=%d)", sawA, sawC)
	}
}

func TestTTL_StartCleanupLoop(t *testing.T) {
	base := time.Unix(1700000000, 0)
	tc := newTestClock(base)
	undo := patchNow(tc)
	defer undo()

	c := New[int](0, time.Minute)
	_, _ = c.Once("z", func() (int, error) { return 1, nil })

	ctx, cancel := context.WithCancel(context.Background())
	c.StartCleanupLoop(ctx, 5*time.Millisecond)

	tc.Set(base.Add(time.Hour))
	time.Sleep(25 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	var miss bool
	_, _ = c.Once("z", func() (int, error) {
		miss = true
		return 2, nil
	})
	if !miss {
		t.Fatal("cleanup loop should remove expired z")
	}
}
