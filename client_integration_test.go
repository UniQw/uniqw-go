package uniqw

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	ikeys "github.com/UniQw/uniqw-go/internal/keys"
	mrd "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newRedisClient spins up a miniredis instance and returns a connected client and a cleanup.
func newRedisClient(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	s := mrd.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	cleanup := func() {
		_ = rdb.Close()
		s.Close()
	}
	return rdb, cleanup
}

func TestIntegration_Enqueue_PendingAndDelayedAndDuplicate(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()

	q := fmt.Sprintf("q-%d", time.Now().UnixNano())

	// pending
	if err := c.Enqueue(ctx, q, "email.send", map[string]any{"x": 1}); err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}
	// delayed
	if err := c.Enqueue(ctx, q, "email.send", map[string]any{"x": 2}, Delay(1*time.Hour)); err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}

	// list pending
	ps, err := c.ListTasks(ctx, q, StatePending, nil)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(ps) != 1 {
		t.Fatalf("pending len=%d", len(ps))
	}

	ds, err := c.ListTasks(ctx, q, StateDelayed, nil)
	if err != nil {
		t.Fatalf("list delayed: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("delayed len=%d", len(ds))
	}

	// duplicate id
	id := "dup-1"
	if err := c.Enqueue(ctx, q, "email.send", map[string]any{"x": 3}, TaskID(id)); err != nil {
		t.Fatalf("enqueue first with id: %v", err)
	}
	if err := c.Enqueue(ctx, q, "email.send", map[string]any{"x": 4}, TaskID(id)); err == nil {
		t.Fatalf("expected duplicate error")
	} else if err != ErrDuplicateTask {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIntegration_Enqueue_ExpiryIndex(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := fmt.Sprintf("q-%d", time.Now().UnixNano())

	if err := c.Enqueue(ctx, q, "t", map[string]any{"a": 1}, ExpireIn(5*time.Minute)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Expiry index should have one member
	if n, err := rdb.ZCard(ctx, ikeys.Expiry(q)).Result(); err != nil {
		t.Fatalf("zcard expiry: %v", err)
	} else if n != 1 {
		t.Fatalf("expiry count=%d", n)
	}
}

func TestIntegration_ListTasks_EmptyStateReturnsEmptySlice(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()

	q := fmt.Sprintf("empty-%d", time.Now().UnixNano())
	got, err := c.ListTasks(ctx, q, StatePending, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %d", len(got))
	}
}

func TestIntegration_DeleteTask_ReleasesUniqueLock(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := fmt.Sprintf("q-%d", time.Now().UnixNano())
	id := "del-1"

	if err := c.Enqueue(ctx, q, "t", map[string]any{"a": 1}, TaskID(id)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// identify the inserted item and delete by id
	ps, _ := c.ListTasks(ctx, q, StatePending, nil)
	if len(ps) != 1 {
		t.Fatalf("want 1 pending, got %d", len(ps))
	}
	if err := c.DeleteTask(ctx, q, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// unique lock should be released
	// In the test, we try to SAdd the same ID. If it was released, SAdd returns 1.
	// However, if we SAdd it ourselves here, the subsequent Enqueue(id) will FAIL with ErrDuplicateTask.
	// The original test seems to have a logical flaw when translated to how Enqueue works.
	// Let's verify it's NOT in the set instead of adding it.
	isMember, err := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	if err != nil {
		t.Fatalf("sismember: %v", err)
	}
	if isMember {
		t.Fatalf("unique lock was not released")
	}

	// Next enqueue with same id should succeed
	if err := c.Enqueue(ctx, q, "t", map[string]any{"a": 2}, TaskID(id)); err != nil {
		t.Fatalf("enqueue after release: %v", err)
	}

	// Now delete but keep unique lock
	if err := c.DeleteTask(ctx, q, id, WithKeepUniqueLock()); err != nil {
		t.Fatalf("delete keep: %v", err)
	}
	// Next enqueue should fail due to retained unique lock
	if err := c.Enqueue(ctx, q, "t", map[string]any{"a": 3}, TaskID(id)); err == nil {
		t.Fatalf("expected duplicate after keep unique")
	}
}

func TestIntegration_RetryDead_ReenqueueAndReset(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := fmt.Sprintf("q-%d", time.Now().UnixNano())

	// craft a dead task
	tsk := Task{ID: "dead-1", Type: "t", Queue: q, CreatedAt: time.Now().UnixMilli(), Retry: 3, LastError: "x"}
	raw, err := (&JSONEncoder{}).Encode(tsk)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := rdb.LPush(ctx, ikeys.Dead(q), raw).Err(); err != nil {
		t.Fatalf("lpush dead: %v", err)
	}

	// retry to delayed
	if err := c.RetryDead(ctx, q, tsk.ID, Delay(30*time.Second)); err != nil {
		t.Fatalf("retry dead: %v", err)
	}

	ds, err := c.ListTasks(ctx, q, StateDelayed, nil)
	if err != nil {
		t.Fatalf("list delayed: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("delayed count %d", len(ds))
	}
	if ds[0].Retry != 0 || ds[0].LastError != "" {
		t.Fatalf("expected reset fields, got %+v", ds[0])
	}
}

func TestIntegration_ConcurrentEnqueue_DuplicateID(t *testing.T) {
	rdb, done := newRedisClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := fmt.Sprintf("q-%d", time.Now().UnixNano())
	id := "race-1"

	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			errs <- c.Enqueue(ctx, q, "t", map[string]int{"i": i}, TaskID(id))
		}(i)
	}
	wg.Wait()
	close(errs)

	var ok, dup, other int
	for err := range errs {
		if err == nil {
			ok++
		} else if err == ErrDuplicateTask {
			dup++
		} else {
			other++
		}
	}
	if ok != 1 || dup != N-1 || other != 0 {
		t.Fatalf("results: ok=%d dup=%d other=%d", ok, dup, other)
	}
}
