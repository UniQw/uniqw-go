package uniqw

import (
	"context"
	"testing"
	"time"

	ikeys "github.com/UniQw/uniqw-go/internal/keys"
	mrd "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newMiniClient(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	s := mrd.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	cleanup := func() {
		_ = rdb.Close()
		s.Close()
	}
	return rdb, cleanup
}

func TestClient_Enqueue_Basics(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-enq-basic"

	// pending
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 1}))
	nPending, _ := rdb.LLen(ctx, ikeys.Pending(q)).Result()
	require.Equal(t, int64(1), nPending)

	// delayed
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 2}, Delay(1*time.Hour)))
	nDelayed, _ := rdb.ZCard(ctx, ikeys.Delayed(q)).Result()
	require.Equal(t, int64(1), nDelayed)

	// duplicate id rejection
	id := "dup-one"
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"x": 1}, TaskID(id)))
	err := c.Enqueue(ctx, q, "t", map[string]int{"x": 2}, TaskID(id))
	require.ErrorIs(t, err, ErrDuplicateTask)

	// expiry index on deadline
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 3}, ExpireIn(2*time.Minute)))
	nExpiry, _ := rdb.ZCard(ctx, ikeys.Expiry(q)).Result()
	require.Equal(t, int64(1), nExpiry)
}

func TestClient_ListTasks_And_NoneType(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-list"

	// none
	ts, err := c.ListTasks(ctx, q, StatePending, nil)
	require.NoError(t, err)
	require.Len(t, ts, 0)

	// list (pending)
	require.NoError(t, c.Enqueue(ctx, q, "email", map[string]any{"x": 1}))
	ts, err = c.ListTasks(ctx, q, StatePending, nil)
	require.NoError(t, err)
	require.Len(t, ts, 1)

	// zset (delayed)
	require.NoError(t, c.Enqueue(ctx, q, "email", map[string]any{"y": 1}, Delay(10*time.Minute)))
	ds, err := c.ListTasks(ctx, q, StateDelayed, nil)
	require.NoError(t, err)
	require.Len(t, ds, 1)
}

func TestClient_DeleteTask_Pending_Expiry_Unique(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-del"
	id := "del-1"

	// enqueue with deadline to index expiry and unique
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id), ExpireIn(5*time.Minute)))
	// verify present
	ts, err := c.ListTasks(ctx, q, StatePending, nil)
	require.NoError(t, err)
	require.Len(t, ts, 1)

	// delete and verify cleanup (pending removed, expiry cleared, unique released)
	require.NoError(t, c.DeleteTask(ctx, q, id))
	nPending, _ := rdb.LLen(ctx, ikeys.Pending(q)).Result()
	require.Equal(t, int64(0), nPending)
	nExpiry, _ := rdb.ZCard(ctx, ikeys.Expiry(q)).Result()
	require.Equal(t, int64(0), nExpiry)
	isMember, _ := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}

func TestClient_DeleteTask_KeepUniqueLock(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-del-keep"
	id := "keep-1"

	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id)))
	// delete but keep unique lock
	require.NoError(t, c.DeleteTask(ctx, q, id, WithKeepUniqueLock()))
	isMember, _ := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.True(t, isMember)
}

func TestClient_DeleteTask_FromDelayed(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-delayed-del"
	id := "d1"

	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id), Delay(1*time.Hour)))
	require.NoError(t, c.DeleteTask(ctx, q, id))
	nDelayed, _ := rdb.ZCard(ctx, ikeys.Delayed(q)).Result()
	require.Equal(t, int64(0), nDelayed)
	isMember, _ := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}

func TestClient_RetryDead_ToDelayed_WithOverrides(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-retry-dead"
	id := "dead-1"

	// craft a dead task and push into dead list
	tsk := Task{ID: id, Type: "t", Queue: q, MaxRetry: 3, Retention: 0, ErrRetention: -1}
	raw, err := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, err)
	require.NoError(t, rdb.LPush(ctx, ikeys.Dead(q), raw).Err())

	// retry with delay and deadline override
	dl := 10 * time.Minute
	require.NoError(t, c.RetryDead(ctx, q, id, Delay(dl), ExpireIn(1*time.Hour)))

	// dead removed; moved to delayed; expiry indexed
	nDead, _ := rdb.LLen(ctx, ikeys.Dead(q)).Result()
	require.Zero(t, nDead)
	nDelayed, _ := rdb.ZCard(ctx, ikeys.Delayed(q)).Result()
	require.Equal(t, int64(1), nDelayed)
	nExpiry, _ := rdb.ZCard(ctx, ikeys.Expiry(q)).Result()
	require.Equal(t, int64(1), nExpiry)
}

func TestClient_ListTasks_UnknownState(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	_, err := c.ListTasks(context.Background(), "q", State("unknown"), nil)
	require.ErrorIs(t, err, ErrUnknownState)
}

func TestClient_DeleteTask_ActiveRejected(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-active-del"
	id := "id-active"

	// add an active task (raw JSON)
	tsk := Task{ID: id, Type: "t", Queue: q}
	raw, _ := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, rdb.ZAdd(ctx, ikeys.Active(q), redis.Z{Score: 1234, Member: raw}).Err())

	err := c.DeleteTask(ctx, q, id)
	require.ErrorIs(t, err, ErrActiveState)
}

func TestClient_DeleteTask_NotFound(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	// queue empty, id not present
	err := c.DeleteTask(context.Background(), "q", "missing")
	require.ErrorIs(t, err, ErrTaskNotFound)
}

func TestClient_DeleteTask_FromDead(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-del-dead"
	id := "dead-x"

	// add a dead task (raw JSON)
	tsk := Task{ID: id, Type: "t", Queue: q}
	raw, _ := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, rdb.LPush(ctx, ikeys.Dead(q), raw).Err())
	// also add unique lock to ensure it is released
	require.NoError(t, rdb.SAdd(ctx, ikeys.Unique(q), id).Err())

	require.NoError(t, c.DeleteTask(ctx, q, id))
	ln, _ := rdb.LLen(ctx, ikeys.Dead(q)).Result()
	require.Equal(t, int64(0), ln)
	isMember, _ := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}

func TestClient_ListTasks_SucceededAndDead(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-list2"

	// succeeded (zset)
	t1 := Task{ID: "s1", Type: "t", Queue: q, CompletedAt: time.Now().UnixMilli()}
	raw1, _ := (&JSONEncoder{}).Encode(t1)
	require.NoError(t, rdb.ZAdd(ctx, ikeys.Succeeded(q), redis.Z{Score: float64(time.Now().UnixMilli() + 10_000), Member: raw1}).Err())
	// dead (list)
	t2 := Task{ID: "d1", Type: "t", Queue: q}
	raw2, _ := (&JSONEncoder{}).Encode(t2)
	require.NoError(t, rdb.LPush(ctx, ikeys.Dead(q), raw2).Err())

	ss, err := c.ListTasks(ctx, q, StateSucceeded, nil)
	require.NoError(t, err)
	require.Len(t, ss, 1)
	ds, err := c.ListTasks(ctx, q, StateDead, nil)
	require.NoError(t, err)
	require.Len(t, ds, 1)
}

func TestClient_ListTasks_WithFilter(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-filter"

	require.NoError(t, c.Enqueue(ctx, q, "x", map[string]int{"a": 1}))
	require.NoError(t, c.Enqueue(ctx, q, "y", map[string]int{"a": 2}))

	got, err := c.ListTasks(ctx, q, StatePending, func(tk *Task) bool { return tk.Type == "x" })
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "x", got[0].Type)
}

func TestClient_RetryDead_ToPending(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-retry-p"
	id := "dead-p"

	// dead item
	tsk := Task{ID: id, Type: "t", Queue: q}
	raw, _ := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, rdb.LPush(ctx, ikeys.Dead(q), raw).Err())

	// retry without delay -> pending
	require.NoError(t, c.RetryDead(ctx, q, id))
	ln, _ := rdb.LLen(ctx, ikeys.Pending(q)).Result()
	require.Equal(t, int64(1), ln)
}

func TestClient_DeleteTask_FromSucceeded(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-del-succ"
	id := "s-x"

	// put succeeded entry
	tsk := Task{ID: id, Type: "t", Queue: q, CompletedAt: time.Now().UnixMilli()}
	raw, _ := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, rdb.SAdd(ctx, ikeys.Unique(q), id).Err())
	require.NoError(t, rdb.ZAdd(ctx, ikeys.Succeeded(q), redis.Z{Score: float64(time.Now().Add(1 * time.Hour).UnixMilli()), Member: raw}).Err())

	require.NoError(t, c.DeleteTask(ctx, q, id))
	zc, _ := rdb.ZCard(ctx, ikeys.Succeeded(q)).Result()
	require.Equal(t, int64(0), zc)
	// uniqueness should NOT be released for succeeded
	isMember, _ := rdb.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.True(t, isMember)
}

func TestClient_RetryDead_NotFound(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	err := c.RetryDead(context.Background(), "q", "nope")
	require.ErrorIs(t, err, ErrTaskNotFound)
}

func TestClient_RetryDead_OverridesRetentionFields(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-ret-ovr"
	id := "dead-ovr"

	// seed dead with minimal task
	tsk := Task{ID: id, Type: "t", Queue: q, Retention: 0, ErrRetention: -1}
	raw, _ := (&JSONEncoder{}).Encode(tsk)
	require.NoError(t, rdb.LPush(ctx, ikeys.Dead(q), raw).Err())

	// override succeeded and error retention, and set delay
	require.NoError(t, c.RetryDead(ctx, q, id, Retention(10*time.Second), RetentionError(5*time.Second), Delay(1*time.Minute)))

	// read from delayed and verify fields updated
	zs, _ := rdb.ZRange(ctx, ikeys.Delayed(q), 0, -1).Result()
	require.Len(t, zs, 1)
	var out Task
	require.NoError(t, (&JSONEncoder{}).Decode([]byte(zs[0]), &out))
	require.Equal(t, int64(10), out.Retention)
	require.Equal(t, int64(5), out.ErrRetention)
}

func TestClient_Enqueue_MaxRetry_Deadline_AndErrRetention(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	c := NewClient(rdb)
	ctx := context.Background()
	q := "q-enq-opts"

	abs := time.Now().Add(30 * time.Minute)
	require.NoError(t, c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, MaxRetry(7), Deadline(abs), RetentionError(15*time.Second)))

	// read pending
	ls, _ := rdb.LRange(ctx, ikeys.Pending(q), 0, -1).Result()
	require.Len(t, ls, 1)
	var out Task
	require.NoError(t, (&JSONEncoder{}).Decode([]byte(ls[0]), &out))
	require.Equal(t, 7, out.MaxRetry)
	require.Equal(t, abs.UnixMilli(), out.DeadlineMs)
	require.Equal(t, int64(15), out.ErrRetention)
}
