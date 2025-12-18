package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/UniQw/uniqw-go/internal/keys"
	mrd "github.com/alicebob/miniredis/v2"
	"github.com/bytedance/sonic"
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

func TestEncodeJSON_MatchesStdlib(t *testing.T) {
	v := &taskRec{ID: "a", Type: "t", Queue: "q", Retry: 1, MaxRetry: 3, Retention: 5}
	got := encodeJSON(v)
	exp, err := json.Marshal(v)
	require.NoError(t, err)
	require.Equal(t, exp, got, "encodeJSON should match json.Marshal output")
}

func TestEncodeJSON_RoundtripWithSonic(t *testing.T) {
	in := &taskRec{ID: "x", Type: "y", Queue: "z", Progress: 77, Result: []byte("ok")}
	data := encodeJSON(in)
	var out taskRec
	require.NoError(t, sonic.Unmarshal(data, &out))
	require.Equal(t, *in, out)
}

func TestWorker_Recycle(t *testing.T) {
	Recycle(nil) // should not panic
	tr := &taskRec{ID: "a"}
	Recycle(tr)
	// no real way to verify pool behavior without internal access, but we ensure it doesn't crash
}

func TestWorker_DequeueTask(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	ctx := context.Background()
	q := keys.For("q")

	// empty
	got, raw := DequeueTask(ctx, rdb, q, 1*time.Minute)
	require.Nil(t, got)
	require.Nil(t, raw)

	// push one
	task := &taskRec{ID: "t1"}
	data, _ := json.Marshal(task)
	rdb.LPush(ctx, q.Pending, data)

	got, raw = DequeueTask(ctx, rdb, q, 1*time.Minute)
	require.NotNil(t, got)
	require.Equal(t, "t1", got.ID)
	require.Equal(t, data, raw)

	// verify in active
	active, _ := rdb.ZCard(ctx, q.Active).Result()
	require.Equal(t, int64(1), active)
}

func TestWorker_Ack(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	ctx := context.Background()
	q := keys.For("q")

	raw := []byte("task-data")
	rdb.ZAdd(ctx, q.Active, redis.Z{Score: 123, Member: raw})

	require.NoError(t, Ack(ctx, rdb, q, raw))
	active, _ := rdb.ZCard(ctx, q.Active).Result()
	require.Equal(t, int64(0), active)
}

func TestWorker_FailToDead(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	ctx := context.Background()
	q := keys.For("q")

	t1 := &taskRec{ID: "t1", DeadlineMs: 12345}
	raw := encodeJSON(t1)

	rdb.ZAdd(ctx, q.Active, redis.Z{Score: 100, Member: raw})
	rdb.ZAdd(ctx, q.Expiry, redis.Z{Score: 12345, Member: raw})

	// Case 1: ErrRetention < 0 (keep forever, default)
	t1.ErrRetention = -1
	require.NoError(t, FailToDead(ctx, rdb, q, t1, raw, "error reason"))

	deadLen, _ := rdb.LLen(ctx, q.Dead).Result()
	require.Equal(t, int64(1), deadLen)

	activeLen, _ := rdb.ZCard(ctx, q.Active).Result()
	require.Equal(t, int64(0), activeLen)

	expiryLen, _ := rdb.ZCard(ctx, q.Expiry).Result()
	require.Equal(t, int64(0), expiryLen)

	// Case 2: ErrRetention > 0 (with expiry)
	rdb.FlushAll(ctx)
	t2 := &taskRec{ID: "t2", ErrRetention: 60, Progress: 150} // progress > 100 should clamp
	raw2 := encodeJSON(t2)
	require.NoError(t, FailToDead(ctx, rdb, q, t2, raw2, "reason"))

	require.Equal(t, 100, t2.Progress)
	expiryCount, _ := rdb.ZCard(ctx, q.DeadExpiry).Result()
	require.Equal(t, int64(1), expiryCount)

	// Case 3: ErrRetention == 0 (drop)
	rdb.FlushAll(ctx)
	t3 := &taskRec{ID: "t3", ErrRetention: 0, Progress: -5} // progress < 0 should clamp
	raw3 := encodeJSON(t3)
	require.NoError(t, FailToDead(ctx, rdb, q, t3, raw3, "drop it"))

	require.Equal(t, 0, t3.Progress)
	deadLen, _ = rdb.LLen(ctx, q.Dead).Result()
	require.Equal(t, int64(0), deadLen)
}

func TestWorker_TrackSucceededWithTTL(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	ctx := context.Background()
	q := keys.For("q")

	// Case 1: Retention <= 0 (no persist)
	t1 := &taskRec{ID: "t1", Retention: 0}
	require.NoError(t, TrackSucceededWithTTL(ctx, rdb, q, t1))
	sc, _ := rdb.ZCard(ctx, q.Succeeded).Result()
	require.Equal(t, int64(0), sc)

	// Case 2: Retention > 0
	t2 := &taskRec{ID: "t2", Retention: 60, Progress: 50}
	require.NoError(t, TrackSucceededWithTTL(ctx, rdb, q, t2))
	sc, _ = rdb.ZCard(ctx, q.Succeeded).Result()
	require.Equal(t, int64(1), sc)
}

func TestWorker_RetryOrDead(t *testing.T) {
	rdb, done := newMiniClient(t)
	defer done()
	ctx := context.Background()
	q := keys.For("q")

	// Case 1: Retry < MaxRetry
	t1 := &taskRec{ID: "t1", Retry: 0, MaxRetry: 3}
	raw1 := encodeJSON(t1)
	rdb.ZAdd(ctx, q.Active, redis.Z{Score: 100, Member: raw1})

	require.NoError(t, RetryOrDead(ctx, rdb, q, t1, raw1, "err"))
	require.Equal(t, 1, t1.Retry)

	delayedCount, _ := rdb.ZCard(ctx, q.Delayed).Result()
	require.Equal(t, int64(1), delayedCount)
	activeCount, _ := rdb.ZCard(ctx, q.Active).Result()
	require.Equal(t, int64(0), activeCount)

	// Case 2: Retry >= MaxRetry -> Dead
	rdb.FlushAll(ctx)
	t2 := &taskRec{ID: "t2", Retry: 3, MaxRetry: 3, ErrRetention: 30}
	raw2 := encodeJSON(t2)
	rdb.ZAdd(ctx, q.Active, redis.Z{Score: 100, Member: raw2})

	require.NoError(t, RetryOrDead(ctx, rdb, q, t2, raw2, "final error"))
	deadLen, _ := rdb.LLen(ctx, q.Dead).Result()
	require.Equal(t, int64(1), deadLen)
	expiryLen, _ := rdb.ZCard(ctx, q.DeadExpiry).Result()
	require.Equal(t, int64(1), expiryLen)
}
