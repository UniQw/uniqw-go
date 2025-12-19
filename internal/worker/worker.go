package worker

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/UniQw/uniqw-go/internal/keys"
	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

// taskRec is a minimal internal representation used to manage task lifecycle.
type taskRec struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Queue        string `json:"queue"`
	Payload      []byte `json:"payload"`
	Retry        int    `json:"retry"`
	MaxRetry     int    `json:"max_retry"`
	Retention    int64  `json:"retention"`
	ErrRetention int64  `json:"err_retention,omitempty"`
	// Metadata
	CreatedAt   int64  `json:"created_at,omitempty"`
	DeadlineMs  int64  `json:"deadline_ms,omitempty"`
	StartedAt   int64  `json:"started_at,omitempty"`
	CompletedAt int64  `json:"completed_at,omitempty"`
	LastError   string `json:"last_error,omitempty"`
	LastErrorAt int64  `json:"last_error_at,omitempty"`
	Progress    int    `json:"progress,omitempty"`
	Result      []byte `json:"result,omitempty"`
}

var taskPool = sync.Pool{New: func() any { return new(taskRec) }}

// bytes buffer pool removed since sonic.Marshal returns []byte directly with
// competitive allocation behavior.

// Atomic dequeue script: RPOP from pending and ZADD into active with visibility score.
var dequeueScript = redis.NewScript(
	// language=Lua
	`
	local v = redis.call('RPOP', KEYS[1])
	if not v then return false end
	redis.call('ZADD', KEYS[2], ARGV[1], v)
	return v
	`,
)

// Recycle returns a taskRec to the pool to reduce allocations.
func Recycle(t *taskRec) {
	if t == nil {
		return
	}
	// zeroing is optional because json.Unmarshal overwrites all fields we use,
	// but keep it explicit to avoid future surprises.
	*t = taskRec{}
	taskPool.Put(t)
}

// DequeueTask atomically moves a task from the Pending list to the Active ZSET
// and returns the task object and its raw JSON representation.
func DequeueTask(ctx context.Context, rdb redis.UniversalClient, k keys.Queue, ttl time.Duration) (*taskRec, []byte) {
	expire := time.Now().Add(ttl).Unix()
	res, err := dequeueScript.Run(ctx, rdb, []string{k.Pending, k.Active}, strconv.FormatInt(expire, 10)).Result()
	if err == redis.Nil || res == nil {
		return nil, nil
	}
	if err != nil {
		return nil, nil
	}
	var raw []byte
	switch v := res.(type) {
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		return nil, nil
	}

	t := taskPool.Get().(*taskRec)
	_ = sonic.Unmarshal(raw, t)
	return t, raw
}

// Ack removes a task from the Active ZSET after successful processing.
func Ack(ctx context.Context, rdb redis.UniversalClient, k keys.Queue, raw []byte) error {
	return rdb.ZRem(ctx, k.Active, raw).Err()
}

// FailToDead moves a task from the Active ZSET to the Dead list.
func FailToDead(ctx context.Context, rdb redis.UniversalClient, k keys.Queue, t *taskRec, raw []byte, reason string) error {
	// Populate last error reason if provided
	if reason != "" {
		t.LastError = reason
		t.LastErrorAt = time.Now().UnixMilli()
	}
	// Clamp progress
	if t.Progress < 0 {
		t.Progress = 0
	} else if t.Progress > 100 {
		t.Progress = 100
	}
	newRaw := encodeJSON(t)
	nowMs := time.Now().UnixMilli()
	// Index into dead expiry only if ErrRetention > 0 (retain for finite time)
	var addExpiry bool
	var expireMs int64
	if t.ErrRetention > 0 {
		addExpiry = true
		expireMs = nowMs + (t.ErrRetention * 1000)
	}

	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, k.Active, raw)
		if t.DeadlineMs > 0 {
			p.ZRem(ctx, k.Expiry, raw)
		}
		// Only push into dead list if we are retaining (ErrRetention != 0).
		// If ErrRetention == 0, drop it (no retention requested).
		if t.ErrRetention != 0 {
			p.LPush(ctx, k.Dead, newRaw)
			if addExpiry {
				p.ZAdd(ctx, k.DeadExpiry, redis.Z{Score: float64(expireMs), Member: newRaw})
			}
		}
		return nil
	})
	return err
}

// TrackSucceededWithTTL moves a task to the Succeeded ZSET with an expiration TTL.
func TrackSucceededWithTTL(ctx context.Context, rdb redis.UniversalClient, k keys.Queue, t *taskRec) error {
	// If retention is zero, do not persist succeeded entry at all.
	if t.Retention <= 0 {
		return nil
	}
	// Store in ZSET with millisecond precision to avoid batch expirations.
	t.CompletedAt = time.Now().UnixMilli()
	// Clamp progress
	if t.Progress < 0 {
		t.Progress = 0
	} else if t.Progress > 100 {
		t.Progress = 100
	}
	newRaw := encodeJSON(t)
	expireMs := t.CompletedAt + (t.Retention * 1000)
	return rdb.ZAdd(ctx, k.Succeeded, redis.Z{Score: float64(expireMs), Member: newRaw}).Err()
}

// RetryOrDead either re-enqueues a task for retry in the Delayed ZSET
// or moves it to the Dead list if max retries are exceeded.
func RetryOrDead(ctx context.Context, rdb redis.UniversalClient, k keys.Queue, t *taskRec, raw []byte, lastErr string) error {
	if t.Retry >= t.MaxRetry {
		t.LastError = lastErr
		t.LastErrorAt = time.Now().UnixMilli()
		// Clamp progress
		if t.Progress < 0 {
			t.Progress = 0
		} else if t.Progress > 100 {
			t.Progress = 100
		}
		newRaw := encodeJSON(t)
		nowMs := time.Now().UnixMilli()
		var addExpiry bool
		var expireMs int64
		if t.ErrRetention > 0 {
			addExpiry = true
			expireMs = nowMs + (t.ErrRetention * 1000)
		}
		_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.ZRem(ctx, k.Active, raw)
			if t.DeadlineMs > 0 {
				p.ZRem(ctx, k.Expiry, raw)
			}
			if t.ErrRetention != 0 {
				p.LPush(ctx, k.Dead, newRaw)
				if addExpiry {
					p.ZAdd(ctx, k.DeadExpiry, redis.Z{Score: float64(expireMs), Member: newRaw})
				}
			}
			return nil
		})
		return err
	}

	t.Retry++
	// Re-encode task to persist the incremented retry count.
	t.LastError = lastErr
	t.LastErrorAt = time.Now().UnixMilli()
	newRaw := encodeJSON(t)
	next := time.Now().Add(time.Second * time.Duration(1<<t.Retry)).Unix()
	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, k.Active, raw)
		p.ZAdd(ctx, k.Delayed, redis.Z{Score: float64(next), Member: newRaw})
		return nil
	})
	return err
}

// encodeJSON encodes value using stdlib json.Marshal for lower latency in encoding.
func encodeJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
