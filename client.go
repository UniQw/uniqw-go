package uniqw

import (
	"context"
	"fmt"
	"strings"
	"time"

	ikeys "github.com/UniQw/uniqw-go/internal/keys"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Client provides APIs to enqueue and manage tasks in Redis.
type Client struct {
	rdb     redis.UniversalClient
	encoder Encoder
}

// NewClient creates a new UniQw client.
func NewClient(rdb redis.UniversalClient) *Client {
	return &Client{rdb: rdb, encoder: &JSONEncoder{}}
}

// Enqueue adds a new task to the specified queue.
// It returns ErrDuplicateTask if the task ID (explicit or generated) already exists in the queue.
func (c *Client) Enqueue(ctx context.Context, queue, taskType string, payload any, opts ...Option) error {
	data, err := c.encoder.Encode(payload)
	if err != nil {
		return err
	}

	cfg := &options{
		errRetention: -1 * time.Second, // default: keep forever (backward compat)
	}
	for _, opt := range opts {
		opt(cfg)
	}

	id := cfg.id
	if id == "" {
		id = uuid.NewString()
	}

	// Uniqueness check: reserve ID in queue-specific set.
	ukey := ikeys.Unique(queue)
	ok, err := c.rdb.SAdd(ctx, ukey, id).Result()
	if err != nil {
		return err
	}
	if ok == 0 {
		return ErrDuplicateTask
	}

	rt := Task{
		ID:           id,
		Type:         taskType,
		Queue:        queue,
		Payload:      data,
		MaxRetry:     cfg.maxRetry,
		Retention:    int64(cfg.retention.Seconds()),
		ErrRetention: int64(cfg.errRetention.Seconds()),
		CreatedAt:    time.Now().UnixMilli(),
		DeadlineMs:   cfg.deadlineMs,
	}

	raw, _ := c.encoder.Encode(rt)

	var opErr error
	if cfg.delay > 0 {
		_, opErr = c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.ZAdd(ctx, ikeys.Delayed(queue), redis.Z{
				Score:  float64(time.Now().Add(cfg.delay).Unix()),
				Member: raw,
			})
			if cfg.deadlineMs > 0 {
				p.ZAdd(ctx, ikeys.Expiry(queue), redis.Z{Score: float64(cfg.deadlineMs), Member: raw})
			}
			return nil
		})
	} else {
		_, opErr = c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.LPush(ctx, ikeys.Pending(queue), raw)
			if cfg.deadlineMs > 0 {
				p.ZAdd(ctx, ikeys.Expiry(queue), redis.Z{Score: float64(cfg.deadlineMs), Member: raw})
			}
			return nil
		})
	}

	if opErr != nil {
		// Rollback uniqueness on failure
		_ = c.rdb.SRem(ctx, ukey, id).Err()
		return opErr
	}

	return nil
}

// TaskFilter is a function used to filter tasks during ListTasks.
type TaskFilter func(*Task) bool

// ListTasks returns a list of tasks in a specific state for the given queue.
// It supports filtering tasks by any field.
func (c *Client) ListTasks(ctx context.Context, queue string, state State, filter TaskFilter) ([]*Task, error) {
	var key string
	switch state {
	case StatePending:
		key = ikeys.Pending(queue)
	case StateActive:
		key = ikeys.Active(queue)
	case StateDelayed:
		key = ikeys.Delayed(queue)
	case StateSucceeded:
		key = ikeys.Succeeded(queue)
	case StateDead:
		key = ikeys.Dead(queue)
	default:
		return nil, ErrUnknownState
	}

	typ, _ := c.rdb.Type(ctx, key).Result()
	if typ == "none" {
		return nil, nil
	}
	var strs []string
	var err error

	if typ == "list" {
		strs, err = c.rdb.LRange(ctx, key, 0, -1).Result()
	} else if typ == "zset" {
		strs, err = c.rdb.ZRange(ctx, key, 0, -1).Result()
	} else {
		return nil, fmt.Errorf("unsupported redis type: %s", typ)
	}

	if err != nil {
		return nil, err
	}

	out := make([]*Task, 0, len(strs))
	for _, s := range strs {
		var t Task
		if err := c.encoder.Decode([]byte(s), &t); err == nil {
			if filter == nil || filter(&t) {
				out = append(out, &t)
			}
		}
	}

	return out, nil
}

// DeleteTask removes a task from the specified queue by its ID.
// It searches across all states (Pending, Delayed, Succeeded, Dead) and removes the first match.
// It returns ErrTaskNotFound if the ID is not found in any of those states.
func (c *Client) DeleteTask(ctx context.Context, queue string, id string, opts ...Option) error {
	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	var target *Task
	var foundState State

	// Search in all deletable states
	searchStates := []State{StatePending, StateDelayed, StateSucceeded, StateDead}
	for _, s := range searchStates {
		tasks, err := c.ListTasks(ctx, queue, s, func(t *Task) bool { return t.ID == id })
		if err != nil {
			return err
		}
		if len(tasks) > 0 {
			target = tasks[0]
			foundState = s
			break
		}
	}

	if target == nil {
		// Check if it's in Active state to provide a better error message
		activeTasks, _ := c.ListTasks(ctx, queue, StateActive, func(t *Task) bool { return t.ID == id })
		if len(activeTasks) > 0 {
			return ErrActiveState
		}
		return ErrTaskNotFound
	}

	var key string
	switch foundState {
	case StatePending:
		key = ikeys.Pending(queue)
	case StateDelayed:
		key = ikeys.Delayed(queue)
	case StateSucceeded:
		key = ikeys.Succeeded(queue)
	case StateDead:
		key = ikeys.Dead(queue)
	}

	raw, _ := c.encoder.Encode(target)

	_, err := c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		typ, _ := c.rdb.Type(ctx, key).Result()
		if typ == "list" {
			p.LRem(ctx, key, 1, raw)
		} else {
			p.ZRem(ctx, key, raw)
		}

		if foundState == StatePending || foundState == StateDelayed {
			if target.DeadlineMs > 0 {
				p.ZRem(ctx, ikeys.Expiry(queue), raw)
			}
		}

		if foundState == StateDead {
			p.ZRem(ctx, ikeys.DeadExpiry(queue), raw)
		}

		if !cfg.keepUnique && foundState != StateSucceeded {
			p.SRem(ctx, ikeys.Unique(queue), id)
		}
		return nil
	})

	return err
}

// RetryDead moves a task from the Dead Letter Queue back to Pending or Delayed state.
// It resets retry counts and errors. You can override retention settings or add a delay.
func (c *Client) RetryDead(ctx context.Context, queue string, id string, opts ...Option) error {
	tasks, err := c.ListTasks(ctx, queue, StateDead, func(t *Task) bool { return t.ID == id })
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return ErrTaskNotFound
	}

	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	t := tasks[0]
	rawOld, _ := c.encoder.Encode(t)

	t.Retry = 0
	t.LastError = ""
	t.LastErrorAt = 0
	if cfg.retention != 0 {
		t.Retention = int64(cfg.retention.Seconds())
	}
	if cfg.errRetention != 0 {
		t.ErrRetention = int64(cfg.errRetention.Seconds())
	}

	rawNew, _ := c.encoder.Encode(t)

	_, err = c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.LRem(ctx, ikeys.Dead(queue), 1, rawOld)
		p.ZRem(ctx, ikeys.DeadExpiry(queue), rawOld)

		if cfg.delay > 0 {
			p.ZAdd(ctx, ikeys.Delayed(queue), redis.Z{
				Score:  float64(time.Now().Add(cfg.delay).Unix()),
				Member: rawNew,
			})
		} else {
			p.LPush(ctx, ikeys.Pending(queue), rawNew)
		}

		if cfg.deadlineMs > 0 {
			p.ZAdd(ctx, ikeys.Expiry(queue), redis.Z{
				Score:  float64(cfg.deadlineMs),
				Member: rawNew,
			})
		}
		return nil
	})

	return err
}

// ExtractQueueName parses a queue name from a raw Redis key (e.g. "uniqw:{default}:pending").
// It returns an empty string if the format is invalid.
func ExtractQueueName(key string) string {
	start := strings.Index(key, "{")
	if start == -1 {
		return ""
	}
	end := strings.Index(key, "}")
	if end == -1 || end <= start+1 {
		return ""
	}
	return key[start+1 : end]
}
