package uniqw

import (
	"context"
	"errors"
	"testing"
	"time"

	ikeys "github.com/UniQw/uniqw-go/internal/keys"
	mrd "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// interceptingClient wraps a real redis.Client and can inject failures for specific commands.
type interceptingClient struct {
	*redis.Client
	failLPush      bool
	failZAddExpiry bool
	failZAddDelay  bool
}

func (ic *interceptingClient) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	if ic.failLPush {
		cmd := redis.NewIntCmd(ctx)
		cmd.SetErr(errors.New("LPUSH failure (injected)"))
		return cmd
	}
	return ic.Client.LPush(ctx, key, values...)
}

func (ic *interceptingClient) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	if ic.failZAddExpiry && key == ikeys.Expiry(extractQueueFromKey(key)) {
		cmd := redis.NewIntCmd(ctx)
		cmd.SetErr(errors.New("ZADD expiry failure (injected)"))
		return cmd
	}
	if ic.failZAddDelay && key == ikeys.Delayed(extractQueueFromKey(key)) {
		cmd := redis.NewIntCmd(ctx)
		cmd.SetErr(errors.New("ZADD delayed failure (injected)"))
		return cmd
	}
	return ic.Client.ZAdd(ctx, key, members...)
}

func (ic *interceptingClient) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if ic.failLPush || ic.failZAddExpiry || ic.failZAddDelay {
		// simulate failure by returning error from the function or from pipeline
		pip := ic.Client.Pipeline()
		err := fn(&interceptingPipeliner{Pipeliner: pip, ic: ic, ctx: ctx})
		if err != nil {
			return nil, err
		}
		return nil, errors.New("pipeline failure (injected)")
	}
	return ic.Client.TxPipelined(ctx, fn)
}

type interceptingPipeliner struct {
	redis.Pipeliner
	ic  *interceptingClient
	ctx context.Context
}

func (ip *interceptingPipeliner) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	if ip.ic.failLPush {
		return ip.ic.LPush(ctx, key, values...)
	}
	return ip.Pipeliner.LPush(ctx, key, values...)
}

func (ip *interceptingPipeliner) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	if ip.ic.failZAddExpiry || ip.ic.failZAddDelay {
		return ip.ic.ZAdd(ctx, key, members...)
	}
	return ip.Pipeliner.ZAdd(ctx, key, members...)
}

// extractQueueFromKey assumes key format: "uniqw:{<queue>}:<suffix>"
func extractQueueFromKey(key string) string {
	// naive parse to avoid allocations; acceptable for test-only
	// find '{' and '}'
	lb := -1
	rb := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			lb = i
		} else if key[i] == '}' {
			rb = i
			break
		}
	}
	if lb >= 0 && rb > lb+1 {
		return key[lb+1 : rb]
	}
	return ""
}

func newInterceptingClient(t *testing.T) (*interceptingClient, func()) {
	t.Helper()
	s := mrd.RunT(t)
	base := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ic := &interceptingClient{Client: base}
	cleanup := func() { _ = base.Close(); s.Close() }
	return ic, cleanup
}

func TestClient_Enqueue_RollbackOnLPushFailure(t *testing.T) {
	ic, done := newInterceptingClient(t)
	defer done()
	c := NewClient(ic)
	ctx := context.Background()
	q := "q-fail-lpush"
	id := "id-lp"

	ic.failLPush = true
	err := c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id))
	require.Error(t, err)
	// unique reservation should be rolled back
	isMember, _ := ic.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}

func TestClient_Enqueue_RollbackOnZAddDelayedFailure(t *testing.T) {
	ic, done := newInterceptingClient(t)
	defer done()
	c := NewClient(ic)
	ctx := context.Background()
	q := "q-fail-zadd-delay"
	id := "id-zd"

	ic.failZAddDelay = true
	err := c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id), Delay(1*time.Hour))
	require.Error(t, err)
	// unique reservation should be rolled back
	isMember, _ := ic.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}

func TestClient_Enqueue_RollbackOnZAddExpiryFailure(t *testing.T) {
	ic, done := newInterceptingClient(t)
	defer done()
	c := NewClient(ic)
	ctx := context.Background()
	q := "q-fail-zadd-exp"
	id := "id-xp"

	ic.failZAddExpiry = true
	err := c.Enqueue(ctx, q, "t", map[string]int{"a": 1}, TaskID(id), ExpireIn(10*time.Minute))
	require.Error(t, err)
	// unique reservation should be rolled back
	isMember, _ := ic.SIsMember(ctx, ikeys.Unique(q), id).Result()
	require.False(t, isMember)
}
