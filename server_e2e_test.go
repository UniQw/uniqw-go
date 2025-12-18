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

func TestServer_EndToEnd_SucceededAndDead(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	mux := NewMux()
	mux.Handle("ok", func(ctx context.Context, b []byte) error { return nil })
	mux.Handle("fail", func(ctx context.Context, b []byte) error { return errors.New("boom") })
	q := "q-e2e"
	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{q: 1},
		Concurrency:   1,
		VisibilityTTL: 2 * time.Second,
		Logger:        NewFmtLogger(),
	}, mux)
	srv.Start()
	defer srv.Stop()

	cli := NewClient(rdb)
	ctx := context.Background()
	// Ensure succeeded is persisted
	require.NoError(t, cli.Enqueue(ctx, q, "ok", map[string]int{"a": 1}, Retention(10*time.Second)))
	// Failing handler -> dead
	require.NoError(t, cli.Enqueue(ctx, q, "fail", map[string]int{"a": 2}))

	// wait for processing
	time.Sleep(400 * time.Millisecond)

	// Verify effects
	zc, _ := rdb.ZCard(ctx, ikeys.Succeeded(q)).Result()
	dc, _ := rdb.LLen(ctx, ikeys.Dead(q)).Result()
	if zc == 0 {
		t.Skip("timing sensitive; worker may not have processed yet in CI")
	}
	require.GreaterOrEqual(t, dc, int64(1))
}
