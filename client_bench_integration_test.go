package uniqw

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newRedisClientForBench(b *testing.B) *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("skipping integration bench: redis ping failed: %v", err)
	}
	return rdb
}

func BenchmarkClientEnqueue_Serial(b *testing.B) {
	rdb := newRedisClientForBench(b)
	c := NewClient(rdb)
	ctx := context.Background()
	queue := "bench:" + uuid.NewString()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload := map[string]any{"i": i, "s": "hello"}
		if err := c.Enqueue(ctx, queue, "bench", payload, TaskID(uuid.NewString())); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientEnqueue_Parallel(b *testing.B) {
	rdb := newRedisClientForBench(b)
	c := NewClient(rdb)
	ctx := context.Background()
	queue := "bench:" + uuid.NewString()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			payload := map[string]any{"s": "hello"}
			if err := c.Enqueue(ctx, queue, "bench", payload, TaskID(uuid.NewString())); err != nil {
				b.Fatal(err)
			}
		}
	})
}
