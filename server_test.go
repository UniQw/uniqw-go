package uniqw

import (
	"context"
	"sync"
	"testing"
	"time"

	mrd "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestServer_StartStop_Idempotent(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	mux := NewMux()
	mux.Handle("t", func(ctx context.Context, b []byte) error { return nil })
	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{"q": 1},
		Concurrency:   0, // no workers
		VisibilityTTL: 1 * time.Second,
		Logger:        NewFmtLogger(),
	}, mux)

	srv.Start()
	srv.Start()
	srv.Stop()
	srv.Stop()
}

func TestServer_LoggingAndExecution(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	// Track execution
	executed := make(chan struct{}, 1)
	var execPayload []byte
	var payloadMu sync.Mutex

	mux := NewMux()
	mux.Handle("test.task", func(ctx context.Context, payload []byte) error {
		payloadMu.Lock()
		execPayload = payload
		payloadMu.Unlock()
		executed <- struct{}{}
		return nil
	})

	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{"test-queue": 1},
		Concurrency:   1,
		VisibilityTTL: 30 * time.Second,
	}, mux)

	// Test that server can be started and stopped multiple times
	srv.Start()
	srv.Start() // Should be idempotent

	// Enqueue a task
	c := NewClient(rdb)
	ctx := context.Background()
	payload := map[string]string{"message": "hello"}
	require.NoError(t, c.Enqueue(ctx, "test-queue", "test.task", payload))

	select {
	case <-executed:
		// Task was executed
	case <-time.After(5 * time.Second):
		t.Fatal("task was not executed within timeout")
	}

	payloadMu.Lock()
	require.NotNil(t, execPayload)
	payloadMu.Unlock()

	// Stop server
	srv.Stop()
	srv.Stop() // Should be idempotent
}

func TestServer_StartStop_WithNilLogger(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	mux := NewMux()
	mux.Handle("test", func(ctx context.Context, payload []byte) error { return nil })

	// Create server with nil logger (should use default)
	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{"q": 1},
		Concurrency:   1,
		VisibilityTTL: 30 * time.Second,
		Logger:        nil, // nil logger
	}, mux)

	srv.Start()
	srv.Stop()
}

func TestServer_NoHandlerExecution(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	// Mux with no handlers registered
	mux := NewMux()

	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{"test-queue": 1},
		Concurrency:   1,
		VisibilityTTL: 30 * time.Second,
	}, mux)

	srv.Start()
	defer srv.Stop()

	// Enqueue a task with no handler
	c := NewClient(rdb)
	ctx := context.Background()
	require.NoError(t, c.Enqueue(ctx, "test-queue", "nonexistent.task", map[string]string{"test": "data"}))

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)
}

func TestServer_NewServer_WithCustomLogger(t *testing.T) {
	s := mrd.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	// Custom logger that tracks log messages
	var logMessages []string
	customLogger := &testLogger{messages: &logMessages}

	mux := NewMux()
	mux.Handle("test", func(ctx context.Context, payload []byte) error { return nil })

	srv := NewServer(rdb, ServerConfig{
		Queues:        map[string]int{"q": 1},
		Concurrency:   1,
		VisibilityTTL: 30 * time.Second,
		Logger:        customLogger,
	}, mux)

	srv.Start()
	srv.Stop()

	// Verify logger was used
	require.NotEmpty(t, logMessages)
}

// testLogger is a simple logger for testing
type testLogger struct {
	messages *[]string
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	*l.messages = append(*l.messages, "[DEBUG] "+format)
}

func (l *testLogger) Infof(format string, args ...interface{}) {
	*l.messages = append(*l.messages, "[INFO] "+format)
}

func (l *testLogger) Warnf(format string, args ...interface{}) {
	*l.messages = append(*l.messages, "[WARN] "+format)
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	*l.messages = append(*l.messages, "[ERROR] "+format)
}
