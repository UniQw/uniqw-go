package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	uniqw "github.com/UniQw/uniqw-go"
	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

// Email is a sample payload used in the examples.
type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// loggingMiddleware logs start/end and duration for each handler invocation.
func loggingMiddleware(l uniqw.Logger) uniqw.Middleware {
	return func(next uniqw.HandlerFunc) uniqw.HandlerFunc {
		return func(ctx context.Context, payload []byte) error {
			start := time.Now()
			err := next(ctx, payload)
			if err != nil {
				l.Warnf("handler error: dur=%s err=%v", time.Since(start), err)
			} else {
				l.Debugf("handler ok: dur=%s", time.Since(start))
			}
			return err
		}
	}
}

func main() {
	// Configure Redis connection (override via REDIS_ADDR / REDIS_PASSWORD).
	addr := getenv("REDIS_ADDR", "127.0.0.1:6379")
	pass := getenv("REDIS_PASSWORD", "")

	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: pass})
	defer rdb.Close()

	log := uniqw.NewFmtLogger()

	// Define handlers via Mux.
	mux := uniqw.NewMux()
	mux.Use(loggingMiddleware(log))

	// Successful handler example.
	mux.Handle("email:send", func(ctx context.Context, payload []byte) error {
		var m Email
		if err := sonic.Unmarshal(payload, &m); err != nil {
			return fmt.Errorf("decode email payload: %w", err)
		}
		log.Infof("email:send -> to=%s subject=%q body=%q", m.To, m.Subject, m.Body)
		return nil
	})

	// Failing handler (to demonstrate retry/backoff and dead-letter after MaxRetry).
	mux.Handle("email:fail", func(ctx context.Context, payload []byte) error {
		return fmt.Errorf("simulated failure")
	})

	// Start server with two queues: "default" and "a" (for duplicate demo).
	srv := uniqw.NewServer(rdb, uniqw.ServerConfig{
		Queues:        map[string]int{"default": 1},
		Concurrency:   40,
		VisibilityTTL: 30 * time.Second,
		Logger:        log,
	}, mux)

	srv.Start()
	log.Infof("server started; redis=%s queues=%v concurrency=%d", addr, []string{"default", "a"}, 4)

	// Wait for SIGINT/SIGTERM and stop gracefully.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Infof("signal received; stopping server...")
	srv.Stop()
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
