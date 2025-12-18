package uniqw

import (
	"context"
	"sync"
	"time"

	rtm "github.com/UniQw/uniqw-go/internal/runtime"
	"github.com/redis/go-redis/v9"
)

// ServerConfig defines the configuration for a UniQw server.
type ServerConfig struct {
	// Queues defines the queues to process and their relative weights.
	Queues map[string]int
	// Concurrency is the number of worker goroutines.
	Concurrency int
	// VisibilityTTL is the duration for which a task is leased by a worker.
	// If the worker fails or crashes, the task will be reclaimed after this TTL.
	VisibilityTTL time.Duration
	// Logger is the logger used for server events.
	Logger Logger
}

// Server processes tasks from Redis queues using workers.
type Server struct {
	rt      *rtm.Runtime
	mux     *Mux
	mu      sync.Mutex
	started bool
	log     Logger
}

// NewServer creates a new UniQw server.
func NewServer(rdb *redis.Client, cfg ServerConfig, mux *Mux) *Server {
	l := cfg.Logger
	if l == nil {
		l = NewFmtLogger()
	}
	exec := func(ctx context.Context, taskType string, payload []byte) error {
		h, ok := mux.handlers[taskType]
		if !ok {
			return rtm.ErrNoHandler
		}
		fn := mux.wrapHandler(h.exec)
		return fn(ctx, payload)
	}

	rtc := rtm.Config{
		Queues:        cfg.Queues,
		Concurrency:   cfg.Concurrency,
		VisibilityTTL: cfg.VisibilityTTL,
		Logger:        rtLogger{Logger: l},
	}
	return &Server{rt: rtm.New(rdb, rtc, exec), mux: mux, log: l}
}

// Start launches the server workers and background maintenance routines.
// It is idempotent and non-blocking.
func (s *Server) Start() {
	s.mu.Lock()
	if s.started {
		if s.log != nil {
			s.log.Warnf("server already started; ignoring Start()")
		}
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	if s.log != nil {
		s.log.Infof("starting server: concurrency=%d queues=%d", s.rt.CfgConcurrency(), len(s.rt.CfgQueues()))
	}
	s.rt.Start()
}

// Stop gracefully shuts down the server, waiting for workers to finish current tasks.
func (s *Server) Stop() {
	s.mu.Lock()
	if !s.started {
		if s.log != nil {
			s.log.Warnf("server not started; ignoring Stop()")
		}
		s.mu.Unlock()
		return
	}
	s.started = false
	s.mu.Unlock()
	if s.log != nil {
		s.log.Infof("stopping server")
	}
	s.rt.Stop()
}

// rtLogger adapts the public Logger to the internal runtime logger interface.
type rtLogger struct{ Logger }
