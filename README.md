# UniQw-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/UniQw/uniqw-go.svg)](https://pkg.go.dev/github.com/UniQw/uniqw-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/UniQw/uniqw-go)](https://goreportcard.com/report/github.com/UniQw/uniqw-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**UniQw-Go** is a Redis-based task queue library for Go that prioritizes performance, reliability, and ease of use.
Designed to handle millions of tasks with unique features such as separate retention for success/failure, progress
tracking, and unique task locking.

## Key Features

- **At-Least-Once Delivery**: Guarantees that tasks are executed at least once.
- **Separate Retention**: Set different retention times for successful tasks vs failed tasks (Dead Letter Queue).
- **Task Uniqueness**: Prevent task duplication with unique keys per queue.
- **Progress & Result Tracking**: Handlers can report progress percentage and store execution results (JSON).
- **Cluster Friendly**: Uses Redis Hash Tags `{queue}` for full compatibility with Redis Cluster.
- **Graceful Shutdown**: Supports safe server termination without losing currently running tasks.
- **Comprehensive Inspection**: APIs to list, delete, and retry tasks in various states.

## Installation

```bash
go get github.com/UniQw/uniqw-go
```

## Quick Start

### Producer (Client)

```go
package main

import (
	"context"
	"time"
	"github.com/UniQw/uniqw-go"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	client := uniqw.NewClient(rdb)

	// Enqueue task with options
	err := client.Enqueue(context.Background(), "default", "send:email",
		map[string]string{"to": "user@example.com"},
		uniqw.MaxRetry(3),
		uniqw.Retention(1*time.Hour),       // Retain for 1 hour if successful
		uniqw.RetentionError(24*time.Hour), // Retain for 24 hours if fully failed
	)
	if err != nil {
		panic(err)
	}
}
```

### Consumer (Server)

```go
package main

import (
	"context"
	"github.com/UniQw/uniqw-go"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	mux := uniqw.NewMux()

	mux.Handle("send:email", func(ctx context.Context, payload []byte) error {
		// Report progress to dashboard
		uniqw.SetProgress(ctx, 50)

		// Store execution result
		uniqw.SetResult(ctx, map[string]string{"status": "delivered"})

		return nil
	})

	server := uniqw.NewServer(rdb, uniqw.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 10,
	}, mux)

	server.Start()
}
```

## Advanced Concepts

### Task States

Tasks move through several states:

1. **Pending**: Task is ready for execution.
2. **Active**: Task is being processed by a worker (with visibility TTL).
3. **Delayed**: Task scheduled for the future or in backoff retry period.
4. **Succeeded**: Task completed successfully (retained according to `Retention`).
5. **Dead**: Task failed after reaching `MaxRetry` (retained according to `RetentionError`).

### Task Management

You can inspect and manage the queue using the `Client`:

```go
// List failed tasks
tasks, _ := client.ListTasks(ctx, "default", uniqw.StateDead, nil)

// Retry task from Dead Letter Queue
client.RetryDead(ctx, "default", "task-id-123", uniqw.Delay(1*time.Minute))

// Delete a specific task (searches across all states)
client.DeleteTask(ctx, "default", "task-id-123")
```

## Testing

### Tests

```bash
# Run all tests (unit and integration)
make test

# With coverage
make cover
```

Note: Integration tests require Docker to run Redis via testcontainers.

## Full Examples

See the `examples/` folder for more detailed producer and consumer implementation examples.

## License

Distributed under the MIT License. See `LICENSE` for more information.