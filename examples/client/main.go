package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	uniqw "github.com/UniQw/uniqw-go"
	"github.com/redis/go-redis/v9"
)

// Email matches the payload expected by the server example.
type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	addr := getenv("REDIS_ADDR", "127.0.0.1:6379")
	pass := getenv("REDIS_PASSWORD", "")

	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: pass})
	defer rdb.Close()

	ctx := context.Background()
	cli := uniqw.NewClient(rdb)

	e := Email{To: "user@example.com", Subject: "Immediate", Body: "Hello now!"}
	wg := sync.WaitGroup{}
	for i := 0; i <= 100; i++ {
		wg.Go(func(i int) func() {
			return func() {
				if err := cli.Enqueue(ctx, "default", "email:send", e,
					uniqw.Retention(30*time.Second),
				); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("task %d\n", i)
			}
		}(i))
	}
	wg.Wait()

	fmt.Println("Enqueued example tasks. Waiting briefly for processing...")
	time.Sleep(2 * time.Second)
	if tasks, err := cli.ListTasks(ctx, "default", uniqw.StateSucceeded, nil); err == nil {
		fmt.Printf("succeeded count: %d\n", len(tasks))
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
