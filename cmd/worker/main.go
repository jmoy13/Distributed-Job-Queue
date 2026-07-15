package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/jmoy13/distributed-job-queue/internal/store"
	"github.com/jmoy13/distributed-job-queue/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	q := queue.New(env("REDIS_ADDR", "localhost:6379"))

	reg := worker.NewRegistry()
	reg.Register("send_email", func(ctx context.Context, payload json.RawMessage) error {
		var p struct{ To, Subject string }
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond) // simulate work
		fmt.Printf("📧 sent %q to %s\n", p.Subject, p.To)
		return nil
	})

	reg.Register("flaky", func(ctx context.Context, payload json.RawMessage) error {
		if rand.Float64() < 0.7 {
			return fmt.Errorf("simulated failure")
		}
		return nil
	})

	for i := 0; i < 5; i++ {
		j, _ := queue.NewJob("flaky", map[string]string{"n": fmt.Sprint(i)})
		if err := q.Enqueue(ctx, j); err != nil {
			log.Fatal(err)
		}
	}

	// seed some jobs for testing
	for i := 0; i < 20; i++ {
		j, _ := queue.NewJob("send_email", map[string]string{
			"To": fmt.Sprintf("user%d@test.com", i), "Subject": "hello",
		})
		if err := q.Enqueue(ctx, j); err != nil {
			log.Fatal(err)
		}
	}

	st, err := store.New(ctx, env("DATABASE_URL", "postgres://postgres:devpass@localhost:5432/jobqueue"))

	if err != nil {
		log.Fatal(err)
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9090", nil)
	}()
	size, _ := strconv.Atoi(env("WORKER_COUNT", "4"))
	worker.NewPool(q, reg, st, size).Run(ctx)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
