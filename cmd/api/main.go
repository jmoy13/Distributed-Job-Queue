package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoy13/distributed-job-queue/internal/api"
	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/jmoy13/distributed-job-queue/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	q := queue.New("localhost:6379")
	st, err := store.New(ctx, "postgres://postgres:devpass@localhost:5432/jobqueue")
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{Addr: ":8080", Handler: api.New(q, st).Routes()}
	go func() {
		log.Println("API listening on :8080")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)
	log.Println("API stopped")
}
