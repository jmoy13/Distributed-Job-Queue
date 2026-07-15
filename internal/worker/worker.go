package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/jmoy13/distributed-job-queue/internal/store"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	q   *queue.Queue
	reg *Registry
	st  *store.Store
	id  int
}

func New(q *queue.Queue, reg *Registry, st *store.Store) *Worker {
	return &Worker{q: q, reg: reg, st: st}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[w%d] worker shutting down", w.id)
			return
		default:
		}

		job, err := w.q.Dequeue(ctx, 5*time.Second)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // timeout, poll again
			}
			log.Printf("dequeue error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job *queue.Job) {
	log.Printf("[w%d] processing job %s (%s) attempt %d", w.id, job.ID, job.Type, job.Attempts+1)

	// detached context so cleanup works during shutdown
	ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// status: running
	w.st.SetStatus(ackCtx, job.ID, "running", "", job.Attempts)

	h, err := w.reg.Get(job.Type)
	if err != nil {
		log.Printf("[w%d] job %s: %v -> DLQ", w.id, job.ID, err)
		job.Attempts = job.MaxRetries // force DLQ
		w.q.Fail(ackCtx, job)
		w.st.SetStatus(ackCtx, job.ID, "dead", err.Error(), job.Attempts)
		return
	}

	if err := h(ctx, job.Payload); err != nil {
		log.Printf("[w%d] job %s failed: %v", w.id, job.ID, err)
		w.q.Fail(ackCtx, job) // increments job.Attempts
		status := "retrying"
		if job.Attempts >= job.MaxRetries {
			status = "dead"
		}
		w.st.SetStatus(ackCtx, job.ID, status, err.Error(), job.Attempts)
		return
	}

	if err := w.q.Ack(ackCtx, job); err != nil {
		log.Printf("[w%d] ack failed for %s: %v", w.id, job.ID, err)
	}
	w.st.SetStatus(ackCtx, job.ID, "succeeded", "", job.Attempts)
	log.Printf("[w%d] job %s done", w.id, job.ID)
}
