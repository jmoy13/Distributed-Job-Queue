package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	q   *queue.Queue
	reg *Registry
	id  int
}

func New(q *queue.Queue, reg *Registry) *Worker {
	return &Worker{q: q, reg: reg}
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
	log.Printf("[w%d] processing job %s (%s)", w.id, job.ID, job.Type)
	h, err := w.reg.Get(job.Type)
	if err != nil {
		log.Printf("job %s: %v", w.id, job.ID, err)
		job.Attempts = job.MaxRetries // force DLQ
		w.q.Fail(ctx, job)
		return
	}
	ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h(ctx, job.Payload); err != nil {
		log.Printf("[w%d] job %s failed: %v", w.id, job.ID, err)
		w.q.Fail(ackCtx, job)
		return
	}
	if err := w.q.Ack(ackCtx, job); err != nil {
		log.Printf("[w%d] ack failed for %s: %v", w.id, job.ID, err)
	}
	log.Printf("[w%d] job %s done", w.id, job.ID)
}
