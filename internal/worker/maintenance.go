package worker

import (
	"context"
	"log"
	"time"

	"github.com/jmoy13/distributed-job-queue/internal/queue"
)

func RunMaintenance(ctx context.Context, q *queue.Queue) {
	retryTick := time.NewTicker(1 * time.Second)
	reapTick := time.NewTicker(10 * time.Second)
	defer retryTick.Stop()
	defer reapTick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("maintenance shutting down")
			return
		case <-retryTick.C:
			if n, err := q.PromoteRetries(ctx); err != nil {
				log.Printf("promote error: %v", err)
			} else if n > 0 {
				log.Printf("promoted %d retry job(s)", n)
			}
		case <-reapTick.C:
			if n, err := q.ReapExpired(ctx); err != nil {
				log.Printf("reap error: %v", err)
			} else if n > 0 {
				log.Printf("reaped %d expired job(s)", n)
			}
		}
	}
}
