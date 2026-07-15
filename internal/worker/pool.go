package worker

import (
	"context"
	"log"
	"sync"

	"github.com/jmoy13/distributed-job-queue/internal/queue"
	"github.com/jmoy13/distributed-job-queue/internal/store"
)

type Pool struct {
	q    *queue.Queue
	reg  *Registry
	st   *store.Store
	size int
}

func NewPool(q *queue.Queue, reg *Registry, st *store.Store, size int) *Pool {
	return &Pool{q: q, reg: reg, st: st, size: size}
}

func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 1; i <= p.size; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := New(p.q, p.reg, p.st)
			w.id = id
			w.Run(ctx)
		}(i)
	}
	wg.Wait()
	log.Println("all workers drained, pool stopped")
}
