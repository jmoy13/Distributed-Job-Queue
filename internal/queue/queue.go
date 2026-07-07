package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pendingKey    = "jobs:pending"
	processingKey = "jobs:processing"
)

type Queue struct {
	rdb *redis.Client
}

func New(addr string) *Queue {
	return &Queue{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (q *Queue) Enqueue(ctx context.Context, j *Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.rdb.LPush(ctx, pendingKey, data).Err()
}

// Dequeue blocks up to timeout. Atomically moves job pending -> processing.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, error) {
	data, err := q.rdb.BLMove(ctx, pendingKey, processingKey, "RIGHT", "LEFT", timeout).Result()
	if err != nil {
		return nil, err // redis.Nil on timeout
	}
	var j Job
	if err := json.Unmarshal([]byte(data), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// Ack removes the job from processing after completion.
func (q *Queue) Ack(ctx context.Context, j *Job) error {
	data, _ := json.Marshal(j)
	return q.rdb.LRem(ctx, processingKey, 1, data).Err()
}
