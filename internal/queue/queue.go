package queue

import (
	"context"
	"encoding/json"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pendingKey    = "jobs:pending"
	processingKey = "jobs:processing"
	retryKey      = "jobs:retry"
	dlqKey        = "jobs:dlq"
	leaseKey      = "jobs:leases"
	leaseTTL      = 30 * time.Second
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
	j.raw = data
	deadline := float64(time.Now().Add(leaseTTL).Unix())
	if err := q.rdb.ZAdd(ctx, leaseKey, redis.Z{Score: deadline, Member: data}).Err(); err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *Queue) Ack(ctx context.Context, j *Job) error {
	pipe := q.rdb.TxPipeline()
	pipe.LRem(ctx, processingKey, 1, j.raw)
	pipe.ZRem(ctx, leaseKey, j.raw)
	_, err := pipe.Exec(ctx)
	return err
}

// Fail: retry with backoff, or DLQ if out of attempts.
func (q *Queue) Fail(ctx context.Context, j *Job) error {
	j.Attempts++
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	pipe := q.rdb.TxPipeline()
	pipe.LRem(ctx, processingKey, 1, j.raw)
	pipe.ZRem(ctx, leaseKey, j.raw)
	if j.Attempts >= j.MaxRetries {
		pipe.LPush(ctx, dlqKey, data)
	} else {
		backoff := time.Duration(1<<j.Attempts) * time.Second
		jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
		retryAt := float64(time.Now().Add(backoff + jitter).Unix())
		pipe.ZAdd(ctx, retryKey, redis.Z{Score: retryAt, Member: data})
	}
	_, err = pipe.Exec(ctx)
	return err
}

// PromoteRetries moves due retry jobs back to pending. Returns count.
func (q *Queue) PromoteRetries(ctx context.Context) (int, error) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	due, err := q.rdb.ZRangeByScore(ctx, retryKey, &redis.ZRangeBy{Min: "0", Max: now}).Result()
	if err != nil || len(due) == 0 {
		return 0, err
	}
	pipe := q.rdb.TxPipeline()
	for _, d := range due {
		pipe.LPush(ctx, pendingKey, d)
		pipe.ZRem(ctx, retryKey, d)
	}
	_, err = pipe.Exec(ctx)
	return len(due), err
}

// ReapExpired re-enqueues jobs whose lease expired (crashed worker).
func (q *Queue) ReapExpired(ctx context.Context) (int, error) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	expired, err := q.rdb.ZRangeByScore(ctx, leaseKey, &redis.ZRangeBy{Min: "0", Max: now}).Result()
	if err != nil || len(expired) == 0 {
		return 0, err
	}
	pipe := q.rdb.TxPipeline()
	for _, e := range expired {
		pipe.LRem(ctx, processingKey, 1, e)
		pipe.ZRem(ctx, leaseKey, e)
		pipe.LPush(ctx, pendingKey, e)
	}
	_, err = pipe.Exec(ctx)
	return len(expired), err
}
