package queue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueDequeueAck(t *testing.T) {
	ctx := context.Background()
	q := New("localhost:6379")

	j, err := NewJob("test", map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job %s, want %s", got.ID, j.ID)
	}
	if err := q.Ack(ctx, got); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
