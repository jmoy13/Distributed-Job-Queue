package queue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Attempts   int             `json:"attempts"`
	MaxRetries int             `json:"max_retries"`
	CreatedAt  time.Time       `json:"created_at"`

	raw string // serialized form currently in Redis; needed for LRem/ZRem
}

func NewJob(jobType string, payload any) (*Job, error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Job{
		ID:         uuid.NewString(),
		Type:       jobType,
		Payload:    p,
		MaxRetries: 3,
		CreatedAt:  time.Now().UTC(),
	}, nil
}

func (j *Job) Raw() string {
	return j.raw
}
