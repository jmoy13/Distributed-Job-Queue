package worker

import (
	"context"
	"encoding/json"
	"fmt"
)

type Handler func(ctx context.Context, payload json.RawMessage) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, error) {
	h, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler for job type %q", jobType)
	}
	return h, nil
}
