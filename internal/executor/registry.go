package executor

import (
	"context"
	"errors"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

func (r *Registry) Register(jobType string, handler Handler) error {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return errors.New("job type is empty")
	}

	if handler == nil {
		return errors.New("handler is nil")
	}

	r.handlers[jobType] = handler
	return nil
}

func (r *Registry) Execute(ctx context.Context, job queue.Job) error {
	jobType := strings.TrimSpace(job.Type)
	if jobType == "" {
		return errors.New("job type is empty")
	}

	handler, ok := r.handlers[jobType]
	if !ok {
		return errors.New("handler not found")
	}
	return handler.Handle(ctx, job)
}
