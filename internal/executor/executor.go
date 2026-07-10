package executor

import (
	"context"

	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type Handler interface {
	Handle(ctx context.Context, job queue.Job) error
}

type HandlerFunc func(ctx context.Context, job queue.Job) error

func (f HandlerFunc) Handle(ctx context.Context, job queue.Job) error {
	return f(ctx, job)
}
