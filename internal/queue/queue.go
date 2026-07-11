package queue

import (
	"context"
	"time"
)

type Queue interface {
	Enqueue(ctx context.Context, job Job) error
	DequeueBatch(ctx context.Context, max int, blockMillis int64) ([]Message, error)
	ClaimPending(ctx context.Context, start string, minIdle time.Duration, max int) (messages []Message, nextStart string, err error)
	Ack(ctx context.Context, msg Message) error
	Nack(ctx context.Context, msg Message, reason error) error
}
