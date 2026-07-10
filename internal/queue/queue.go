package queue

import "context"

type Queue interface {
	Enqueue(ctx context.Context, job Job) error
	DequeueBatch(ctx context.Context, max int, blockMillis int64) ([]Message, error)
	Ack(ctx context.Context, msg Message) error
	Nack(ctx context.Context, msg Message, reason error) error
}
