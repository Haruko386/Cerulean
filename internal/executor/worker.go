package executor

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type Worker struct {
	queue    queue.Queue
	registry Registry

	batchSize   int
	blockMillis int64
	jobTimeout  time.Duration
}

type WorkerOptions struct {
	BatchSize   int
	BlockMillis int64
	JobTimeout  time.Duration
}

func NewWorker(queue queue.Queue, registry *Registry, options WorkerOptions) (*Worker, error) {
	if queue == nil {
		return nil, errors.New("queue is nil")
	}
	if registry == nil {
		return nil, errors.New("registry is nil")
	}

	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = 16
	}

	blockMillis := options.BlockMillis
	if blockMillis == 0 {
		blockMillis = 5000
	}

	jobTimeout := options.JobTimeout
	if jobTimeout == 0 {
		jobTimeout = 30 * time.Minute
	}

	return &Worker{
		queue:       queue,
		registry:    *registry,
		batchSize:   batchSize,
		blockMillis: blockMillis,
		jobTimeout:  jobTimeout,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	log.Printf(
		"executor worker started: batch_size=%d block_millis=%d job_timeout=%s",
		w.batchSize,
		w.blockMillis,
		w.jobTimeout,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, err := w.queue.DequeueBatch(ctx, w.batchSize, w.blockMillis)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Printf("dequeue batch failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		for _, msg := range messages {
			w.handleMessage(ctx, msg)
		}
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg queue.Message) {
	log.Printf(
		"start job: redis_id=%s job_id=%s task_id=%s type=%s paper_id=%s attempt=%d",
		msg.RedisID,
		msg.Job.ID,
		msg.Job.TaskID,
		msg.Job.Type,
		msg.Job.PaperID,
		msg.Job.Attempt,
	)

	jobCtx, cancel := context.WithTimeout(ctx, w.jobTimeout)
	defer cancel()

	err := w.registry.Execute(jobCtx, msg.Job)
	if err != nil {
		log.Printf(
			"job failed: redis_id=%s task_id=%s type=%s err=%v",
			msg.RedisID,
			msg.Job.TaskID,
			msg.Job.Type,
			err,
		)

		if nackErr := w.queue.Nack(context.Background(), msg, err); nackErr != nil {
			log.Printf("nack job failed: redis_id=%s task_id=%s err=%v", msg.RedisID, msg.Job.TaskID, nackErr)
		}

		return
	}

	if err := w.queue.Ack(context.Background(), msg); err != nil {
		log.Printf("ack job failed: redis_id=%s task_id=%s err=%v", msg.RedisID, msg.Job.TaskID, err)
		return
	}

	log.Printf(
		"job succeeded and acked: redis_id=%s task_id=%s type=%s",
		msg.RedisID,
		msg.Job.TaskID,
		msg.Job.Type,
	)
}
