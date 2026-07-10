package executor

import (
	"context"
	"errors"
	"log"
	"time"

	celestial "github.com/Haruko386/Celestial"

	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type Worker struct {
	queue    queue.Queue
	registry Registry

	batchSize   int
	blockMillis int64
	jobTimeout  time.Duration
	concurrency int
}

type WorkerOptions struct {
	BatchSize   int
	BlockMillis int64
	JobTimeout  time.Duration
	Concurrency int
}

type JobResult struct {
	RedisID string
	TaskID  string
	Type    string
	PaperID string
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

	concurrency := options.Concurrency
	if concurrency == 0 {
		concurrency = 4
	}

	return &Worker{
		queue:       queue,
		registry:    *registry,
		batchSize:   batchSize,
		blockMillis: blockMillis,
		jobTimeout:  jobTimeout,
		concurrency: concurrency,
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

		if err := w.handleBatch(ctx, messages); err != nil {
			log.Printf("handle batch finished with error: %v", err)
		}
	}
}

func (w *Worker) handleBatch(ctx context.Context, messages []queue.Message) error {
	if len(messages) == 0 {
		return nil
	}

	dispatcher := celestial.New[queue.Message, JobResult](celestial.Config{
		Workers:     w.concurrency,
		QueueSize:   w.batchSize,
		StopOnError: false,
	})

	run := dispatcher.RunSlice(
		ctx,
		messages,
		func(ctx context.Context, worker celestial.Worker, msg queue.Message) (JobResult, error) {
			log.Printf(
				"celestial worker=%d picked job: redis_id=%s task_id=%s type=%s",
				worker.Index,
				msg.RedisID,
				msg.Job.TaskID,
				msg.Job.Type,
			)

			return w.handleMessage(ctx, msg)
		},
	)

	for result := range run.Results() {
		if result.Err != nil {
			log.Printf(
				"celestial job failed: worker=%d task_index=%d err=%v",
				result.WorkerIndex,
				result.TaskID,
				result.Err,
			)
			continue
		}

		log.Printf(
			"celestial job done: worker=%d task_index=%d redis_id=%s task_id=%s type=%s paper_id=%s",
			result.WorkerIndex,
			result.TaskID,
			result.Value.RedisID,
			result.Value.TaskID,
			result.Value.Type,
			result.Value.PaperID,
		)
	}

	if err := run.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Worker) handleMessage(ctx context.Context, msg queue.Message) (JobResult, error) {
	result := JobResult{
		RedisID: msg.RedisID,
		TaskID:  msg.Job.TaskID,
		Type:    msg.Job.Type,
		PaperID: msg.Job.PaperID,
	}

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

		nackCtx, cancel := context.WithTimeout(context.Background(), w.jobTimeout)
		defer cancel()

		if nackErr := w.queue.Nack(nackCtx, msg, err); nackErr != nil {
			log.Printf("nack job failed: redis_id=%s task_id=%s err=%v", msg.RedisID, msg.Job.TaskID, nackErr)
		}

		return result, err
	}

	ackCtx, cancel := context.WithTimeout(context.Background(), w.jobTimeout)
	defer cancel()

	if err := w.queue.Ack(ackCtx, msg); err != nil {
		log.Printf("ack job failed: redis_id=%s task_id=%s err=%v", msg.RedisID, msg.Job.TaskID, err)
		return result, err
	}

	log.Printf(
		"job succeeded and acked: redis_id=%s task_id=%s type=%s",
		msg.RedisID,
		msg.Job.TaskID,
		msg.Job.Type,
	)
	return result, nil
}
