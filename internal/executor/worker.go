package executor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
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

	claimInterval  time.Duration
	claimMinIdle   time.Duration
	claimBatchSize int
	claimCursor    string
}

type WorkerOptions struct {
	BatchSize   int
	BlockMillis int64
	JobTimeout  time.Duration
	Concurrency int

	ClaimInterval  time.Duration
	ClaimMinIdle   time.Duration
	ClaimBatchSize int
}

type JobResult struct {
	RedisID string
	TaskID  string
	Type    string
	PaperID string
}

// NewWorker creates a configured task executor worker.
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

	claimInterval := options.ClaimInterval
	if claimInterval == 0 {
		claimInterval = time.Minute
	}

	claimMinIdle := options.ClaimMinIdle
	if claimMinIdle == 0 {
		claimMinIdle = jobTimeout + 5*time.Minute
	}

	claimBatchSize := options.ClaimBatchSize
	if claimBatchSize == 0 {
		claimBatchSize = batchSize
	}

	if claimMinIdle < jobTimeout {
		return nil, errors.New("claim minimum time limit exceeded")
	}

	return &Worker{
		queue:          queue,
		registry:       *registry,
		batchSize:      batchSize,
		blockMillis:    blockMillis,
		jobTimeout:     jobTimeout,
		concurrency:    concurrency,
		claimInterval:  claimInterval,
		claimMinIdle:   claimMinIdle,
		claimBatchSize: claimBatchSize,
		claimCursor:    "0-0",
	}, nil
}

// Run starts the worker loop and processes queued jobs.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf(
		"executor worker started: batch_size=%d concurrency=%d job_timeout=%s claim_interval=%s claim_min_idle=%s",
		w.batchSize,
		w.concurrency,
		w.jobTimeout,
		w.claimInterval,
		w.claimMinIdle,
	)

	lastClaimAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Println("executor worker stopping")
			return ctx.Err()
		default:
		}

		if lastClaimAt.IsZero() || time.Since(lastClaimAt) >= w.claimInterval {
			if err := w.recoverPending(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				log.Printf("recover pending jobs failed: %v", err)
			}
			lastClaimAt = time.Now()
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

// handleBatch processes a batch of messages concurrently.
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

// handleMessage executes and acknowledges a single queue message.
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

// recoverPending claims and processes idle pending messages.
func (w *Worker) recoverPending(ctx context.Context) error {
	messages, nextStart, err := w.queue.ClaimPending(ctx, w.claimCursor, w.claimMinIdle, w.claimBatchSize)
	if err != nil {
		return err
	}

	w.claimCursor = nextStart
	if strings.TrimSpace(nextStart) == "" {
		w.claimCursor = "0-0"
	}

	if len(messages) == 0 {
		return nil
	}

	log.Printf(
		"claimed pending jobs: count=%d next_start=%s min_idle=%s",
		len(messages),
		w.claimCursor,
		w.claimMinIdle,
	)

	if err := w.handleBatch(ctx, messages); err != nil {
		return fmt.Errorf("handle claimed pending batch: %w", err)
	}
	return nil
}
