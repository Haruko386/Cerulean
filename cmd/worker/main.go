package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/config"
	"github.com/CeruleanFlow/cerulean/internal/executor"
	"github.com/CeruleanFlow/cerulean/internal/pipeline"
	"github.com/CeruleanFlow/cerulean/internal/queue"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, err := queue.NewRedisStreamQueue(ctx, queue.RedisStreamConfig{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		Stream:   cfg.QueueStream,
		Group:    cfg.QueueGroup,
		Consumer: cfg.QueueConsumer,
	})
	if err != nil {
		log.Fatalf("create redis queue: %v", err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			log.Printf("close redis queue: %v", err)
		}
	}()

	registry := executor.NewRegistry()

	logHandler := pipeline.NewLogJobHandler()

	if err := registry.Register(queue.JobTypePaperIngest, logHandler); err != nil {
		log.Fatalf("register paper ingest handler: %v", err)
	}

	if err := registry.Register(queue.JobTypePaperReindex, logHandler); err != nil {
		log.Fatalf("register paper reindex handler: %v", err)
	}

	worker, err := executor.NewWorker(q, registry, executor.WorkerOptions{
		BatchSize:   cfg.WorkerBatchSize,
		BlockMillis: 5000,
		JobTimeout:  10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("create executor worker: %v", err)
	}

	log.Printf(
		"Cerulean worker started: redis=%s stream=%s group=%s consumer=%s batch_size=%d",
		cfg.RedisAddr,
		cfg.QueueStream,
		cfg.QueueGroup,
		cfg.QueueConsumer,
		cfg.WorkerBatchSize,
	)

	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("worker failed: %v", err)
	}
}
