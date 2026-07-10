package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/config"
	"github.com/CeruleanFlow/cerulean/internal/dao"
	"github.com/CeruleanFlow/cerulean/internal/executor"
	"github.com/CeruleanFlow/cerulean/internal/ingest"
	docparser "github.com/CeruleanFlow/cerulean/internal/parser"
	"github.com/CeruleanFlow/cerulean/internal/pipeline"
	"github.com/CeruleanFlow/cerulean/internal/queue"
	"github.com/CeruleanFlow/cerulean/internal/search"
	"github.com/CeruleanFlow/cerulean/internal/storage"
	"github.com/CeruleanFlow/cerulean/internal/task"
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

	database, err := dao.NewMySQLDatabase(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}

	objectStore, err := buildObjectStorage(cfg)
	if err != nil {
		log.Fatalf("create object storage: %v", err)
	}

	searchBackend, err := buildSearchBackend(cfg)
	if err != nil {
		log.Fatalf("create search backend: %v", err)
	}

	taskManager := task.NewMemoryManager()

	documentParser := docparser.NewPDFTextParser()

	ingestService := ingest.NewService(
		database.Papers,
		database.Chunks,
		objectStore,
		taskManager,
		searchBackend,
		documentParser,
		nil,
	)

	registry := executor.NewRegistry()

	if err := registry.Register(queue.JobTypePaperIngest, pipeline.NewPaperIngestHandler(ingestService)); err != nil {
		log.Fatalf("register paper ingest handler: %v", err)
	}

	if err := registry.Register(queue.JobTypePaperReindex, pipeline.NewPaperReindexHandler(ingestService)); err != nil {
		log.Fatalf("register paper reindex handler: %v", err)
	}

	worker, err := executor.NewWorker(q, registry, executor.WorkerOptions{
		BatchSize:   cfg.WorkerBatchSize,
		BlockMillis: 5000,
		JobTimeout:  30 * time.Minute,
		Concurrency: cfg.WorkerConcurrency,
	})
	if err != nil {
		log.Fatalf("create executor worker: %v", err)
	}

	log.Printf(
		"Cerulean worker started: redis=%s stream=%s group=%s consumer=%s batch_size=%d concurrency=%d",
		cfg.RedisAddr,
		cfg.QueueStream,
		cfg.QueueGroup,
		cfg.QueueConsumer,
		cfg.WorkerBatchSize,
		cfg.WorkerConcurrency,
	)

	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("worker failed: %v", err)
	}
}

func buildObjectStorage(cfg config.Config) (storage.ObjectStorage, error) {
	switch strings.ToLower(cfg.StorageDriver) {
	case "", "local":
		return storage.NewLocalObjectStorage(cfg.LocalStorageDir)

	case "minio":
		useSSL := strings.EqualFold(cfg.MinIOUseSSL, "true")
		return storage.NewMinIOObjectStorage(context.Background(), storage.MinIOConfig{
			Endpoint:  cfg.MinIOEndpoint,
			AccessKey: cfg.MinIOAccessKey,
			SecretKey: cfg.MinIOSecretKey,
			Bucket:    cfg.MinIOBucket,
			UseSSL:    useSSL,
		})

	default:
		return nil, fmt.Errorf("unsupported CERULEAN_STORAGE_DRIVER=%q; supported: local, minio", cfg.StorageDriver)
	}
}

func buildSearchBackend(cfg config.Config) (search.Backend, error) {
	switch strings.ToLower(cfg.SearchDriver) {
	//case "", "local":
	//	return search.NewLocalBackend(), nil

	case "elastic", "elasticsearch", "es":
		backend, err := search.NewElasticBackend(context.Background(), search.ElasticConfig{
			URL:      cfg.ElasticURL,
			Index:    cfg.ElasticIndex,
			Username: cfg.ElasticUsername,
			Password: cfg.ElasticPassword,
		})
		if err != nil {
			return nil, err
		}
		if backend == nil {
			return nil, fmt.Errorf("elastic backend constructor returned nil")
		}
		return backend, nil

	default:
		return nil, fmt.Errorf("unsupported CERULEAN_SEARCH_DRIVER=%q; supported: local, elastic", cfg.SearchDriver)
	}
}
