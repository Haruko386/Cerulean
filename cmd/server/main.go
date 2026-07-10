package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/dao"
	"github.com/CeruleanFlow/cerulean/internal/queue"

	"github.com/CeruleanFlow/cerulean/internal/api"
	"github.com/CeruleanFlow/cerulean/internal/config"
	"github.com/CeruleanFlow/cerulean/internal/ingest"
	"github.com/CeruleanFlow/cerulean/internal/rag"
	"github.com/CeruleanFlow/cerulean/internal/repository"
	"github.com/CeruleanFlow/cerulean/internal/search"
	"github.com/CeruleanFlow/cerulean/internal/storage"
	"github.com/CeruleanFlow/cerulean/internal/task"

	docparser "github.com/CeruleanFlow/cerulean/internal/parser"
)

func main() {
	cfg := config.Load()

	paperRepo, chunkRepo, userDAO, err := buildRepositories(cfg)
	if err != nil {
		log.Fatal(err)
	}
	objectStore, err := buildObjectStorage(cfg)
	if err != nil {
		log.Fatal(err)
	}
	taskManager := task.NewMemoryManager()
	documentParser := docparser.NewPDFTextParser()
	searchBackend, err := buildSearchBackend(cfg)
	if err != nil {
		log.Fatal(err)
	}
	jobQueue, err := buildJobQueue(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if closer, ok := jobQueue.(interface{ Close() error }); ok {
		defer func() {
			if err := closer.Close(); err != nil {
				log.Printf("job queue close error: %v\n", err)
			}
		}()
	}

	ragService := rag.NewService(paperRepo, searchBackend)
	ingestService := ingest.NewService(paperRepo, chunkRepo, objectStore, taskManager, searchBackend, documentParser, jobQueue)

	router := api.NewRouter(api.RouterOptions{
		Config:        cfg,
		PaperRepo:     paperRepo,
		ChunkRepo:     chunkRepo,
		UserDAO:       userDAO,
		TaskManager:   taskManager,
		ObjectStore:   objectStore,
		IngestService: ingestService,
		RAGService:    ragService,
	})

	log.Printf("Cerulean server listening on %s", cfg.HTTPAddr)
	log.Printf("database=%s storage=%s search=%s", cfg.DBDriver, cfg.StorageDriver, cfg.SearchDriver)
	if err := http.ListenAndServe(cfg.HTTPAddr, router); err != nil {
		log.Fatal(err)
	}
}

func buildRepositories(cfg config.Config) (repository.PaperRepository, repository.ChunkRepository, *dao.UserDAO, error) {
	switch strings.ToLower(cfg.DBDriver) {
	case "", "mysql":
		database, err := dao.NewMySQLDatabase(cfg.MySQLDSN)
		if err != nil {
			return nil, nil, nil, err
		}
		return database.Papers, database.Chunks, database.Users, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported CERULEAN_DB_DRIVER=%q; supported: mysql, json, memory", cfg.DBDriver)
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

func buildJobQueue(cfg config.Config) (queue.Queue, error) {
	switch strings.ToLower(cfg.QueueDriver) {
	case "", "redis":
		return queue.NewRedisStreamQueue(context.Background(), queue.RedisStreamConfig{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
			Stream:   cfg.QueueStream,
			Group:    cfg.QueueGroup,
			Consumer: cfg.QueueConsumer,
		})

	default:
		return nil, fmt.Errorf("unsupported CERULEAN_QUEUE_DRIVER=%q; supported: redis", cfg.QueueDriver)
	}
}
