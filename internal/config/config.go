package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv"
)

// Config contains runtime configuration for the Cerulean API server.
type Config struct {
	HTTPAddr    string
	CORSOrigins string

	DBDriver string
	DBPath   string
	MySQLDSN string

	StorageDriver   string
	LocalStorageDir string

	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    string

	SearchDriver       string
	ElasticURL         string
	ElasticIndex       string
	ElasticUsername    string
	ElasticPassword    string
	AmaranthURL        string
	AmaranthCollection string

	LLMBaseURL string
	LLMModel   string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	QueueDriver   string
	QueueStream   string
	QueueGroup    string
	QueueConsumer string

	WorkerConcurrency int
	WorkerBatchSize   int

	WorkerJobTimeout     time.Duration
	WorkerClaimInterval  time.Duration
	WorkerClaimMinIdle   time.Duration
	WorkerClaimBatchSize int
}

func Load() Config {
	_ = godotenv.Load()

	return Config{
		HTTPAddr:    env("CERULEAN_HTTP_ADDR", ":8080"),
		CORSOrigins: env("CERULEAN_CORS_ORIGINS", "http://localhost:5173"),

		DBDriver: env("CERULEAN_DB_DRIVER", "mysql"),
		DBPath:   env("CERULEAN_DB_PATH", ".var/cerulean.json"),
		MySQLDSN: env(
			"CERULEAN_MYSQL_DSN",
			"cerulean:cerulean@tcp(127.0.0.1:3306)/cerulean?charset=utf8mb4&parseTime=True&loc=Local",
		),

		StorageDriver:   env("CERULEAN_STORAGE_DRIVER", "minio"),
		LocalStorageDir: env("CERULEAN_LOCAL_STORAGE_DIR", ".var/objects"),

		MinIOEndpoint:  env("CERULEAN_MINIO_ENDPOINT", "127.0.0.1:9000"),
		MinIOAccessKey: env("CERULEAN_MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey: env("CERULEAN_MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:    env("CERULEAN_MINIO_BUCKET", "cerulean-papers"),
		MinIOUseSSL:    env("CERULEAN_MINIO_USE_SSL", "false"),

		SearchDriver:       env("CERULEAN_SEARCH_DRIVER", "local"),
		ElasticURL:         env("CERULEAN_ELASTIC_URL", "http://127.0.0.1:9200"),
		ElasticIndex:       env("CERULEAN_ELASTIC_INDEX", "cerulean_chunks"),
		ElasticUsername:    env("CERULEAN_ELASTIC_USERNAME", ""),
		ElasticPassword:    env("CERULEAN_ELASTIC_PASSWORD", ""),
		AmaranthURL:        env("CERULEAN_AMARANTH_URL", "http://127.0.0.1:9090"),
		AmaranthCollection: env("CERULEAN_AMARANTH_COLLECTION", "papers"),

		LLMBaseURL: env("CERULEAN_LLM_BASE_URL", ""),
		LLMModel:   env("CERULEAN_LLM_MODEL", ""),

		RedisAddr:     env("CERULEAN_REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword: env("CERULEAN_REDIS_PASSWORD", ""),
		RedisDB:       envInt("CERULEAN_REDIS_DB", 0),

		QueueDriver:   env("CERULEAN_QUEUE_DRIVER", "redis"),
		QueueStream:   env("CERULEAN_QUEUE_STREAM", "cerulean_tasks"),
		QueueConsumer: env("CERULEAN_QUEUE_CONSUMER", "worker_local_1"),
		QueueGroup:    env("CERULEAN_QUEUE_GROUP", "cerulean_workers"),

		WorkerBatchSize:   envInt("CERULEAN_WORKER_BATCH_SIZE", 4),
		WorkerConcurrency: envInt("CERULEAN_WORKER_CONCURRENCY", 16),

		WorkerJobTimeout:     envDuration("CERULEAN_WORKER_JOB_TIMEOUT", 30*time.Minute),
		WorkerClaimInterval:  envDuration("CERULEAN_WORKER_CLAIM_INTERVAL", time.Minute),
		WorkerClaimMinIdle:   envDuration("CERULEAN_WORKER_CLAIM_MIN_IDLE", 35*time.Minute),
		WorkerClaimBatchSize: envInt("CERULEAN_WORKER_CLAIM_BATCH_SIZE", 16),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return i
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}
