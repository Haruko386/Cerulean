package config

import (
	"os"

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
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
