package config

import "os"

// Config contains runtime configuration for the Cerulean API server.
type Config struct {
	HTTPAddr           string
	CORSOrigins        string
	StorageDriver      string
	LocalStorageDir    string
	MinIOEndpoint      string
	MinIOAccessKey     string
	MinIOSecretKey     string
	MinIOBucket        string
	MinIOUseSSL        string
	ElasticURL         string
	ElasticIndex       string
	AmaranthURL        string
	AmaranthCollection string
	LLMBaseURL         string
	LLMModel           string
}

func Load() Config {
	return Config{
		HTTPAddr:           env("CERULEAN_HTTP_ADDR", ":8080"),
		CORSOrigins:        env("CERULEAN_CORS_ORIGINS", "http://localhost:5173"),
		StorageDriver:      env("CERULEAN_STORAGE_DRIVER", "local"),
		LocalStorageDir:    env("CERULEAN_LOCAL_STORAGE_DIR", ".var/objects"),
		MinIOEndpoint:      env("CERULEAN_MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:     env("CERULEAN_MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey:     env("CERULEAN_MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:        env("CERULEAN_MINIO_BUCKET", "cerulean-papers"),
		MinIOUseSSL:        env("CERULEAN_MINIO_USE_SSL", "false"),
		ElasticURL:         env("CERULEAN_ELASTIC_URL", "http://localhost:9200"),
		ElasticIndex:       env("CERULEAN_ELASTIC_INDEX", "cerulean_chunks"),
		AmaranthURL:        env("CERULEAN_AMARANTH_URL", "http://localhost:9090"),
		AmaranthCollection: env("CERULEAN_AMARANTH_COLLECTION", "papers"),
		LLMBaseURL:         env("CERULEAN_LLM_BASE_URL", ""),
		LLMModel:           env("CERULEAN_LLM_MODEL", ""),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
