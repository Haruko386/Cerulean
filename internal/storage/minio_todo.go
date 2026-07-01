package storage

// TODO: Add a real MinIO adapter in v0.2.
//
// Recommended implementation:
//   go get github.com/minio/minio-go/v7
//
// type MinIOObjectStorage struct { ... }
//
// func NewMinIOObjectStorage(cfg config.Config) (*MinIOObjectStorage, error) {
//     client, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
//         Creds: credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
//         Secure: cfg.MinIOUseSSL == "true",
//     })
//     ...
// }
//
// Keep the ObjectStorage interface unchanged so the rest of the RAG pipeline
// does not care whether objects live in MinIO, S3, R2, OSS, or the local dev
// fallback.
