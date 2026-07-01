package storage

import (
	"context"
	"io"
	"time"
)

type PutOptions struct {
	ContentType string
	Metadata    map[string]string
}

type ObjectInfo struct {
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ObjectStorage interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) (ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	PresignedGet(ctx context.Context, key string, expire time.Duration) (string, error)
}
