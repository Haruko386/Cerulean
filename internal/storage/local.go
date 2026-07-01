package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type LocalObjectStorage struct {
	root string
}

func NewLocalObjectStorage(root string) *LocalObjectStorage {
	return &LocalObjectStorage{root: root}
}

func (s *LocalObjectStorage) Put(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) (ObjectInfo, error) {
	_ = ctx
	path, err := s.safePath(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return ObjectInfo{}, err
	}
	file, err := os.Create(path)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer file.Close()
	written, err := io.Copy(file, r)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: key, Size: written, ContentType: opts.ContentType, Metadata: opts.Metadata}, nil
}

func (s *LocalObjectStorage) Get(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	_ = ctx
	path, err := s.safePath(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, ObjectInfo{}, err
	}
	return file, ObjectInfo{Key: key, Size: stat.Size()}, nil
}

func (s *LocalObjectStorage) Delete(ctx context.Context, key string) error {
	_ = ctx
	path, err := s.safePath(key)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (s *LocalObjectStorage) PresignedGet(ctx context.Context, key string, expire time.Duration) (string, error) {
	_ = ctx
	values := url.Values{}
	values.Set("key", key)
	values.Set("expire", expire.String())
	return "/api/v1/objects/local?" + values.Encode(), nil
}

func (s *LocalObjectStorage) safePath(key string) (string, error) {
	clean := filepath.Clean(key)
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe object key: %s", key)
	}
	return filepath.Join(s.root, clean), nil
}
