package repository

import (
	"context"

	"github.com/CeruleanFlow/cerulean/internal/domain"
)

type ChunkRepository interface {
	UpsertMany(ctx context.Context, chunks []domain.Chunk) error
	List(ctx context.Context, filters map[string]string) ([]domain.Chunk, error)
	ListByPaperID(ctx context.Context, paperID string) ([]domain.Chunk, error)
	DeleteByPaperID(ctx context.Context, paperID string) error
	ReplaceByPaperID(ctx context.Context, paperID string, chunks []domain.Chunk) error
}
