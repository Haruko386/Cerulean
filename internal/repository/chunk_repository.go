package repository

import (
	"context"
	"sort"
	"sync"

	"github.com/CeruleanFlow/cerulean/internal/domain"
)

type MemoryChunkRepository struct {
	mu     sync.RWMutex
	chunks map[string]domain.Chunk
}

func NewMemoryChunkRepository() *MemoryChunkRepository {
	return &MemoryChunkRepository{chunks: make(map[string]domain.Chunk)}
}

func (r *MemoryChunkRepository) UpsertMany(ctx context.Context, chunks []domain.Chunk) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, chunk := range chunks {
		r.chunks[chunk.ID] = chunk
	}
	return nil
}

func (r *MemoryChunkRepository) List(ctx context.Context, filters map[string]string) ([]domain.Chunk, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]domain.Chunk, 0, len(r.chunks))
	for _, chunk := range r.chunks {
		if matchChunkFilters(chunk, filters) {
			items = append(items, chunk)
		}
	}
	sortChunks(items)
	return items, nil
}

func (r *MemoryChunkRepository) ListByPaperID(ctx context.Context, paperID string) ([]domain.Chunk, error) {
	return r.List(ctx, map[string]string{"paper_id": paperID})
}

func (r *MemoryChunkRepository) DeleteByPaperID(ctx context.Context, paperID string) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, chunk := range r.chunks {
		if chunk.PaperID == paperID {
			delete(r.chunks, id)
		}
	}
	return nil
}

func matchChunkFilters(chunk domain.Chunk, filters map[string]string) bool {
	if filters == nil {
		return true
	}
	if paperID := filters["paper_id"]; paperID != "" && chunk.PaperID != paperID {
		return false
	}
	return true
}

func sortChunks(items []domain.Chunk) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].PaperID != items[j].PaperID {
			return items[i].PaperID < items[j].PaperID
		}
		if items[i].PageNo != items[j].PageNo {
			return items[i].PageNo < items[j].PageNo
		}
		return items[i].Index < items[j].Index
	})
}
