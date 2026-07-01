package amaranth

import (
	"context"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
	"github.com/CeruleanFlow/cerulean-server/internal/search"
)

// Backend is the future HTTP/gRPC client for the C++ Amaranth vector engine.
type Backend struct {
	URL        string
	Collection string
}

func NewBackend(url string, collection string) *Backend {
	return &Backend{URL: url, Collection: collection}
}

func (b *Backend) Name() string { return "amaranth" }

func (b *Backend) Index(ctx context.Context, chunks []domain.Chunk) error {
	_ = ctx
	_ = chunks
	return nil
}

func (b *Backend) Search(ctx context.Context, query search.Query) ([]search.Result, error) {
	_ = ctx
	if query.Text == "" && len(query.Vector) == 0 {
		return nil, nil
	}
	return []search.Result{
		{
			ChunkID: "mock-amaranth-chunk",
			PaperID: "mock-paper",
			PageNo:  2,
			Text:    "Amaranth placeholder result. Replace with vector topK results.",
			Score:   0.9,
			Backend: b.Name(),
		},
	}, nil
}

func (b *Backend) DeleteByPaperID(ctx context.Context, paperID string) error {
	_ = ctx
	_ = paperID
	return nil
}
