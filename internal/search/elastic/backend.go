package elastic

import (
	"context"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
	"github.com/CeruleanFlow/cerulean-server/internal/search"
)

// Backend is a placeholder for Elasticsearch/BM25 retrieval.
// Keep it dependency-light for the initial skeleton; replace the methods with
// calls to Elasticsearch _search and _bulk APIs when indexing is implemented.
type Backend struct {
	URL       string
	IndexName string
}

func NewBackend(url string, index string) *Backend {
	return &Backend{URL: url, IndexName: index}
}

func (b *Backend) Name() string { return "elasticsearch" }

func (b *Backend) Index(ctx context.Context, chunks []domain.Chunk) error {
	_ = ctx
	_ = chunks
	return nil
}

func (b *Backend) Search(ctx context.Context, query search.Query) ([]search.Result, error) {
	_ = ctx
	if query.Text == "" {
		return nil, nil
	}
	return []search.Result{
		{
			ChunkID: "mock-es-chunk",
			PaperID: "mock-paper",
			PageNo:  1,
			Text:    "Elasticsearch placeholder result. Replace with BM25 results.",
			Score:   1.0,
			Backend: b.Name(),
		},
	}, nil
}

func (b *Backend) DeleteByPaperID(ctx context.Context, paperID string) error {
	_ = ctx
	_ = paperID
	return nil
}
