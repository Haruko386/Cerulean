package search

import (
	"context"
	"sort"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
)

type Query struct {
	Text    string
	Vector  []float32
	TopK    int
	Filters map[string]string
}

type Result struct {
	ChunkID string
	PaperID string
	PageNo  int
	Text    string
	Score   float64
	Backend string
}

type Backend interface {
	Name() string
	Index(ctx context.Context, chunks []domain.Chunk) error
	Search(ctx context.Context, query Query) ([]Result, error)
	DeleteByPaperID(ctx context.Context, paperID string) error
}

type Fusion interface {
	Fuse(topK int, resultSets ...[]Result) []Result
}

type RRFusion struct {
	K float64
}

func NewRRFusion(k float64) RRFusion {
	return RRFusion{K: k}
}

func (f RRFusion) Fuse(topK int, resultSets ...[]Result) []Result {
	scores := map[string]Result{}
	for _, results := range resultSets {
		for rank, result := range results {
			key := result.ChunkID
			if key == "" {
				key = result.PaperID + ":" + result.Text
			}
			existing := scores[key]
			if existing.ChunkID == "" {
				existing = result
				existing.Score = 0
			}
			existing.Score += 1.0 / (f.K + float64(rank+1))
			scores[key] = existing
		}
	}
	merged := make([]Result, 0, len(scores))
	for _, result := range scores {
		merged = append(merged, result)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Score > merged[j].Score })
	if topK > 0 && len(merged) > topK {
		merged = merged[:topK]
	}
	return merged
}
