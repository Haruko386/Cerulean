package search

import (
	"context"
	"errors"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
)

type HybridBackend struct {
	Lexical Backend
	Vector  Backend
	Fusion  Fusion
}

func NewHybridBackend(lexical Backend, vector Backend, fusion Fusion) *HybridBackend {
	return &HybridBackend{Lexical: lexical, Vector: vector, Fusion: fusion}
}

func (b *HybridBackend) Name() string { return "hybrid" }

func (b *HybridBackend) Index(ctx context.Context, chunks []domain.Chunk) error {
	var err error
	if b.Lexical != nil {
		err = errors.Join(err, b.Lexical.Index(ctx, chunks))
	}
	if b.Vector != nil {
		err = errors.Join(err, b.Vector.Index(ctx, chunks))
	}
	return err
}

func (b *HybridBackend) Search(ctx context.Context, query Query) ([]Result, error) {
	var sets [][]Result
	if b.Lexical != nil {
		results, err := b.Lexical.Search(ctx, query)
		if err == nil {
			sets = append(sets, results)
		}
	}
	if b.Vector != nil {
		results, err := b.Vector.Search(ctx, query)
		if err == nil {
			sets = append(sets, results)
		}
	}
	return b.Fusion.Fuse(query.TopK, sets...), nil
}

func (b *HybridBackend) DeleteByPaperID(ctx context.Context, paperID string) error {
	var err error
	if b.Lexical != nil {
		err = errors.Join(err, b.Lexical.DeleteByPaperID(ctx, paperID))
	}
	if b.Vector != nil {
		err = errors.Join(err, b.Vector.DeleteByPaperID(ctx, paperID))
	}
	return err
}
