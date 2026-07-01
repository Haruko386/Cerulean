package rag

import (
	"context"
	"fmt"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
	"github.com/CeruleanFlow/cerulean-server/internal/repository"
	"github.com/CeruleanFlow/cerulean-server/internal/search"
)

type Service struct {
	papers  repository.PaperRepository
	backend search.Backend
}

func NewService(papers repository.PaperRepository, backend search.Backend) *Service {
	return &Service{papers: papers, backend: backend}
}

func (s *Service) Search(ctx context.Context, req domain.SearchRequest) (domain.SearchResponse, error) {
	if req.TopK <= 0 {
		req.TopK = 5
	}
	results, err := s.backend.Search(ctx, search.Query{Text: req.Query, TopK: req.TopK, Filters: req.Filters})
	if err != nil {
		return domain.SearchResponse{}, err
	}
	return domain.SearchResponse{Query: req.Query, Results: toSources(results)}, nil
}

func (s *Service) Chat(ctx context.Context, req domain.ChatRequest) (domain.ChatResponse, error) {
	if req.TopK <= 0 {
		req.TopK = 5
	}
	results, err := s.backend.Search(ctx, search.Query{Text: req.Question, TopK: req.TopK, Filters: req.Filters})
	if err != nil {
		return domain.ChatResponse{}, err
	}
	sources := toSources(results)
	answer := fmt.Sprintf("MVP mock answer for: %q. Wire LLM completion after retrieval is ready.", req.Question)
	return domain.ChatResponse{Answer: answer, Sources: sources}, nil
}

func toSources(results []search.Result) []domain.Source {
	sources := make([]domain.Source, 0, len(results))
	for _, result := range results {
		sources = append(sources, domain.Source{
			ChunkID: result.ChunkID,
			PaperID: result.PaperID,
			PageNo:  result.PageNo,
			Text:    result.Text,
			Score:   result.Score,
		})
	}
	return sources
}
