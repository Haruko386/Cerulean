package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
	"github.com/CeruleanFlow/cerulean-server/internal/repository"
	"github.com/CeruleanFlow/cerulean-server/internal/storage"
	"github.com/CeruleanFlow/cerulean-server/internal/task"
)

type Service struct {
	papers repository.PaperRepository
	store  storage.ObjectStorage
	tasks  task.Manager
}

func NewService(papers repository.PaperRepository, store storage.ObjectStorage, tasks task.Manager) *Service {
	return &Service{papers: papers, store: store, tasks: tasks}
}

func (s *Service) StartPaperIngest(ctx context.Context, paperID string) (task.Task, error) {
	paper, err := s.papers.Get(ctx, paperID)
	if err != nil {
		return task.Task{}, err
	}
	now := time.Now()
	job := task.Task{
		ID:        fmt.Sprintf("task_%d", now.UnixNano()),
		PaperID:   paperID,
		Type:      "paper_ingest",
		Status:    task.Queued,
		Message:   "MVP mock task created. Wire PaddleOCR worker here.",
		CreatedAt: now,
		UpdatedAt: now,
	}
	paper.Status = domain.PaperProcessing
	paper.UpdatedAt = now
	if err := s.papers.Update(ctx, paper); err != nil {
		return task.Task{}, err
	}
	if err := s.tasks.Create(ctx, job); err != nil {
		return task.Task{}, err
	}
	return job, nil
}
