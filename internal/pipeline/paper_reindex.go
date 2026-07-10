package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/ingest"
	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type PaperReindexHandler struct {
	ingest *ingest.Service
}

func NewPaperReindexHandler(ingest *ingest.Service) *PaperReindexHandler {
	return &PaperReindexHandler{
		ingest: ingest,
	}
}

func (h *PaperReindexHandler) Handle(ctx context.Context, job queue.Job) error {
	if h.ingest == nil {
		return fmt.Errorf("ingest service is nil")
	}

	taskID := strings.TrimSpace(job.TaskID)
	paperID := strings.TrimSpace(job.PaperID)
	if taskID == "" {
		return fmt.Errorf("task id is empty")
	}
	if paperID == "" {
		return fmt.Errorf("paper id is empty")
	}

	return h.ingest.ProcessPaperReindex(ctx, paperID, taskID)
}
