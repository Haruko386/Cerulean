package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/ingest"
	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type PaperIngestHandler struct {
	ingest *ingest.Service
}

func NewPaperIngestHandler(ingest *ingest.Service) *PaperIngestHandler {
	return &PaperIngestHandler{
		ingest: ingest,
	}
}

func (h *PaperIngestHandler) Handle(ctx context.Context, job queue.Job) error {
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

	return h.ingest.ProcessPaperReindex(ctx, taskID, paperID)
}
