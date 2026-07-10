package pipeline

import (
	"context"
	"log"

	"github.com/CeruleanFlow/cerulean/internal/queue"
)

type LogJobHandler struct{}

func NewLogJobHandler() *LogJobHandler {
	return &LogJobHandler{}
}

func (h *LogJobHandler) Handle(ctx context.Context, job queue.Job) error {
	log.Printf(
		"pipeline log handler: job_id=%s task_id=%s type=%s paper_id=%s attempt=%d",
		job.ID,
		job.TaskID,
		job.Type,
		job.PaperID,
		job.Attempt,
	)

	return nil
}
