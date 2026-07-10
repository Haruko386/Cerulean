package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/domain"
	"github.com/CeruleanFlow/cerulean/internal/queue"
	"github.com/CeruleanFlow/cerulean/internal/repository"
	"github.com/CeruleanFlow/cerulean/internal/search"
	"github.com/CeruleanFlow/cerulean/internal/storage"
	"github.com/CeruleanFlow/cerulean/internal/task"

	docparser "github.com/CeruleanFlow/cerulean/internal/parser"
)

type Service struct {
	papers repository.PaperRepository
	chunks repository.ChunkRepository
	store  storage.ObjectStorage
	search search.Backend
	tasks  task.Manager
	parser docparser.Parser

	jobQueue queue.Queue
}

func NewService(
	papers repository.PaperRepository,
	chunks repository.ChunkRepository,
	store storage.ObjectStorage,
	tasks task.Manager,
	searchBackend search.Backend,
	parser docparser.Parser,
	jobQueue queue.Queue,
) *Service {
	return &Service{
		papers:   papers,
		chunks:   chunks,
		store:    store,
		tasks:    tasks,
		search:   searchBackend,
		parser:   parser,
		jobQueue: jobQueue,
	}
}

func (s *Service) StartPaperIngest(ctx context.Context, paperID string) (task.Task, error) {
	paperID = strings.TrimSpace(paperID)
	if paperID == "" {
		return task.Task{}, fmt.Errorf("paper id is empty")
	}
	if s.jobQueue == nil {
		return task.Task{}, fmt.Errorf("job queue is empty")
	}

	paper, err := s.papers.Get(ctx, paperID)
	if err != nil {
		return task.Task{}, err
	}

	optCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	now := time.Now()
	job := task.Task{
		ID:        fmt.Sprintf("task_%d", now.UnixNano()),
		PaperID:   paperID,
		Type:      "paper_ingest",
		Status:    task.Queued,
		Message:   "queued PDF text ingestion; PaddleOCR will be used later for scanned PDFs",
		CreatedAt: now,
		UpdatedAt: now,
	}
	paper.Status = domain.PaperProcessing
	paper.Error = ""
	paper.UpdatedAt = now

	if err := s.papers.Update(optCtx, paper); err != nil {
		return task.Task{}, err
	}
	if err := s.tasks.Create(optCtx, job); err != nil {
		return task.Task{}, err
	}

	redisJob := queue.Job{
		ID:        job.ID,
		TaskID:    job.ID,
		Type:      queue.JobTypePaperIngest,
		PaperID:   paper.ID,
		Attempt:   0,
		CreatedAt: now,
	}

	if err := s.jobQueue.Enqueue(optCtx, redisJob); err != nil {
		s.fail(context.Background(), job, paper, err)
		return task.Task{}, fmt.Errorf("enqueue ingest job: %w", err)
	}

	return job, nil
}

func (s *Service) StartPaperReindex(ctx context.Context, paperID string) (task.Task, error) {
	paperID = strings.TrimSpace(paperID)
	if paperID == "" {
		return task.Task{}, fmt.Errorf("paper id is empty")
	}
	if s.jobQueue == nil {
		return task.Task{}, fmt.Errorf("job queue is empty")
	}

	if _, err := s.papers.Get(ctx, paperID); err != nil {
		return task.Task{}, fmt.Errorf("paper not found: %w", err)
	}

	optCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	now := time.Now()

	job := task.Task{
		ID:        fmt.Sprintf("task_%d", now.UnixNano()),
		PaperID:   paperID,
		Type:      queue.JobTypePaperReindex,
		Status:    task.Queued,
		Message:   "queued paper reindex",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.tasks.Create(optCtx, job); err != nil {
		return task.Task{}, fmt.Errorf("create paper reindex: %w", err)
	}

	redisJob := queue.Job{
		ID:        job.ID,
		TaskID:    job.ID,
		Type:      queue.JobTypePaperReindex,
		PaperID:   paperID,
		Attempt:   0,
		CreatedAt: now,
	}

	if err := s.jobQueue.Enqueue(optCtx, redisJob); err != nil {
		s.failTaskOnly(ctx, job, err)
		return task.Task{}, fmt.Errorf("enqueue paper reindex: %w", err)
	}
	return job, nil
}

func (s *Service) runPDFTextIngest(ctx context.Context, job task.Task, paper domain.Paper) error {
	// precheck
	if s.parser == nil {
		return fmt.Errorf("document parser is not initialized")
	}

	now := time.Now()
	job.Status = task.Running
	job.Message = "parsing PDF text"
	job.UpdatedAt = now
	_ = s.tasks.Update(ctx, job)

	// download the file to local
	pdfPath, cleanup, err := s.downloadOriginalPDF(ctx, paper)
	if err != nil {
		return err
	}
	defer cleanup()

	// parse the file
	doc, err := s.parser.ParseFile(ctx, pdfPath)
	if err != nil {
		return fmt.Errorf("parse pdf text: %w", err)
	}

	// generate the markdown
	markdown := parsedMarkdown(paper, doc)
	artifactKey := fmt.Sprintf("papers/%s/parsed/document.md", paper.ID)

	// save the markdown to MinIO
	if _, err := s.store.Put(
		ctx,
		artifactKey,
		bytes.NewReader([]byte(markdown)),
		int64(len(markdown)),
		storage.PutOptions{ContentType: "text/markdown; charset=utf-8"},
	); err != nil {
		return fmt.Errorf("store parsed markdown: %w", err)
	}

	// generate the chunk
	chunks := docparser.BuildChunks(paper, artifactKey, doc, 1200, 150)
	if len(chunks) == 0 {
		return fmt.Errorf("pdf text parser produced no chunks")
	}

	if err := s.chunks.ReplaceByPaperID(ctx, paper.ID, chunks); err != nil {
		return fmt.Errorf("replace chunks in mysql: %w", err)
	}

	if s.search != nil {
		if err := s.search.DeleteByPaperID(ctx, paper.ID); err != nil {
			return fmt.Errorf("delete old chunks from elasticsearch: %w", err)
		}

		if err := s.search.IndexChunks(ctx, chunks); err != nil {
			return fmt.Errorf("index chunks to elasticsearch: %w", err)
		}
	}

	// update paper / task status
	now = time.Now()

	paper.Status = domain.PaperParsed
	paper.UpdatedAt = now
	paper.Error = ""
	paper.PageCount = doc.PageCount

	if err := s.papers.Update(ctx, paper); err != nil {
		return fmt.Errorf("update paper status: %w", err)
	}

	job.Status = task.Succeeded
	job.Message = fmt.Sprintf("PDF text ingestion completed: %d pages, %d chunks", doc.PageCount, len(chunks))
	job.UpdatedAt = now

	if err := s.tasks.Update(ctx, job); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}

	return nil
}

// fail set failed status for paper and task
func (s *Service) fail(ctx context.Context, job task.Task, paper domain.Paper, err error) {
	opCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now()

	paper.Status = domain.PaperFailed
	paper.Error = err.Error()
	paper.UpdatedAt = now
	_ = s.papers.Update(opCtx, paper)

	job.Status = task.Failed
	job.Message = err.Error()
	job.UpdatedAt = now
	_ = s.tasks.Update(opCtx, job)
}

func (s *Service) failTaskOnly(ctx context.Context, job task.Task, err error) {
	opCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now()
	job.Status = task.Failed
	job.Message = err.Error()
	job.UpdatedAt = now

	_ = s.tasks.Update(opCtx, job)
}

// downloadOriginalPDF download original PDF to tmp
func (s *Service) downloadOriginalPDF(ctx context.Context, paper domain.Paper) (string, func(), error) {
	if s.store == nil {
		return "", nil, fmt.Errorf("object storage is not initialized")
	}

	if err := os.MkdirAll(".var/temp", 0755); err != nil {
		return "", nil, fmt.Errorf("create tmp dir: %w", err)
	}

	reader, _, err := s.store.Get(ctx, paper.ObjectKey)
	if err != nil {
		return "", nil, fmt.Errorf("get original pdf from storage: %w", err)
	}
	defer reader.Close()

	tmp, err := os.CreateTemp(".var/tmp", "ingest-*.pdf")
	if err != nil {
		return "", nil, fmt.Errorf("create ingest temp file: %w", err)
	}

	tmpPath := tmp.Name()
	cleanup := func() {
		_ = os.Remove(tmpPath)
	}

	if _, err := io.Copy(tmp, reader); err != nil {
		_ = os.Remove(tmpPath)
		cleanup()
		return "", nil, fmt.Errorf("copy pdf to temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}
	return tmpPath, cleanup, nil
}

func parsedMarkdown(paper domain.Paper, doc docparser.Document) string {
	var b strings.Builder

	b.WriteString("# ")
	b.WriteString(paper.Title)
	b.WriteString("\n\n")

	b.WriteString("<!-- Generated by Cerulean PDF text parser. -->\n\n")

	for _, page := range doc.Pages {
		b.WriteString(fmt.Sprintf("## Page %d\n\n", page.PageNo))
		b.WriteString(strings.TrimSpace(page.Text))
		b.WriteString("\n\n")
	}

	return b.String()
}

func (s *Service) ReindexPaper(ctx context.Context, paperID string) error {
	if s.search == nil {
		return fmt.Errorf("search backend is not initialized")
	}

	paperID = strings.TrimSpace(paperID)
	if paperID == "" {
		return fmt.Errorf("paperID cannot be empty")
	}

	// get chunks
	chunks, err := s.chunks.ListByPaperID(ctx, paperID)
	if err != nil {
		return fmt.Errorf("get chunks by paperID: %w", err)
	}

	if len(chunks) == 0 {
		return fmt.Errorf("paper %s has no chunks", paperID)
	}

	// delete first, update next
	if err := s.search.DeleteByPaperID(ctx, paperID); err != nil {
		return fmt.Errorf("delete chunks by paperID: %w", err)
	}

	if err := s.search.IndexChunks(ctx, chunks); err != nil {
		return fmt.Errorf("replace chunks by paperID: %w", err)
	}

	return nil
}

func (s *Service) ProcessPaperIngest(ctx context.Context, paperID, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	paperID = strings.TrimSpace(paperID)

	if paperID == "" {
		return fmt.Errorf("paper id cannot be empty")
	}
	if taskID == "" {
		return fmt.Errorf("task id cannot be empty")
	}

	job, ok := s.tasks.Get(ctx, taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	paper, err := s.papers.Get(ctx, paperID)
	if err != nil {
		return fmt.Errorf("get paper %s from storage: %w", paperID, err)
	}

	if err := s.runPDFTextIngest(ctx, job, paper); err != nil {
		s.fail(ctx, job, paper, err)
		return err
	}
	return nil
}

func (s *Service) ProcessPaperReindex(ctx context.Context, paperID, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	paperID = strings.TrimSpace(paperID)

	if paperID == "" {
		return fmt.Errorf("paper id cannot be empty")
	}
	if taskID == "" {
		return fmt.Errorf("task id cannot be empty")
	}

	job, ok := s.tasks.Get(ctx, taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	now := time.Now()
	job.Status = task.Running
	job.Message = "reindexing paper chunks to Elasticsearch"
	job.UpdatedAt = now

	if err := s.tasks.Update(ctx, job); err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	if err := s.ReindexPaper(ctx, paperID); err != nil {
		s.failTaskOnly(ctx, job, err)
		return err
	}

	now = time.Now()
	job.Status = task.Succeeded
	job.Message = "reindexing paper chunks to Elasticsearch"
	job.UpdatedAt = now

	finishCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := s.tasks.Update(finishCtx, job); err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	return nil
}
