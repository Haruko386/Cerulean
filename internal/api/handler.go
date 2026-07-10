package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/dao"

	"github.com/CeruleanFlow/cerulean/internal/domain"
	"github.com/CeruleanFlow/cerulean/internal/ingest"
	"github.com/CeruleanFlow/cerulean/internal/rag"
	"github.com/CeruleanFlow/cerulean/internal/repository"
	"github.com/CeruleanFlow/cerulean/internal/storage"
	"github.com/CeruleanFlow/cerulean/internal/task"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	papers repository.PaperRepository
	chunks repository.ChunkRepository
	tasks  task.Manager
	store  storage.ObjectStorage
	ingest *ingest.Service
	rag    *rag.Service
	users  *dao.UserDAO
}

func NewHandler(opts RouterOptions) *Handler {
	return &Handler{
		papers: opts.PaperRepo,
		chunks: opts.ChunkRepo,
		users:  opts.UserDAO,
		tasks:  opts.TaskManager,
		store:  opts.ObjectStore,
		ingest: opts.IngestService,
		rag:    opts.RAGService,
	}
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "cerulean-server",
	})
}

// UploadPaper upload a new paper
func (h *Handler) UploadPaper(c *gin.Context) {
	header, err := c.FormFile("file")
	if err != nil {
		writeError(c, http.StatusBadRequest, fmt.Errorf("multipart field 'file' is required: %w", err))
		return
	}

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext != ".pdf" {
		writeError(c, http.StatusBadRequest, fmt.Errorf("only PDF upload is supported in the current paper-RAG MVP"))
		return
	}

	file, err := header.Open()
	if err != nil {
		writeError(c, http.StatusBadRequest, err)
		return
	}
	defer file.Close()

	if err := os.MkdirAll(".var/tmp", 0o755); err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	tmp, err := os.CreateTemp(".var/tmp", "upload-*.pdf")
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, hasher), file)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	if size == 0 {
		writeError(c, http.StatusBadRequest, fmt.Errorf("empty file"))
		return
	}

	digest := hex.EncodeToString(hasher.Sum(nil))

	opCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	existing, err := h.papers.FindBySHA256(opCtx, digest)
	if err == nil {
		c.JSON(http.StatusOK, existing)
		return
	}

	if !errors.Is(err, repository.ErrNotFound) {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	paperID := "paper_" + digest[:16]
	objectKey := fmt.Sprintf("papers/%s/original.pdf", paperID)

	contentType := header.Header.Get("Content-Type")
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = "application/pdf"
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	_, err = h.store.Put(
		opCtx,
		objectKey,
		tmp,
		size,
		storage.PutOptions{
			ContentType: contentType,
			Metadata: map[string]string{
				"sha256":   digest,
				"filename": header.Filename,
			},
		},
	)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	now := time.Now()
	paper := domain.Paper{
		ID:          paperID,
		Title:       strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename)),
		Filename:    header.Filename,
		ContentType: contentType,
		Size:        size,
		SHA256:      digest,
		ObjectKey:   objectKey,
		Status:      domain.PaperUploaded,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := h.papers.Create(opCtx, paper); err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusCreated, paper)
}

func (h *Handler) ListPapers(c *gin.Context) {
	papers, err := h.papers.List(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": papers,
	})
}

func (h *Handler) GetPaper(c *gin.Context) {
	id := c.Param("id")

	paper, err := h.papers.Get(c.Request.Context(), id)
	if err != nil {
		writeError(c, http.StatusNotFound, err)
		return
	}

	c.JSON(http.StatusOK, paper)
}

func (h *Handler) DownloadPaper(c *gin.Context) {
	id := c.Param("id")

	paper, err := h.papers.Get(c.Request.Context(), id)
	if err != nil {
		writeError(c, http.StatusNotFound, err)
		return
	}

	reader, info, err := h.store.Get(c.Request.Context(), paper.ObjectKey)
	if err != nil {
		writeError(c, http.StatusNotFound, err)
		return
	}
	defer reader.Close()

	contentType := paper.ContentType
	if contentType == "" {
		contentType = info.ContentType
	}
	if contentType == "" {
		contentType = "application/pdf"
	}

	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=%q", paper.Filename))

	if paper.Size > 0 {
		c.Header("Content-Length", fmt.Sprintf("%d", paper.Size))
	}

	_, _ = io.Copy(c.Writer, reader)
}

func (h *Handler) ListPaperChunks(c *gin.Context) {
	id := c.Param("id")

	if _, err := h.papers.Get(c.Request.Context(), id); err != nil {
		writeError(c, http.StatusNotFound, err)
		return
	}

	chunks, err := h.chunks.ListByPaperID(c.Request.Context(), id)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": chunks,
	})
}

func (h *Handler) StartPaperIngest(c *gin.Context) {
	id := c.Param("id")

	job, err := h.ingest.StartPaperIngest(c.Request.Context(), id)
	if err != nil {
		writeError(c, http.StatusNotFound, err)
		return
	}

	c.JSON(http.StatusAccepted, job)
}

func (h *Handler) ReindexPaper(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		writeError(c, http.StatusBadRequest, errors.New("id is required"))
		return
	}

	job, err := h.ingest.StartPaperReindex(c.Request.Context(), id)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusAccepted, job)
}

func (h *Handler) GetTask(c *gin.Context) {
	id := c.Param("id")

	job, ok := h.tasks.Get(c.Request.Context(), id)
	if !ok {
		writeError(c, http.StatusNotFound, fmt.Errorf("task not found"))
		return
	}

	c.JSON(http.StatusOK, job)
}

func (h *Handler) Search(c *gin.Context) {
	var req domain.SearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, err)
		return
	}

	resp, err := h.rag.Search(c.Request.Context(), req)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, resp)
}

//func (h *Handler) Chat(c *gin.Context) {
//	var req domain.ChatRequest
//	if err := c.ShouldBindJSON(&req); err != nil {
//		writeError(c, http.StatusBadRequest, err)
//		return
//	}
//
//	resp, err := h.rag.Chat(c.Request.Context(), req)
//	if err != nil {
//		writeError(c, http.StatusInternalServerError, err)
//		return
//	}
//
//	c.JSON(http.StatusOK, resp)
//}

func writeError(c *gin.Context, status int, err error) {
	c.JSON(status, gin.H{
		"error": err.Error(),
	})
}

func (h *Handler) GetDeepSeekSettings(c *gin.Context) {
	if h.users == nil {
		writeError(c, http.StatusInternalServerError, fmt.Errorf("user dao is not initialized"))
		return
	}

	user, err := h.users.GetDefault(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":           user.ID,
		"deepseek_base_url": user.DeepSeekBaseURL,
		"deepseek_model":    user.DeepSeekModel,
		"has_api_key":       user.DeepSeekAPIKey != "",
	})
}

type updateDeepSeekSettingsRequest struct {
	APIKey  string `json:"api_key"`
	BaseURL string `json:"base_url"`
	Model   string `json:"model"`
}

func (h *Handler) UpdateDeepSeekSettings(c *gin.Context) {
	if h.users == nil {
		writeError(c, http.StatusInternalServerError, fmt.Errorf("user dao is not initialized"))
		return
	}

	var req updateDeepSeekSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, err)
		return
	}

	user, err := h.users.UpdateDeepSeekConfig(
		c.Request.Context(),
		req.APIKey,
		req.BaseURL,
		req.Model,
	)
	if err != nil {
		writeError(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":           user.ID,
		"deepseek_base_url": user.DeepSeekBaseURL,
		"deepseek_model":    user.DeepSeekModel,
		"has_api_key":       user.DeepSeekAPIKey != "",
	})
}
