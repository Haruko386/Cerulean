package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/CeruleanFlow/cerulean-server/internal/domain"
	"github.com/CeruleanFlow/cerulean-server/internal/ingest"
	"github.com/CeruleanFlow/cerulean-server/internal/rag"
	"github.com/CeruleanFlow/cerulean-server/internal/repository"
	"github.com/CeruleanFlow/cerulean-server/internal/storage"
	jsonhttp "github.com/CeruleanFlow/cerulean-server/pkg/httputil"
)

type Handler struct {
	papers repository.PaperRepository
	store  storage.ObjectStorage
	ingest *ingest.Service
	rag    *rag.Service
}

func NewHandler(opts RouterOptions) *Handler {
	return &Handler{
		papers: opts.PaperRepo,
		store:  opts.ObjectStore,
		ingest: opts.IngestService,
		rag:    opts.RAGService,
	}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	jsonhttp.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "cerulean-server"})
}

func (h *Handler) UploadPaper(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(128 << 20); err != nil {
		jsonhttp.WriteError(w, http.StatusBadRequest, err)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		jsonhttp.WriteError(w, http.StatusBadRequest, fmt.Errorf("multipart field 'file' is required: %w", err))
		return
	}
	defer file.Close()

	tmp, err := io.ReadAll(file)
	if err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	sum := sha256.Sum256(tmp)
	digest := hex.EncodeToString(sum[:])
	paperID := "paper_" + digest[:16]
	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext == "" {
		ext = ".pdf"
	}
	objectKey := fmt.Sprintf("papers/%s/original%s", paperID, ext)
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/pdf"
	}

	if _, err := h.store.Put(r.Context(), objectKey, bytes.NewReader(tmp), int64(len(tmp)), storage.PutOptions{ContentType: contentType}); err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	now := time.Now()
	paper := domain.Paper{
		ID:          paperID,
		Title:       strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename)),
		Filename:    header.Filename,
		ContentType: contentType,
		Size:        int64(len(tmp)),
		SHA256:      digest,
		ObjectKey:   objectKey,
		Status:      domain.PaperUploaded,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := h.papers.Create(r.Context(), paper); err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	jsonhttp.WriteJSON(w, http.StatusCreated, paper)
}

func (h *Handler) ListPapers(w http.ResponseWriter, r *http.Request) {
	papers, err := h.papers.List(r.Context())
	if err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	jsonhttp.WriteJSON(w, http.StatusOK, map[string]any{"items": papers})
}

func (h *Handler) GetPaper(w http.ResponseWriter, r *http.Request) {
	id, action := parsePaperPath(r.URL.Path)
	if id == "" || action != "" {
		jsonhttp.WriteError(w, http.StatusNotFound, fmt.Errorf("paper not found"))
		return
	}
	paper, err := h.papers.Get(r.Context(), id)
	if err != nil {
		jsonhttp.WriteError(w, http.StatusNotFound, err)
		return
	}
	jsonhttp.WriteJSON(w, http.StatusOK, paper)
}

func (h *Handler) PaperAction(w http.ResponseWriter, r *http.Request) {
	id, action := parsePaperPath(r.URL.Path)
	if id == "" || action == "" {
		jsonhttp.WriteError(w, http.StatusNotFound, fmt.Errorf("paper action not found"))
		return
	}
	switch action {
	case "ingest":
		job, err := h.ingest.StartPaperIngest(r.Context(), id)
		if err != nil {
			jsonhttp.WriteError(w, http.StatusNotFound, err)
			return
		}
		jsonhttp.WriteJSON(w, http.StatusAccepted, job)
	default:
		jsonhttp.WriteError(w, http.StatusNotFound, fmt.Errorf("unknown paper action: %s", action))
	}
}

func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	var req domain.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonhttp.WriteError(w, http.StatusBadRequest, err)
		return
	}
	resp, err := h.rag.Search(r.Context(), req)
	if err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	jsonhttp.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) Chat(w http.ResponseWriter, r *http.Request) {
	var req domain.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonhttp.WriteError(w, http.StatusBadRequest, err)
		return
	}
	resp, err := h.rag.Chat(r.Context(), req)
	if err != nil {
		jsonhttp.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	jsonhttp.WriteJSON(w, http.StatusOK, resp)
}

func parsePaperPath(path string) (id string, action string) {
	trimmed := strings.TrimPrefix(path, "/api/v1/papers/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) >= 1 {
		id = parts[0]
	}
	if len(parts) >= 2 {
		action = parts[1]
	}
	return id, action
}
