package api

import (
	"net/http"
	"strings"

	"github.com/CeruleanFlow/cerulean-server/internal/config"
	"github.com/CeruleanFlow/cerulean-server/internal/ingest"
	"github.com/CeruleanFlow/cerulean-server/internal/rag"
	"github.com/CeruleanFlow/cerulean-server/internal/repository"
	"github.com/CeruleanFlow/cerulean-server/internal/storage"
)

type RouterOptions struct {
	Config        config.Config
	PaperRepo     repository.PaperRepository
	ObjectStore   storage.ObjectStorage
	IngestService *ingest.Service
	RAGService    *rag.Service
}

func NewRouter(opts RouterOptions) http.Handler {
	mux := http.NewServeMux()
	h := NewHandler(opts)

	mux.HandleFunc("GET /api/v1/health", h.Health)
	mux.HandleFunc("POST /api/v1/papers", h.UploadPaper)
	mux.HandleFunc("GET /api/v1/papers", h.ListPapers)
	mux.HandleFunc("GET /api/v1/papers/", h.GetPaper)
	mux.HandleFunc("POST /api/v1/papers/", h.PaperAction)
	mux.HandleFunc("POST /api/v1/search", h.Search)
	mux.HandleFunc("POST /api/v1/chat", h.Chat)

	return cors(opts.Config.CORSOrigins, mux)
}

func cors(origins string, next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, origin := range strings.Split(origins, ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			allowed[origin] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allowed[origin] || origins == "*" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
