package main

import (
	"log"
	"net/http"

	"github.com/CeruleanFlow/cerulean-server/internal/api"
	"github.com/CeruleanFlow/cerulean-server/internal/config"
	"github.com/CeruleanFlow/cerulean-server/internal/ingest"
	"github.com/CeruleanFlow/cerulean-server/internal/rag"
	"github.com/CeruleanFlow/cerulean-server/internal/repository"
	"github.com/CeruleanFlow/cerulean-server/internal/search"
	"github.com/CeruleanFlow/cerulean-server/internal/search/amaranth"
	"github.com/CeruleanFlow/cerulean-server/internal/search/elastic"
	"github.com/CeruleanFlow/cerulean-server/internal/storage"
	"github.com/CeruleanFlow/cerulean-server/internal/task"
)

func main() {
	cfg := config.Load()

	objectStore := storage.NewLocalObjectStorage(cfg.LocalStorageDir)
	paperRepo := repository.NewMemoryPaperRepository()
	taskManager := task.NewMemoryManager()

	elasticBackend := elastic.NewBackend(cfg.ElasticURL, cfg.ElasticIndex)
	amaranthBackend := amaranth.NewBackend(cfg.AmaranthURL, cfg.AmaranthCollection)
	hybrid := search.NewHybridBackend(elasticBackend, amaranthBackend, search.NewRRFusion(60))

	ingestService := ingest.NewService(paperRepo, objectStore, taskManager)
	ragService := rag.NewService(paperRepo, hybrid)

	router := api.NewRouter(api.RouterOptions{
		Config:        cfg,
		PaperRepo:     paperRepo,
		ObjectStore:   objectStore,
		IngestService: ingestService,
		RAGService:    ragService,
	})

	log.Printf("Cerulean server listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, router); err != nil {
		log.Fatal(err)
	}
}
