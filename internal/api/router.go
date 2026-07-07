package api

import (
	"net/http"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/dao"

	"github.com/CeruleanFlow/cerulean/internal/config"
	"github.com/CeruleanFlow/cerulean/internal/ingest"
	"github.com/CeruleanFlow/cerulean/internal/rag"
	"github.com/CeruleanFlow/cerulean/internal/repository"
	"github.com/CeruleanFlow/cerulean/internal/storage"
	"github.com/CeruleanFlow/cerulean/internal/task"
	"github.com/gin-gonic/gin"
)

type RouterOptions struct {
	Config        config.Config
	PaperRepo     repository.PaperRepository
	ChunkRepo     repository.ChunkRepository
	UserDAO       *dao.UserDAO
	TaskManager   task.Manager
	ObjectStore   storage.ObjectStorage
	IngestService *ingest.Service
	RAGService    *rag.Service
}

func NewRouter(opts RouterOptions) http.Handler {
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(cors(opts.Config.CORSOrigins))

	h := NewHandler(opts)

	v1 := router.Group("/api/v1")
	{
		v1.GET("/health", h.Health)

		v1.GET("/settings/deepseek", h.GetDeepSeekSettings)
		v1.PUT("/settings/deepseek", h.UpdateDeepSeekSettings)

		papers := v1.Group("/papers")
		{
			papers.POST("", h.UploadPaper)
			papers.GET("", h.ListPapers)
			papers.GET("/:id", h.GetPaper)
			papers.GET("/:id/download", h.DownloadPaper)
			papers.GET("/:id/chunks", h.ListPaperChunks)
			papers.POST("/:id/ingest", h.StartPaperIngest)
			papers.POST("/:id/reindex", h.)
		}

		tasks := v1.Group("/tasks")
		{
			tasks.GET("/:id", h.GetTask)
		}

		v1.POST("/search", h.Search)
		//v1.POST("/chat", h.Chat)
	}

	return router
}

func cors(origins string) gin.HandlerFunc {
	allowed := map[string]bool{}
	for _, origin := range strings.Split(origins, ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			allowed[origin] = true
		}
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")

		if origins == "*" {
			c.Header("Access-Control-Allow-Origin", "*")
		} else if allowed[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
		}

		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
