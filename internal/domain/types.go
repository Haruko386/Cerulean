package domain

import "time"

type PaperStatus string

const (
	PaperUploaded   PaperStatus = "uploaded"
	PaperProcessing PaperStatus = "processing"
	PaperParsed     PaperStatus = "parsed"
	PaperIndexed    PaperStatus = "indexed"
	PaperFailed     PaperStatus = "failed"
)

type Paper struct {
	ID          string      `json:"id"`
	Title       string      `json:"title"`
	Filename    string      `json:"filename"`
	ContentType string      `json:"content_type"`
	Size        int64       `json:"size"`
	SHA256      string      `json:"sha256"`
	ObjectKey   string      `json:"object_key"`
	Status      PaperStatus `json:"status"`
	PageCount   int         `json:"page_count"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

type Chunk struct {
	ID        string            `json:"id"`
	PaperID   string            `json:"paper_id"`
	PageNo    int               `json:"page_no"`
	Index     int               `json:"index"`
	Text      string            `json:"text"`
	ObjectKey string            `json:"object_key,omitempty"`
	VectorID  string            `json:"vector_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Source struct {
	ChunkID string  `json:"chunk_id"`
	PaperID string  `json:"paper_id"`
	PageNo  int     `json:"page_no"`
	Text    string  `json:"text"`
	Score   float64 `json:"score"`
}

type SearchRequest struct {
	Query   string            `json:"query"`
	TopK    int               `json:"top_k"`
	Filters map[string]string `json:"filters,omitempty"`
}

type SearchResponse struct {
	Query   string   `json:"query"`
	Results []Source `json:"results"`
}

type ChatRequest struct {
	Question string            `json:"question"`
	TopK     int               `json:"top_k"`
	Filters  map[string]string `json:"filters,omitempty"`
}

type ChatResponse struct {
	Answer  string   `json:"answer"`
	Sources []Source `json:"sources"`
}
