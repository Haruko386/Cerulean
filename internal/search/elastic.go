package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/domain"
)

type ElasticConfig struct {
	URL      string `json:"url"`
	Index    string `json:"index"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ElasticBackend struct {
	baseURL  string
	index    string
	username string
	password string
	client   *http.Client
}

type elasticChunkDoc struct {
	ChunkID    string    `json:"chunk_id"`
	PaperID    string    `json:"paper_id"`
	PageNo     int       `json:"page_no"`
	ChunkIndex int       `json:"chunk_index"`
	Text       string    `json:"text"`
	CreateAt   time.Time `json:"create_at"`
}

func NewElasticBackend(ctx context.Context, cfg ElasticConfig) (*ElasticBackend, error) {
	baseURL := strings.TrimRight(cfg.URL, "/")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:9200"
	}

	index := strings.TrimSpace(cfg.Index)
	if index == "" {
		index = "cerulean_chunks"
	}

	b := &ElasticBackend{
		baseURL:  baseURL,
		index:    index,
		username: cfg.Username,
		password: cfg.Password,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}

	if err := b.EnsureIndex(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *ElasticBackend) Name() string {
	return "elastic"
}

// EnsureIndex Make sure index exists
func (b *ElasticBackend) EnsureIndex(ctx context.Context) error {
	if b == nil {
		return fmt.Errorf("elastic backend is nil")
	}
	if b.client == nil {
		return fmt.Errorf("elastic http client is nil")
	}
	if strings.TrimSpace(b.index) == "" {
		return fmt.Errorf("elastic index is empty")
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodHead,
		b.endpoint("/"+url.PathEscape(b.index)),
		nil,
	)
	if err != nil {
		return err
	}

	b.applyAuth(req)

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("check elastic index %q: %w", b.index, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// 200 表示 index 已经存在。
		return nil

	case http.StatusNotFound:
		// 404 表示 index 不存在，继续创建。

	default:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf(
			"check elastic index %q failed: status=%d body=%s",
			b.index,
			resp.StatusCode,
			string(body),
		)
	}

	mapping := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"chunk_id": map[string]any{
					"type": "keyword",
				},
				"paper_id": map[string]any{
					"type": "keyword",
				},
				"page_no": map[string]any{
					"type": "integer",
				},
				"chunk_index": map[string]any{
					"type": "integer",
				},
				"text": map[string]any{
					"type": "text",
				},
				"created_at": map[string]any{
					"type": "date",
				},
			},
		},
	}

	_, err = b.doJSON(ctx, http.MethodPut, "/"+url.PathEscape(b.index), mapping)
	if err != nil {
		return fmt.Errorf("create elastic index %q: %w", b.index, err)
	}

	return nil
}

func (b *ElasticBackend) endpoint(path string) string {
	if strings.HasPrefix(path, "/") {
		return b.baseURL + path
	}
	return b.baseURL + "/" + path
}

func (b *ElasticBackend) applyAuth(req *http.Request) {
	if b.username != "" {
		req.SetBasicAuth(b.username, b.password)
	}
}

func (b *ElasticBackend) doJSON(ctx context.Context, method string, path string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return b.doRaw(ctx, method, path, "application/json", bytes.NewReader(payload))
}

// doRaw do http request
func (b *ElasticBackend) doRaw(ctx context.Context, method string, path string, contentType string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint(path), body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")

	b.applyAuth(req)

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("elastic request failed: method=%s path=%s status=%d body=%s", method, path, resp.StatusCode, string(data))
	}

	return data, nil
}

// IndexChunks Write chunk into ElasticSearch
func (b *ElasticBackend) IndexChunks(ctx context.Context, chunks []domain.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	for _, chunk := range chunks {
		meta := map[string]any{
			"index": map[string]any{
				"_index": b.index,
				"_id":    chunk.ID,
			},
		}

		if err := encoder.Encode(meta); err != nil {
			return err
		}

		createAt := chunk.CreatedAt
		if createAt.IsZero() {
			createAt = time.Now()
		}

		doc := elasticChunkDoc{
			ChunkID:    chunk.ID,
			PaperID:    chunk.PaperID,
			PageNo:     chunk.PageNo,
			ChunkIndex: chunk.Index,
			Text:       chunk.Text,
			CreateAt:   createAt,
		}

		if err := encoder.Encode(doc); err != nil {
			return err
		}
	}

	data, err := b.doRaw(ctx, http.MethodPost, "/_bulk?refresh=true", "application/x-ndjson", &buf)
	if err != nil {
		return err
	}

	var result struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  any `json:"error"`
		} `json:"items"`
	}

	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}

	if result.Errors {
		return fmt.Errorf("elastic bulk index chunks failed: %s", string(data))
	}
	return nil
}

func (b *ElasticBackend) Search(ctx context.Context, req domain.SearchRequest) (domain.SearchResponse, error) {
	queryText := strings.TrimSpace(req.Query)
	if queryText == "" {
		return domain.SearchResponse{
			Query:   req.Query,
			Results: []domain.SearchResult{},
		}, errors.New("empty query")
	}

	topK := req.TopK
	if topK == 0 {
		topK = 10 // at least return something
	}

	boolQuery := map[string]any{
		"must": []any{
			map[string]any{
				"match": map[string]any{
					"text": queryText,
				},
			},
		},
	}

	// Search in specific paper
	if req.PaperID != "" {
		boolQuery["filter"] = []any{
			map[string]any{
				"term": map[string]any{
					"paper_id": req.PaperID,
				},
			},
		}
	}

	body := map[string]any{
		"size": topK,
		"query": map[string]any{
			"bool": boolQuery,
		},
	}

	data, err := b.doJSON(ctx, http.MethodPost, "/"+url.PathEscape(b.index)+"/_search", body)
	if err != nil {
		return domain.SearchResponse{}, err
	}

	var esResp struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Score  float64         `json:"score"`
				Source elasticChunkDoc `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	results := make([]domain.SearchResult, 0, len(esResp.Hits.Hits))
	if err := json.Unmarshal(data, &esResp); err != nil {
		return domain.SearchResponse{}, err
	}

	for _, hit := range esResp.Hits.Hits {
		results = append(results, domain.SearchResult{
			ChunkID:    hit.Source.ChunkID,
			PaperID:    hit.Source.PaperID,
			PageNo:     hit.Source.PageNo,
			ChunkIndex: hit.Source.ChunkIndex,
			Text:       hit.Source.Text,
			Score:      hit.Score,
			Backend:    b.Name(),
		})
	}

	return domain.SearchResponse{
		Query:   req.Query,
		Results: results,
	}, nil
}

func (b *ElasticBackend) DeleteByPaperID(ctx context.Context, paperID string) error {
	paperID = strings.TrimSpace(paperID)
	if paperID == "" {
		return nil
	}

	body := map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				"paper_id": paperID,
			},
		},
	}

	_, err := b.doJSON(ctx, http.MethodDelete, "/"+url.PathEscape(b.index)+"/_delete_by_query?refresh=true", body)
	return err
}
