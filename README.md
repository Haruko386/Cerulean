# Cerulean Server

Cerulean Server is the Go backend for **Cerulean**, a paper-oriented RAG system for graduate students and researchers.

The project is intentionally initialized as a clean, dependency-light skeleton. The default build uses only the Go standard library so the architecture can compile immediately. Real adapters for MinIO, Elasticsearch, PaddleOCR workers, and Amaranth can be added behind the existing interfaces.

## Target Architecture

```text
PDF Upload
  -> Object Storage: MinIO / local fallback
  -> Ingest Task: PaddleOCR / PDF text parser
  -> Chunking
  -> Lexical Index: Elasticsearch
  -> Vector Index: Amaranth
  -> Hybrid Retrieval: RRF
  -> RAG Answer + page-level sources
```

## Current MVP Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/health` | health check |
| POST | `/api/v1/papers` | upload paper PDF as `file` multipart field |
| GET | `/api/v1/papers` | list papers |
| GET | `/api/v1/papers/{id}` | get paper detail |
| POST | `/api/v1/papers/{id}/ingest` | start a mock ingest task |
| POST | `/api/v1/search` | hybrid search placeholder |
| POST | `/api/v1/chat` | RAG answer placeholder |

## Run

```bash
go run ./cmd/server
```

Upload a file:

```bash
curl -F "file=@paper.pdf" http://localhost:8080/api/v1/papers
```

Search:

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"what is the main contribution?","top_k":5}'
```

## Project Layout

```text
cmd/server              main entry
internal/api            HTTP router and handlers
internal/domain         core domain types
internal/storage        object storage interface and local fallback
internal/ingest         document parsing / OCR pipeline interfaces
internal/search         search backend interfaces and hybrid retrieval
internal/rag            RAG service
internal/repository     metadata repository
internal/task           in-memory task manager for MVP
```

## Next Steps

1. Replace `LocalObjectStorage` with a real MinIO implementation using `minio-go`.
2. Add a PaddleOCR Python worker and task queue.
3. Index chunks into Elasticsearch and Amaranth.
4. Implement OCR JSON -> Markdown -> chunk conversion.
5. Add source highlighting with page number and bbox.
