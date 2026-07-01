# Cerulean Backend Architecture

Cerulean is split into independent backend and frontend repositories.

## Backend Responsibilities

- Manage paper metadata and processing status.
- Store original PDFs and derived OCR/layout artifacts in object storage.
- Dispatch OCR / parsing / chunking / indexing jobs.
- Provide search and RAG APIs to the frontend.
- Hide retrieval backend details behind interfaces.

## Storage Layout

Recommended MinIO bucket: `cerulean-papers`.

```text
papers/{paper_id}/original.pdf
papers/{paper_id}/pages/page_001.png
papers/{paper_id}/ocr/paddleocr-v1/page_001.json
papers/{paper_id}/layout/paddleocr-v1/page_001.json
papers/{paper_id}/document.md
papers/{paper_id}/chunks/chunks.json
figures/{paper_id}/figure_001.png
tables/{paper_id}/table_001.html
```

## Retrieval Layout

```text
Elasticsearch
  - chunk_id
  - paper_id
  - page_no
  - section
  - text
  - BM25 / keyword search

Amaranth
  - vector_id / chunk_id
  - paper_id
  - embedding
  - cosine / L2 / inner product search

PostgreSQL
  - paper metadata
  - chunk metadata
  - page mapping
  - task states
```

## Search Flow

```text
query
  -> embedding model
  -> Elasticsearch BM25 topK
  -> Amaranth vector topK
  -> RRF fusion
  -> chunk metadata lookup
  -> answer generation
  -> sources with page number
```

## API Stability

The frontend should only depend on `/api/v1/*`. Internal services can be replaced freely.
