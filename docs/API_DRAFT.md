# API Draft

## Upload Paper

```http
POST /api/v1/papers
Content-Type: multipart/form-data

file=<PDF>
```

## Start Ingestion

```http
POST /api/v1/papers/{paper_id}/ingest
```

## Search

```http
POST /api/v1/search
Content-Type: application/json

{
  "query": "what is the main contribution?",
  "top_k": 5,
  "filters": {
    "paper_id": "paper_xxx"
  }
}
```

## Chat

```http
POST /api/v1/chat
Content-Type: application/json

{
  "question": "summarize the method section",
  "top_k": 5,
  "filters": {
    "paper_id": "paper_xxx"
  }
}
```
