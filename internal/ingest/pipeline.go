package ingest

import "context"

// Pipeline describes the future document processing chain.
//
// Planned implementations:
//  1. Native PDF text extraction
//  2. PaddleOCR worker for scanned PDFs
//  3. Layout JSON -> Markdown conversion
//  4. Page-aware chunking
//  5. Elasticsearch + Amaranth indexing
//
// The server skeleton keeps this as an interface so a Python OCR worker,
// a Go parser, or a remote service can all be plugged in later.
type Pipeline interface {
	Run(ctx context.Context, paperID string) error
}
