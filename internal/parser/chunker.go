package parser

import (
	"fmt"
	"strings"
	"time"

	"github.com/CeruleanFlow/cerulean/internal/domain"
)

// BuildChunks converts parsed document pages into searchable chunks.
func BuildChunks(paper domain.Paper, artifactKey string, doc Document, maxRunes, overlap int) []domain.Chunk {
	if maxRunes <= 0 {
		maxRunes = 1200
	}
	if overlap < 0 {
		overlap = 0
	}

	now := time.Now()
	chunks := make([]domain.Chunk, 0)

	chunkIndex := 0

	for _, page := range doc.Pages {
		pageText := strings.TrimSpace(page.Text)
		if pageText == "" {
			continue
		}

		pieces := splitTextByRunes(pageText, maxRunes, overlap)

		for _, piece := range pieces {
			piece = strings.TrimSpace(piece)
			if piece == "" {
				continue
			}

			chunks = append(chunks, domain.Chunk{
				ID:        fmt.Sprintf("%s_chunk_%04d", paper.ID, chunkIndex+1),
				PaperID:   paper.ID,
				PageNo:    page.PageNo,
				Index:     chunkIndex,
				Text:      piece,
				ObjectKey: artifactKey,
				Metadata: map[string]string{
					"source": "pdf_text_parse",
					"title":  paper.Title,
				},
				CreatedAt: now,
				UpdatedAt: now,
			})

			chunkIndex++
		}
	}
	return chunks
}

// splitTextByRunes splits text into overlapping UTF-8-safe chunks.
func splitTextByRunes(text string, maxRunes int, overlap int) []string {
	runes := []rune(text)
	if len(runes) == 0 {
		return nil
	}

	result := make([]string, 0)

	start := 0
	for start < len(runes) {
		end := start + maxRunes
		if end >= len(runes) {
			end = len(runes)
		} else {
			end = chooseChunkEnd(runes, start, end)
		}

		result = append(result, string(runes[start:end]))

		if end >= len(runes) {
			break
		}

		next := end - overlap
		if next <= start {
			next = end
		}
		start = next
	}

	return result
}

// chooseChunkEnd selects a natural sentence boundary for a chunk.
func chooseChunkEnd(runes []rune, start int, end int) int {
	minEnd := start + int(float64(end-start)*0.7)

	for i := end; i > minEnd; i-- {
		switch runes[i-1] {
		case '\n', '.', '。', ';', '；', '!', '！', '?', '？':
			return i
		}
	}

	return end
}
