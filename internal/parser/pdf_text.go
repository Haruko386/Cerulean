package parser

import (
	"context"
	"errors"
	"strings"

	"github.com/ledongthuc/pdf"
)

var (
	ErrNoExtractableText = errors.New("no extractable text from PDF; OCR is required")
)

type PDFTextParser struct{}

func NewPDFTextParser() *PDFTextParser {
	return &PDFTextParser{}
}

// ParseFile extracts page-level text from a PDF file.
func (p *PDFTextParser) ParseFile(ctx context.Context, path string) (Document, error) {
	f, reader, err := pdf.Open(path)
	if err != nil {
		return Document{}, err
	}
	defer f.Close()

	pageCount := reader.NumPage()
	pages := make([]Page, 0, pageCount)

	var full strings.Builder

	for i := 1; i <= pageCount; i++ {
		if err := ctx.Err(); err != nil {
			return Document{}, err
		}

		page := reader.Page(i)
		if page.V.IsNull() {
			continue
		}

		text, err := page.GetPlainText(nil)
		if err != nil {
			return Document{}, err
		}

		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		pages = append(pages, Page{
			PageNo: i,
			Text:   text,
		})

		if full.Len() > 0 {
			full.WriteString("\n\n")
		}
		full.WriteString(text)
	}

	documentText := strings.TrimSpace(full.String())
	if documentText == "" {
		return Document{}, ErrNoExtractableText
	}

	return Document{
		PageCount: pageCount,
		Pages:     pages,
		Text:      documentText,
	}, nil
}
