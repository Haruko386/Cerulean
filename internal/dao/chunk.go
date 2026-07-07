package dao

import (
	"context"
	"fmt"
	"strings"

	"github.com/CeruleanFlow/cerulean/internal/domain"
	"github.com/CeruleanFlow/cerulean/internal/entity"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ChunkDAO struct {
	db *gorm.DB
}

func NewChunkDAO(db *gorm.DB) *ChunkDAO {
	return &ChunkDAO{db: db}
}

func (d *ChunkDAO) UpsertMany(ctx context.Context, chunks []domain.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	models := make([]entity.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		models = append(models, toChunkEntity(chunk))
	}

	return d.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			UpdateAll: true,
		}).
		Create(&models).Error
}

func (d *ChunkDAO) List(ctx context.Context, filters map[string]string) ([]domain.Chunk, error) {
	query := d.db.WithContext(ctx).Model(&entity.Chunk{})

	if filters != nil {
		if paperID := filters["paper_id"]; paperID != "" {
			query = query.Where("paper_id = ?", paperID)
		}
	}

	var models []entity.Chunk

	if err := query.
		Order("paper_id ASC").
		Order("page_no ASC").
		Order("chunk_index ASC").
		Find(&models).Error; err != nil {
		return nil, err
	}

	chunks := make([]domain.Chunk, 0, len(models))
	for _, model := range models {
		chunks = append(chunks, chunkEntityToDomain(model))
	}

	return chunks, nil
}

func (d *ChunkDAO) ListByPaperID(ctx context.Context, paperID string) ([]domain.Chunk, error) {
	return d.List(ctx, map[string]string{
		"paper_id": paperID,
	})
}

func (d *ChunkDAO) DeleteByPaperID(ctx context.Context, paperID string) error {
	return d.db.WithContext(ctx).
		Delete(&entity.Chunk{}, "paper_id = ?", paperID).
		Error
}

func toChunkEntity(chunk domain.Chunk) entity.Chunk {
	return entity.Chunk{
		ID:         chunk.ID,
		UserID:     DefaultUserID,
		PaperID:    chunk.PaperID,
		PageNo:     chunk.PageNo,
		ChunkIndex: chunk.Index,
		Text:       chunk.Text,
		ObjectKey:  chunk.ObjectKey,
		VectorID:   chunk.VectorID,
		Metadata:   toJSONMap(chunk.Metadata),
		CreatedAt:  normalizeTime(chunk.CreatedAt),
		UpdatedAt:  normalizeTime(chunk.UpdatedAt),
	}
}

func chunkEntityToDomain(model entity.Chunk) domain.Chunk {
	return domain.Chunk{
		ID:        model.ID,
		PaperID:   model.PaperID,
		PageNo:    model.PageNo,
		Index:     model.ChunkIndex,
		Text:      model.Text,
		ObjectKey: model.ObjectKey,
		VectorID:  model.VectorID,
		Metadata:  fromJSONMap(model.Metadata),
		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}
}

func toJSONMap(values map[string]string) datatypes.JSONMap {
	out := datatypes.JSONMap{}

	for key, value := range values {
		out[key] = value
	}

	return out
}

func fromJSONMap(values datatypes.JSONMap) map[string]string {
	if values == nil {
		return nil
	}

	out := make(map[string]string, len(values))

	for key, value := range values {
		switch v := value.(type) {
		case string:
			out[key] = v
		default:
			out[key] = fmt.Sprint(v)
		}
	}

	return out
}

func (d *ChunkDAO) ReplaceByPaperID(ctx context.Context, paperID string, chunks []domain.Chunk) error {
	if strings.TrimSpace(paperID) == "" {
		return fmt.Errorf("invalid paper id")
	}

	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("paper_id = ?", paperID).Delete(&entity.Chunk{}).Error; err != nil {
			return fmt.Errorf("delete old chunks: %w", err)
		}
		if len(chunks) == 0 {
			return nil
		}

		entities := make([]entity.Chunk, 0, len(chunks))

		for _, chunk := range chunks {
			if chunk.PaperID != paperID {
				return fmt.Errorf("invalid paper id")
			}
			if chunk.PaperID == "" {
				chunk.PaperID = paperID
			}

			entities = append(entities, domainChunkToEntity(chunk))
		}

		if err := tx.Create(&entities).Error; err != nil {
			return fmt.Errorf("insert new chunks: %w", err)
		}
		return nil
	})
}

func domainChunkToEntity(chunk domain.Chunk) entity.Chunk {
	metadata := map[string]any{}
	for k, v := range chunk.Metadata {
		metadata[k] = v
	}

	return entity.Chunk{
		ID:         chunk.ID,
		PaperID:    chunk.PaperID,
		PageNo:     chunk.PageNo,
		ChunkIndex: chunk.Index,
		Text:       chunk.Text,
		ObjectKey:  chunk.ObjectKey,
		VectorID:   chunk.VectorID,
		Metadata:   metadata,
		CreatedAt:  chunk.CreatedAt,
		UpdatedAt:  chunk.UpdatedAt,
	}
}
