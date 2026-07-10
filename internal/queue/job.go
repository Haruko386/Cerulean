package queue

import "time"

const (
	JobTypePaperIngest  = "paper_ingest"
	JobTypePaperReindex = "paper_reindex"
)

type Job struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	Type      string    `json:"type"`
	PaperID   string    `json:"paper_id"`
	Attempt   int       `json:"attempt"`
	CreatedAt time.Time `json:"created_at"`
}

type Message struct {
	RedisID string `json:"redis_id"`
	Job     Job    `json:"job"`
}
