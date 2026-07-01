package task

import (
	"context"
	"sync"
	"time"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
)

type Task struct {
	ID        string    `json:"id"`
	PaperID   string    `json:"paper_id"`
	Type      string    `json:"type"`
	Status    Status    `json:"status"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Manager interface {
	Create(ctx context.Context, task Task) error
	Update(ctx context.Context, task Task) error
	Get(ctx context.Context, id string) (Task, bool)
}

type MemoryManager struct {
	mu    sync.RWMutex
	tasks map[string]Task
}

func NewMemoryManager() *MemoryManager {
	return &MemoryManager{tasks: make(map[string]Task)}
}

func (m *MemoryManager) Create(ctx context.Context, task Task) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[task.ID] = task
	return nil
}

func (m *MemoryManager) Update(ctx context.Context, task Task) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[task.ID] = task
	return nil
}

func (m *MemoryManager) Get(ctx context.Context, id string) (Task, bool) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[id]
	return task, ok
}
