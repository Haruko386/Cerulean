package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStreamConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`

	Stream   string `json:"stream"`
	Group    string `json:"group"`
	Consumer string `json:"consumer"`
}

type RedisStreamQueue struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
}

func NewRedisStreamQueue(ctx context.Context, cfg RedisStreamConfig) (*RedisStreamQueue, error) {
	// precheck
	stream := strings.TrimSpace(cfg.Stream)
	if stream == "" {
		stream = "cerulean_tasks"
	}

	group := strings.TrimSpace(cfg.Group)
	if group == "" {
		group = "cerulean_workers"
	}

	consumer := strings.TrimSpace(cfg.Consumer)
	if consumer == "" {
		consumer = "worker_local_1"
	}

	// initialize the redis
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	// return if cannot initialize redis
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	q := &RedisStreamQueue{
		client:   client,
		stream:   stream,
		group:    group,
		consumer: consumer,
	}

	if err := q.ensureGroup(ctx); err != nil {
		return nil, err
	}

	return q, nil
}

func (q *RedisStreamQueue) ensureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf("failed to create group '%s': %w", q.group, err)

}

func (q *RedisStreamQueue) Enqueue(ctx context.Context, job Job) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to encode job: %w", err)
	}

	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"payload":  string(payload),
			"type":     job.Type,
			"task_id":  job.TaskID,
			"paper_id": job.PaperID,
		},
	}).Err()
}

func (q *RedisStreamQueue) DequeueBatch(ctx context.Context, max int, blockMillis int64) ([]Message, error) {
	if max <= 0 {
		max = 16
	}

	if blockMillis <= 0 {
		blockMillis = 5000
	}

	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"},
		Count:    int64(max),
		Block:    time.Duration(blockMillis) * time.Millisecond,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	messages := make([]Message, 0)

	for _, stream := range streams {
		for _, redisMsg := range stream.Messages {
			raw, ok := redisMsg.Values["payload"].(string)
			if !ok {
				continue
			}

			var job Job
			if err := json.Unmarshal([]byte(raw), &job); err != nil {
				continue
			}

			messages = append(messages, Message{
				RedisID: redisMsg.ID,
				Job:     job,
			})
		}
	}
	return messages, nil
}

func (q *RedisStreamQueue) Ack(ctx context.Context, msg Message) error {
	if strings.TrimSpace(msg.RedisID) == "" {
		return nil
	}

	return q.client.XAck(ctx, q.stream, q.group, msg.RedisID).Err()
}

func (q *RedisStreamQueue) Nack(ctx context.Context, msg Message, reason error) error {
	return nil
}

// Close the redis
func (q *RedisStreamQueue) Close() error {
	if q.client == nil {
		return nil
	}
	return q.client.Close()
}
