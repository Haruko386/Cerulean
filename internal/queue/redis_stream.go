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

// NewRedisStreamQueue creates and initializes a Redis Stream queue.
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

// ensureGroup ensures that the Redis Stream consumer group exists.
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

// Enqueue adds a job to the Redis Stream.
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

// DequeueBatch reads a batch of new messages from the Redis Stream.
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

	redisMessages := make([]redis.XMessage, 0)

	for _, stream := range streams {
		redisMessages = append(redisMessages, stream.Messages...)
	}
	return decodeRedisMessages(redisMessages)
}

// ClaimPending claims idle pending messages for the current consumer.
func (q *RedisStreamQueue) ClaimPending(ctx context.Context, start string, minIdle time.Duration, max int) ([]Message, string, error) {
	if q == nil || q.client == nil {
		return nil, start, fmt.Errorf("redis stream queue is not initialized")
	}

	if strings.TrimSpace(start) == "" {
		start = "0-0"
	}

	if minIdle < 0 {
		minIdle = 35 * time.Minute
	}

	if max <= 0 {
		max = 16
	}

	redisMessages, nextStart, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: q.consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    int64(max),
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, start, nil
		}

		return nil, start, fmt.Errorf("xautoclaim pending messages: %w", err)
	}

	messages, err := decodeRedisMessages(redisMessages)
	if err != nil {
		return nil, nextStart, fmt.Errorf("xautoclaim pending messages: %w", err)
	}
	if strings.TrimSpace(nextStart) == "" {
		nextStart = "0-0"
	}
	return messages, nextStart, nil
}

// Ack acknowledges a successfully processed Redis message.
func (q *RedisStreamQueue) Ack(ctx context.Context, msg Message) error {
	if strings.TrimSpace(msg.RedisID) == "" {
		return nil
	}

	return q.client.XAck(ctx, q.stream, q.group, msg.RedisID).Err()
}

// Nack handles a failed Redis message.
func (q *RedisStreamQueue) Nack(ctx context.Context, msg Message, reason error) error {
	return nil
}

// Close closes the Redis client connection.
func (q *RedisStreamQueue) Close() error {
	if q.client == nil {
		return nil
	}
	return q.client.Close()
}

// decodeRedisMessages converts Redis messages into queue messages.
func decodeRedisMessages(redisMessages []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(redisMessages))

	for _, redisMsg := range redisMessages {
		rawValue, ok := redisMsg.Values["payload"]
		if !ok {
			return nil, fmt.Errorf("redis stream message %s has no payload", redisMsg.ID)
		}

		var raw string

		switch value := rawValue.(type) {
		case string:
			raw = value
		case []byte:
			raw = string(value)
		default:
			raw = fmt.Sprint(value)
		}

		var job Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			return nil, fmt.Errorf("decode redis message %s payload: %w", redisMsg.ID, err)
		}

		messages = append(messages, Message{
			RedisID: redisMsg.ID,
			Job:     job,
		})
	}
	return messages, nil
}
