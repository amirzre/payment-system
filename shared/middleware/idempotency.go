package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisIdempotency provides a fast-path idempotency check using Redis SETNX.
// The Postgres UNIQUE constraint on idempotency_key remains the durable guarantee.
type RedisIdempotency struct {
	client *redis.Client
	ttl    time.Duration
	prefix string
}

// NewRedisIdempotency creates a new RedisIdempotency helper.
func NewRedisIdempotency(client *redis.Client, ttl time.Duration, prefix string) *RedisIdempotency {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if prefix == "" {
		prefix = "idempotency"
	}
	return &RedisIdempotency{client: client, ttl: ttl, prefix: prefix}
}

// CheckAndSet attempts to atomically set the idempotency key in Redis.
// Returns (false, nil) if the key already exists (duplicate request).
// Returns (true, nil) if the key was set (new request).
// Returns (true, err) if Redis is unavailable — the caller should proceed (fail open).
func (r *RedisIdempotency) CheckAndSet(ctx context.Context, key string) (bool, error) {
	redisKey := fmt.Sprintf("%s:%s", r.prefix, key)

	ok, err := r.client.SetNX(ctx, redisKey, "seen", r.ttl).Result()
	if err != nil {
		// Redis unavailable: fail open — let Postgres handle deduplication
		return true, fmt.Errorf("redis setnx: %w", err)
	}

	return ok, nil
}
