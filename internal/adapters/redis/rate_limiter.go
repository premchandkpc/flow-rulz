package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RateLimiter implements a sliding-window limit using a Redis sorted set.
type RateLimiter struct {
	client *redis.Client
}

// NewRateLimiter creates a new RateLimiter from a Redis client.
func NewRateLimiter(client *Client) *RateLimiter {
	return &RateLimiter{client: client.client}
}

// Allow checks if a request is allowed under the rate limit.
func (r *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("rate limit: key is required")
	}
	if limit <= 0 {
		return false, fmt.Errorf("rate limit: limit must be positive")
	}
	if window <= 0 {
		return false, fmt.Errorf("rate limit: window must be positive")
	}

	now := time.Now()
	nowMS := now.UnixMilli()
	windowStart := nowMS - window.Milliseconds()
	key = "ratelimit:" + key

	pipe := r.client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(windowStart, 10))
	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(nowMS),
		Member: uuid.NewString(),
	})
	countCmd := pipe.ZCard(ctx, key)
	pipe.PExpire(ctx, key, window)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("rate limit check: %w", err)
	}
	count, err := countCmd.Result()
	if err != nil {
		return false, fmt.Errorf("read rate limit count: %w", err)
	}
	return count <= int64(limit), nil
}

// Reset removes the rate limit counter for a key.
func (r *RateLimiter) Reset(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("rate limit reset: key is required")
	}
	if err := r.client.Del(ctx, "ratelimit:"+key).Err(); err != nil {
		return fmt.Errorf("rate limit reset: %w", err)
	}
	return nil
}

// Limit returns the current count for a key.
func (r *RateLimiter) Limit(ctx context.Context, key string) (int, error) {
	key = "ratelimit:" + key
	count, err := r.client.ZCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("rate limit count: %w", err)
	}
	return int(count), nil
}

// Remaining returns the remaining requests allowed for a key.
func (r *RateLimiter) Remaining(ctx context.Context, key string) (int, error) {
	key = "ratelimit:" + key
	count, err := r.client.ZCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("rate limit count: %w", err)
	}
	return int(count), nil
}

var _ ports.RateLimiter = (*RateLimiter)(nil)
