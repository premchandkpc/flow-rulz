// Package redis provides Redis-backed cache adapters.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/redis/go-redis/v9"
)

// Cache implements a Redis-backed cache.
type Cache struct {
	client *redis.Client
}

// NewCache creates a new Cache from a Redis client.
func NewCache(client *Client) *Cache {
	return &Cache{client: client.client}
}

// Get retrieves a value from the cache.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache get: %w", err)
	}
	return val, nil
}

// Set stores a value in the cache with the given TTL.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := c.client.Set(ctx, key, value, ttl).Err()
	if err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

// Delete removes a key from the cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("cache delete: %w", err)
	}
	return nil
}

// Exists checks if a key exists in the cache.
func (c *Cache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("cache exists: %w", err)
	}
	return n > 0, nil
}

// Increment atomically increments a key by delta and returns the new value.
func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	val, err := c.client.IncrBy(ctx, key, delta).Result()
	if err != nil {
		return 0, fmt.Errorf("cache increment: %w", err)
	}
	return val, nil
}

// Expire sets the TTL for a key.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("cache expire: ttl must be positive")
	}
	err := c.client.Expire(ctx, key, ttl).Err()
	if err != nil {
		return fmt.Errorf("cache expire: %w", err)
	}
	return nil
}

// TTL returns the remaining time-to-live for a key.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	dur, err := c.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("cache ttl: %w", err)
	}
	return dur, nil
}

// Keys returns all keys matching the given pattern.
func (c *Cache) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("cache keys: %w", err)
	}
	return keys, nil
}

// Flush removes all keys from the current database.
func (c *Cache) Flush(ctx context.Context) error {
	err := c.client.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("cache flush: %w", err)
	}
	return nil
}

var _ ports.Cache = (*Cache)(nil)
