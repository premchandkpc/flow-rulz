package ports

import (
	"context"
	"time"
)

// DistributedLock provides distributed locking primitives
type DistributedLock interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, token string) (bool, error)
	Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	Lock(ctx context.Context, key string, ttl time.Duration) (LockGuard, error)
}

// LockGuard represents a held lock with release and extend capabilities.
type LockGuard interface {
	Key() string
	Token() string
	Release(ctx context.Context) error
	Extend(ctx context.Context, ttl time.Duration) error
}

// RateLimiter provides rate limiting primitives
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
	Limit(ctx context.Context, key string) (int, error)
	Remaining(ctx context.Context, key string) (int, error)
	Reset(ctx context.Context, key string) error
}
