package redis

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterRejectsInvalidPolicyWithoutRedis(t *testing.T) {
	limiter := &RateLimiter{}
	ctx := context.Background()
	cases := []struct {
		name   string
		key    string
		limit  int
		window time.Duration
	}{
		{name: "empty key", key: "", limit: 1, window: time.Second},
		{name: "zero limit", key: "tenant", limit: 0, window: time.Second},
		{name: "negative limit", key: "tenant", limit: -1, window: time.Second},
		{name: "zero window", key: "tenant", limit: 1, window: 0},
		{name: "negative window", key: "tenant", limit: 1, window: -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := limiter.Allow(ctx, tc.key, tc.limit, tc.window); err == nil {
				t.Fatal("Allow() expected validation error")
			}
		})
	}
}

func TestLockManagerRejectsInvalidLeaseWithoutRedis(t *testing.T) {
	manager := &LockManager{}
	ctx := context.Background()
	if _, err := manager.Lock(ctx, "", time.Second); err == nil {
		t.Fatal("Lock() expected empty-key validation error")
	}
	if _, err := manager.Lock(ctx, "tenant/key", time.Millisecond); err == nil {
		t.Fatal("Lock() expected minimum-TTL validation error")
	}
}
