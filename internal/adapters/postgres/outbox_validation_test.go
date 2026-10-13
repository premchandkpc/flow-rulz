package postgres

import (
	"context"
	"testing"
	"time"
)

func TestClaimPendingRejectsInvalidArguments(t *testing.T) {
	repo := &OutboxRepository{}
	cases := []struct {
		name  string
		size  int
		owner string
		ttl   time.Duration
	}{
		{name: "zero batch", size: 0, owner: "worker-1", ttl: time.Minute},
		{name: "oversized batch", size: 1001, owner: "worker-1", ttl: time.Minute},
		{name: "empty owner", size: 1, owner: " ", ttl: time.Minute},
		{name: "zero ttl", size: 1, owner: "worker-1", ttl: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := repo.ClaimPending(context.Background(), tc.size, tc.owner, tc.ttl); err == nil {
				t.Fatal("ClaimPending() expected validation error")
			}
		})
	}
}

func TestNormalizeOutboxLimit(t *testing.T) {
	for _, tc := range []struct{ input, want int }{
		{0, 100}, {-1, 100}, {50, 50}, {900, 500},
	} {
		if got := normalizeOutboxLimit(tc.input); got != tc.want {
			t.Errorf("normalizeOutboxLimit(%d) = %d, want %d", tc.input, got, tc.want)
		}
	}
}
