package shard

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type expiredRecoveryRepo struct {
	leaseAllocationTestRepo
	expired []*domain.ShardLease
}

func (r *expiredRecoveryRepo) AcquireExpired(_ context.Context, _ string, _ time.Duration, limit int) ([]*domain.ShardLease, error) {
	if limit < 1 {
		return nil, nil
	}
	return r.expired, nil
}

func TestRecoverExpiredLeasesBeyondReplicaTarget(t *testing.T) {
	repo := &expiredRecoveryRepo{expired: []*domain.ShardLease{
		{VirtualShard: 3, Owner: "survivor", FencingToken: 4},
		{VirtualShard: 4, Owner: "survivor", FencingToken: 9},
	}}
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(repo, nil, "survivor", 6, 3, time.Second, time.Millisecond)
	manager.ownedShards[0] = &domain.ShardLease{VirtualShard: 0, Owner: "survivor", FencingToken: 1}
	manager.ownedShards[1] = &domain.ShardLease{VirtualShard: 1, Owner: "survivor", FencingToken: 1}
	manager.recoverExpiredShards(context.Background())
	if len(manager.OwnedShards()) != 4 {
		t.Fatalf("owned %d shards after recovery, want 4", len(manager.OwnedShards()))
	}
	manager.rebalanceIfNeeded(context.Background())
	if len(repo.released) != 0 {
		t.Fatalf("recoveries must not be shed to static target: %v", repo.released)
	}
	if token, ok := manager.GetFencingToken(4); !ok || token != 9 {
		t.Fatalf("fencing token after recovery = %d, owned=%v", token, ok)
	}
}
