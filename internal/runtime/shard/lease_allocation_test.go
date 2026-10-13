package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestTargetOwnedShardsUsesConfiguredReplicaCount(t *testing.T) {
	tests := []struct {
		name        string
		numShards   uint32
		replicas    uint32
		wantPerNode uint32
	}{
		{name: "default-sized shard set with three replicas", numShards: 4096, replicas: 3, wantPerNode: 1366},
		{name: "single replica owns every shard", numShards: 4096, replicas: 1, wantPerNode: 4096},
		{name: "non-divisible shard count rounds up", numShards: 10, replicas: 4, wantPerNode: 3},
		{name: "more replicas than shards still targets one lease", numShards: 1, replicas: 3, wantPerNode: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewLeaseManagerWithReplicaCountAndRenewInterval(
				nil,
				nil,
				"worker-test",
				tt.numShards,
				tt.replicas,
				time.Second,
				time.Millisecond,
			)
			if got := manager.targetOwnedShards(); got != tt.wantPerNode {
				t.Fatalf("targetOwnedShards() = %d, want %d", got, tt.wantPerNode)
			}
		})
	}
}

func TestLeaseManagerDefaultsInvalidReplicaCount(t *testing.T) {
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(
		nil,
		nil,
		"worker-test",
		10,
		0,
		time.Second,
		time.Millisecond,
	)
	if got, want := manager.replicaCount, uint32(3); got != want {
		t.Fatalf("replicaCount = %d, want default %d", got, want)
	}
}

type leaseAllocationTestRepo struct {
	released     []uint32
	releaseErr   error
	batchCalls   int
	batchRenewed map[uint32]*domain.ShardLease
	batchErr     error
	renewCalls   int
}

func (r *leaseAllocationTestRepo) Acquire(context.Context, uint32, string, time.Duration) (*domain.ShardLease, error) {
	return nil, errors.New("unexpected acquire")
}
func (r *leaseAllocationTestRepo) Renew(context.Context, uint32, string, int64, time.Duration) (*domain.ShardLease, error) {
	r.renewCalls++
	return nil, errors.New("unexpected per-shard renew")
}
func (r *leaseAllocationTestRepo) RenewOwned(_ context.Context, _ string, _ map[uint32]int64, _ time.Duration) (map[uint32]*domain.ShardLease, error) {
	r.batchCalls++
	if r.batchErr != nil {
		return nil, r.batchErr
	}
	return r.batchRenewed, nil
}
func (r *leaseAllocationTestRepo) Release(_ context.Context, shard uint32, _ string) error {
	if r.releaseErr != nil {
		return r.releaseErr
	}
	r.released = append(r.released, shard)
	return nil
}
func (r *leaseAllocationTestRepo) GetOwner(context.Context, uint32) (*domain.ShardLease, error) {
	return nil, nil
}
func (r *leaseAllocationTestRepo) ValidateFencingToken(context.Context, uint32, string, int64) error {
	return nil
}
func (r *leaseAllocationTestRepo) AssertHeld(context.Context, uint32, string, int64) error {
	return nil
}

func TestRenewOwnedUsesBatchAndDropsLeasesMissingFromDatabaseResult(t *testing.T) {
	repo := &leaseAllocationTestRepo{batchRenewed: map[uint32]*domain.ShardLease{
		1: {VirtualShard: 1, Owner: "worker-test", FencingToken: 7, ExpiresAt: time.Now().Add(time.Minute)},
	}}
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(repo, nil, "worker-test", 4, 1, time.Minute, time.Second)
	manager.ownedShards = map[uint32]*domain.ShardLease{
		1: {VirtualShard: 1, Owner: "worker-test", FencingToken: 7},
		2: {VirtualShard: 2, Owner: "worker-test", FencingToken: 9},
	}

	manager.renewOwned(context.Background())

	if repo.batchCalls != 1 || repo.renewCalls != 0 {
		t.Fatalf("renew calls: batch=%d per-shard=%d, want batch=1 per-shard=0", repo.batchCalls, repo.renewCalls)
	}
	if !manager.IsOwner(1) {
		t.Fatal("lease returned by batch renewal should remain owned")
	}
	if token, ok := manager.GetFencingToken(1); !ok || token != 7 {
		t.Fatalf("renewed fencing token = %d, owned=%v; want 7, true", token, ok)
	}
	if manager.IsOwner(2) {
		t.Fatal("lease omitted from batch renewal result must be removed as lost")
	}
}

func TestRebalanceReleasesExcessLeasesWhenTargetShrinks(t *testing.T) {
	repo := &leaseAllocationTestRepo{}
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(repo, nil, "worker-test", 10, 5, time.Second, time.Millisecond)
	manager.ownedShards = map[uint32]*domain.ShardLease{
		1: {VirtualShard: 1, Owner: "worker-test", FencingToken: 1},
		2: {VirtualShard: 2, Owner: "worker-test", FencingToken: 1},
		9: {VirtualShard: 9, Owner: "worker-test", FencingToken: 1},
	}

	manager.rebalanceIfNeeded(context.Background())

	if len(repo.released) != 1 || repo.released[0] != 9 {
		t.Fatalf("released shards = %v, want [9]", repo.released)
	}
	if manager.IsOwner(9) {
		t.Fatal("worker must stop advertising the successfully released shard")
	}
	if got := len(manager.OwnedShards()); got != 2 {
		t.Fatalf("owned shard count = %d, want 2", got)
	}
}

func TestRebalanceRetainsLeaseWhenReleaseFails(t *testing.T) {
	repo := &leaseAllocationTestRepo{releaseErr: errors.New("database unavailable")}
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(repo, nil, "worker-test", 10, 5, time.Second, time.Millisecond)
	manager.ownedShards = map[uint32]*domain.ShardLease{
		1: {VirtualShard: 1, Owner: "worker-test", FencingToken: 1},
		2: {VirtualShard: 2, Owner: "worker-test", FencingToken: 1},
		9: {VirtualShard: 9, Owner: "worker-test", FencingToken: 1},
	}

	manager.rebalanceIfNeeded(context.Background())

	if !manager.IsOwner(9) {
		t.Fatal("failed release must retain the local lease until ownership is relinquished")
	}
	if got := len(manager.OwnedShards()); got != 3 {
		t.Fatalf("owned shard count = %d, want 3", got)
	}
}
