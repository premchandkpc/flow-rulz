package shard

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const (
	failoverNumShards     = 30
	failoverReplicas      = 3
	failoverLeaseTTL      = time.Second
	failoverRenewInterval = 50 * time.Millisecond
	failoverSettleTimeout = 5 * time.Second
)

// TestSurvivorsReclaimExpiredShardsAfterWorkerCrash kills one of three workers
// and asserts that its shards move to the survivors within two lease TTLs.
// It documents the fixed-replica failover contract: survivors at their
// ceil(numShards/replicas) target must still reclaim a crashed peer's shards.
func TestSurvivorsReclaimExpiredShardsAfterWorkerCrash(t *testing.T) {
	repo := newSharedLeaseRepo()
	stopA := startTestWorker(t, repo, "worker-a")
	stopB := startTestWorker(t, repo, "worker-b")
	stopC := startTestWorker(t, repo, "worker-c")
	defer stopA()
	defer stopB()
	defer stopC()
	waitFor(t, "settle into an even three-way split", failoverSettleTimeout,
		evenSplit(repo))
	stopC() // Crash: cancel only. Stop() would release leases, which a crash cannot.
	waitFor(t, "survivors reclaim the crashed worker's shards",
		2*failoverLeaseTTL, survivorsHoldAll(repo))
}

// startTestWorker starts a lease manager under a cancellable context and
// returns the cancel function. Stop is deliberately unused: cancelling the
// context kills the renewal loop without releasing leases, like a crash.
func startTestWorker(t *testing.T, repo ports.ShardLeaseRepository, owner string) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(
		repo, nil, owner, failoverNumShards, failoverReplicas,
		failoverLeaseTTL, failoverRenewInterval)
	if err := manager.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start %s: %v", owner, err)
	}
	return cancel
}

// waitFor polls condition until it holds or the timeout elapses, failing the
// test with the last diagnostic detail.
func waitFor(t *testing.T, step string, timeout time.Duration, condition func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, detail := condition()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s", step, detail)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// evenSplit reports whether each of the three workers holds exactly the
// ceil(30/3) target before the crash.
func evenSplit(repo *sharedLeaseRepo) func() (bool, string) {
	return func() (bool, string) {
		counts := repo.ownedCounts()
		target := failoverNumShards / failoverReplicas
		split := counts["worker-a"] == target &&
			counts["worker-b"] == target &&
			counts["worker-c"] == target
		return split, fmt.Sprintf("owned counts: %v, union: %d/%d",
			counts, repo.unionSize(), failoverNumShards)
	}
}

// survivorsHoldAll reports whether worker-a and worker-b together hold every
// shard after the crash, with a diagnostic naming the stranded leases.
func survivorsHoldAll(repo *sharedLeaseRepo) func() (bool, string) {
	return func() (bool, string) {
		held := repo.unionOf("worker-a", "worker-b")
		return held == failoverNumShards,
			fmt.Sprintf("survivors hold %d/%d shards; %s",
				held, failoverNumShards, crashedDetail(repo))
	}
}

// crashedDetail lists the shards the crashed worker still owns and which of
// them have already expired.
func crashedDetail(repo *sharedLeaseRepo) string {
	return fmt.Sprintf("crashed worker-c still owns shards %v (expired: %v)",
		repo.ownerShards("worker-c"), repo.expiredShards("worker-c"))
}

// sharedLeaseRepo is an in-memory stand-in for the shard_leases table. Acquire
// mirrors shardLeaseRepositoryAcquireSQL: an unexpired row keeps its owner, an
// expired row is taken over with fencing_token+1, and a missing row is
// inserted with token 1.
type sharedLeaseRepo struct {
	mu     sync.Mutex
	leases map[uint32]*domain.ShardLease
}

var _ ports.ShardLeaseRepository = (*sharedLeaseRepo)(nil)

func newSharedLeaseRepo() *sharedLeaseRepo {
	return &sharedLeaseRepo{leases: make(map[uint32]*domain.ShardLease)}
}

func (r *sharedLeaseRepo) Acquire(_ context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, ok := r.leases[shard]; ok && lease.ExpiresAt.After(time.Now()) {
		return cloneLease(lease), nil // Unexpired: the current owner keeps the row.
	}
	return cloneLease(r.claimLocked(shard, owner, ttl)), nil
}

// claimLocked inserts a missing lease row or takes an expired row over with
// a bumped fencing token; the caller must hold r.mu.
func (r *sharedLeaseRepo) claimLocked(shard uint32, owner string, ttl time.Duration) *domain.ShardLease {
	if _, ok := r.leases[shard]; !ok {
		lease := &domain.ShardLease{VirtualShard: shard, Owner: owner, FencingToken: 1}
		r.leases[shard] = lease
		lease.ExpiresAt = time.Now().Add(ttl)
		return lease
	}
	return r.takeoverLocked(shard, owner, ttl)
}

// takeoverLocked reassigns an expired row to owner with a bumped fencing token
// and a fresh expiry, returning a clone; the caller must hold r.mu.
func (r *sharedLeaseRepo) takeoverLocked(shard uint32, owner string, ttl time.Duration) *domain.ShardLease {
	lease := r.leases[shard]
	lease.Owner, lease.FencingToken = owner, lease.FencingToken+1
	lease.ExpiresAt = time.Now().Add(ttl)
	return cloneLease(lease)
}

// AcquireExpired implements the same optional recovery capability as the
// PostgreSQL repository. Without it, this fake cannot exercise survivor takeover.
func (r *sharedLeaseRepo) AcquireExpired(_ context.Context, owner string, ttl time.Duration, limit int) ([]*domain.ShardLease, error) {
	if limit <= 0 {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	acquired := make([]*domain.ShardLease, 0, limit)
	for _, shard := range r.expiredShardsLocked() {
		if len(acquired) >= limit {
			break
		}
		acquired = append(acquired, r.takeoverLocked(shard, owner, ttl))
	}
	return acquired, nil
}

// expiredShardsLocked returns every expired shard in ascending order; the
// caller must hold r.mu.
func (r *sharedLeaseRepo) expiredShardsLocked() []uint32 {
	now := time.Now()
	return sortedShards(r.leases, func(lease *domain.ShardLease) bool {
		return lease.ExpiresAt.Before(now)
	})
}

func (r *sharedLeaseRepo) Renew(_ context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shard]
	if !ok || lease.Owner != owner || lease.FencingToken != fencingToken ||
		!lease.ExpiresAt.After(time.Now()) {
		return nil, domain.ErrLeaseExpired
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return cloneLease(lease), nil
}

func (r *sharedLeaseRepo) Release(_ context.Context, shard uint32, owner string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, ok := r.leases[shard]; ok && lease.Owner == owner {
		lease.ExpiresAt = time.Now().Add(-time.Second)
	}
	return nil
}

func (r *sharedLeaseRepo) GetOwner(_ context.Context, shard uint32) (*domain.ShardLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shard]
	if !ok {
		return nil, fmt.Errorf("get owner: shard %d has no lease row", shard)
	}
	return cloneLease(lease), nil
}

func (r *sharedLeaseRepo) ValidateFencingToken(_ context.Context, shard uint32, owner string, fencingToken int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shard]
	if !ok || lease.Owner != owner || !lease.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("validate fencing token: shard %d not held by %s", shard, owner)
	}
	if lease.FencingToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

func (r *sharedLeaseRepo) AssertHeld(_ context.Context, shard uint32, owner string, fencingToken int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[shard]
	if !ok || lease.Owner != owner || lease.FencingToken != fencingToken ||
		!lease.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("assert held: shard %d not held by %s with token %d",
			shard, owner, fencingToken)
	}
	return nil
}

func (r *sharedLeaseRepo) ownedCounts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make(map[string]int)
	for _, lease := range r.leases {
		counts[lease.Owner]++
	}
	return counts
}

func (r *sharedLeaseRepo) ownerShards(owner string) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return sortedShards(r.leases, func(lease *domain.ShardLease) bool {
		return lease.Owner == owner
	})
}

func (r *sharedLeaseRepo) expiredShards(owner string) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	return sortedShards(r.leases, func(lease *domain.ShardLease) bool {
		return lease.Owner == owner && !lease.ExpiresAt.After(now)
	})
}

// unionOf counts distinct shards whose lease row belongs to any named owner.
func (r *sharedLeaseRepo) unionOf(owners ...string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	members := make(map[string]bool, len(owners))
	for _, owner := range owners {
		members[owner] = true
	}
	count := 0
	for _, lease := range r.leases {
		if members[lease.Owner] {
			count++
		}
	}
	return count
}

func (r *sharedLeaseRepo) unionSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.leases)
}

func sortedShards(leases map[uint32]*domain.ShardLease, match func(*domain.ShardLease) bool) []uint32 {
	shards := make([]uint32, 0, len(leases))
	for shard, lease := range leases {
		if match(lease) {
			shards = append(shards, shard)
		}
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}

func cloneLease(lease *domain.ShardLease) *domain.ShardLease {
	copied := *lease
	return &copied
}
