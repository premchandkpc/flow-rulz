// Package shard provides shard lease management with fencing tokens.
// It handles lease acquisition, renewal, and rebalancing across a fixed set of worker replicas.
package shard

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// LeaseManager acquires and renews shard leases with fencing tokens.
// It uses a fixed-replica allocation policy where each worker targets
// ceil(numShards/replicaCount) leases.
type LeaseManager struct {
	repo          ports.ShardLeaseRepository
	clock         ports.Clock
	owner         string
	numShards     uint32
	replicaCount  uint32
	ttl           time.Duration
	renewInterval time.Duration

	mu          sync.RWMutex
	ownedShards map[uint32]*domain.ShardLease
	recovered   map[uint32]struct{}
	stopRenew   chan struct{}
	wg          sync.WaitGroup

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
}

// NewLeaseManager creates a new LeaseManager with default replica count (3).
func NewLeaseManager(repo ports.ShardLeaseRepository, clock ports.Clock, owner string, numShards uint32, ttl time.Duration) *LeaseManager {
	return NewLeaseManagerWithRenewInterval(repo, clock, owner, numShards, ttl, 0)
}

// NewLeaseManagerWithRenewInterval constructs a lease manager with an explicit renewal interval.
// A non-positive renewal interval defaults to one third of the lease TTL.
func NewLeaseManagerWithRenewInterval(repo ports.ShardLeaseRepository, clock ports.Clock, owner string, numShards uint32, ttl time.Duration, renewInterval time.Duration) *LeaseManager {
	return NewLeaseManagerWithReplicaCountAndRenewInterval(repo, clock, owner, numShards, 3, ttl, renewInterval)
}

// NewLeaseManagerWithReplicaCountAndRenewInterval configures the expected fixed
// worker replica count. Every worker targets ceil(numShards/replicaCount) leases.
// This is a fixed-replica allocation policy, not dynamic membership/rebalancing.
func NewLeaseManagerWithReplicaCountAndRenewInterval(repo ports.ShardLeaseRepository, clock ports.Clock, owner string, numShards uint32, replicaCount uint32, ttl time.Duration, renewInterval time.Duration) *LeaseManager {
	m := &LeaseManager{repo: repo, clock: clock, owner: owner, numShards: numShards,
		replicaCount: replicaCount, ttl: ttl, renewInterval: renewInterval,
		ownedShards: make(map[uint32]*domain.ShardLease), recovered: make(map[uint32]struct{}), stopRenew: make(chan struct{})}
	m.normalizeAllocation()
	m.normalizeTiming()
	return m
}

// Start acquires initial shard leases and begins the renewal loop.
func (m *LeaseManager) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("start lease manager: context is nil")
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stopped {
		return errors.New("start lease manager: manager is stopped")
	}
	if m.started {
		return nil
	}
	return m.startInitialLeases(ctx)
}

// Stop is idempotent, waits for lease renewal to stop, and releases leases with
// a bounded context that remains usable during signal-driven shutdown.
func (m *LeaseManager) Stop(ctx context.Context) error { //nolint:contextcheck // intentionally assign to parameter for fallback
	if ctx == nil {
		ctx = context.Background()
	}
	m.stopRenewal()
	owned := m.detachOwnedLeases()
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return m.releaseOwnedLeases(releaseCtx, owned)
}

func (m *LeaseManager) acquireShard(ctx context.Context, shard uint32) error {
	lease, err := m.repo.Acquire(ctx, shard, m.owner, m.ttl)
	if err != nil {
		return err
	}
	if lease == nil {
		return errors.New("acquire shard: repository returned a nil lease")
	}
	if lease.Owner != m.owner {
		return domain.ErrLeaseOwnedByOther
	}
	m.rememberLease(shard, lease)
	log.Printf("acquired shard %d (fencing_token=%d)", shard, lease.FencingToken)
	return nil
}

func (m *LeaseManager) renewLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopRenew:
			return
		case <-ticker.C:
			m.renewOwned(ctx)
		}
	}
}

type batchLeaseRenewer interface {
	RenewOwned(ctx context.Context, owner string, leases map[uint32]int64, ttl time.Duration) (map[uint32]*domain.ShardLease, error)
}

func (m *LeaseManager) renewOwned(ctx context.Context) {
	leases := m.ownedLeaseTokens()
	if renewer, ok := m.repo.(batchLeaseRenewer); ok {
		if !m.renewOwnedBatch(ctx, renewer, leases) {
			return
		}
	} else {
		m.renewOwnedIndividually(ctx, leases)
	}
	m.rebalanceIfNeeded(ctx)
	m.recoverExpiredShards(ctx)
}

func (m *LeaseManager) ownedLeaseTokens() map[uint32]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	leases := make(map[uint32]int64, len(m.ownedShards))
	for shardID, lease := range m.ownedShards {
		if lease != nil {
			leases[shardID] = lease.FencingToken
		}
	}
	return leases
}

func (m *LeaseManager) renewOwnedBatch(ctx context.Context, renewer batchLeaseRenewer, leases map[uint32]int64) bool {
	// Batch renewal avoids a serial two-round-trip loop that can outlast the TTL.
	renewed, err := renewer.RenewOwned(ctx, m.owner, leases, m.ttl)
	if err != nil {
		// Failed batch results are ambiguous, so do not rebalance on stale state.
		log.Printf("renew owned shards: %v", err)
		return false
	}
	m.applyBatchRenewals(leases, renewed)
	return true
}

func (m *LeaseManager) applyBatchRenewals(expectedLeases map[uint32]int64, renewed map[uint32]*domain.ShardLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for shardID, expectedToken := range expectedLeases {
		m.applyRenewalLocked(shardID, expectedToken, renewed)
	}
}

func (m *LeaseManager) renewOwnedIndividually(ctx context.Context, leases map[uint32]int64) {
	for shardID, fencingToken := range leases {
		lease, err := m.repo.Renew(ctx, shardID, m.owner, fencingToken, m.ttl)
		if err != nil {
			m.handleIndividualRenewalError(shardID, err)
			continue
		}
		if lease == nil {
			log.Printf("shard %d: renew returned an empty lease; retaining current ownership snapshot", shardID)
			continue
		}
		m.rememberLease(shardID, lease)
	}
}

func (m *LeaseManager) handleIndividualRenewalError(shardID uint32, err error) {
	if errors.Is(err, domain.ErrLeaseExpired) ||
		errors.Is(err, domain.ErrFencingTokenMismatch) ||
		errors.Is(err, domain.ErrLeaseOwnedByOther) {
		log.Printf("shard %d: lease lost (%v), removing from owned shards", shardID, err)
		m.mu.Lock()
		delete(m.ownedShards, shardID)
		delete(m.recovered, shardID)
		m.mu.Unlock()
		return
	}
	log.Printf("shard %d: renew failed (transient): %v", shardID, err)
}

// targetOwnedShards rounds up so the configured replicas can cover every shard,
// including configurations where the shard count is not divisible by replicas.
func (m *LeaseManager) targetOwnedShards() uint32 {
	target := m.numShards / m.replicaCount
	if m.numShards%m.replicaCount != 0 {
		target++
	}
	if target == 0 {
		return 1
	}
	return target
}

// rebalanceIfNeeded reconciles this worker's lease count to the configured
// fixed-replica target. It does not discover membership or coordinate a cluster-wide
// rebalance; every worker must use the same WORKER_REPLICAS value.
func (m *LeaseManager) rebalanceIfNeeded(ctx context.Context) {
	target := int(m.targetOwnedShards())
	owned := m.baseOwnedShards()
	if len(owned) > target {
		m.releaseExcessLeases(ctx, owned, target)
	}
	m.mu.RLock()
	needed := target - len(m.ownedShards)
	m.mu.RUnlock()
	m.acquireMissingLeases(ctx, needed)
}

// expiredLeaseAcquirer is an optional database capability for reclaiming
// expired shards even when this live worker is already at its nominal target.
type expiredLeaseAcquirer interface {
	AcquireExpired(context.Context, string, time.Duration, int) ([]*domain.ShardLease, error)
}

func (m *LeaseManager) recoverExpiredShards(ctx context.Context) {
	acquirer, ok := m.repo.(expiredLeaseAcquirer)
	if !ok {
		return
	}
	leases, err := acquirer.AcquireExpired(ctx, m.owner, m.ttl, 256)
	if err != nil {
		log.Printf("recover expired shard leases: %v", err)
		return
	}
	m.rememberRecoveredShards(leases)
}

func (m *LeaseManager) rememberRecoveredShards(leases []*domain.ShardLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, lease := range leases {
		if lease == nil || lease.Owner != m.owner {
			continue
		}
		m.ownedShards[lease.VirtualShard] = lease
		m.recovered[lease.VirtualShard] = struct{}{}
	}
}

func (m *LeaseManager) baseOwnedShards() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []uint32
	for shard := range m.ownedShards {
		if _, recovered := m.recovered[shard]; !recovered {
			result = append(result, shard)
		}
	}
	return result
}

// OwnedShards returns the list of shard IDs currently owned by this lease manager.
func (m *LeaseManager) OwnedShards() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shards := make([]uint32, 0, len(m.ownedShards))
	for shard := range m.ownedShards {
		shards = append(shards, shard)
	}
	return shards
}

// IsOwner returns true if this lease manager owns the given shard.
func (m *LeaseManager) IsOwner(shard uint32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.ownedShards[shard]
	return ok
}

// GetFencingToken returns the fencing token for a shard if owned.
func (m *LeaseManager) GetFencingToken(shard uint32) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lease, ok := m.ownedShards[shard]
	if !ok {
		return 0, false
	}
	return lease.FencingToken, true
}

// VirtualShardForEvent computes the virtual shard for an event envelope.
func (m *LeaseManager) VirtualShardForEvent(env *domain.EventEnvelope) uint32 {
	return env.VirtualShard(m.numShards)
}

func (m *LeaseManager) normalizeAllocation() {
	if m.numShards == 0 {
		m.numShards = 4096
	}
	if m.replicaCount == 0 {
		m.replicaCount = 3
	}
}

func (m *LeaseManager) normalizeTiming() {
	if m.ttl <= 0 {
		m.ttl = 30 * time.Second
	}
	if m.renewInterval <= 0 {
		m.renewInterval = m.ttl / 3
	}
	if m.renewInterval <= 0 {
		m.renewInterval = m.ttl
	}
}

func (m *LeaseManager) startInitialLeases(ctx context.Context) error {
	m.started = true
	if err := m.acquireInitialLeases(ctx); err != nil {
		m.started = false
		return err
	}
	m.wg.Add(1)
	go m.renewLoop(ctx)
	return nil
}

func (m *LeaseManager) acquireInitialLeases(ctx context.Context) error {
	start := uint32(0)
	if m.owner != "" {
		start = domain.ShardHash(m.owner) % m.numShards
	}
	for i := uint32(0); i < m.targetOwnedShards(); i++ {
		if err := m.acquireInitialShard(ctx, (start+i)%m.numShards); err != nil {
			return err
		}
	}
	return nil
}

func (m *LeaseManager) acquireInitialShard(ctx context.Context, shardID uint32) error {
	if err := m.acquireShard(ctx, shardID); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("acquire initial shard %d: %w", shardID, ctx.Err())
		}
		// Contention with other workers is expected in a shared deployment.
		log.Printf("shard %d: initial acquire skipped: %v", shardID, err)
	}
	return nil
}

func (m *LeaseManager) stopRenewal() {
	m.lifecycleMu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopRenew)
	}
	m.lifecycleMu.Unlock()
	m.wg.Wait()
}

func (m *LeaseManager) detachOwnedLeases() map[uint32]*domain.ShardLease {
	m.mu.Lock()
	defer m.mu.Unlock()
	owned := m.ownedShards
	m.ownedShards = make(map[uint32]*domain.ShardLease)
	return owned
}

func (m *LeaseManager) releaseOwnedLeases(ctx context.Context, owned map[uint32]*domain.ShardLease) error {
	var firstErr error
	for shardID := range owned {
		if err := m.repo.Release(ctx, shardID, m.owner); err != nil {
			log.Printf("shard %d: release failed: %v", shardID, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m *LeaseManager) rememberLease(shard uint32, lease *domain.ShardLease) {
	m.mu.Lock()
	m.ownedShards[shard] = lease
	m.mu.Unlock()
}

func (m *LeaseManager) applyRenewalLocked(shardID uint32, expectedToken int64, renewed map[uint32]*domain.ShardLease) {
	current, stillOwned := m.ownedShards[shardID]
	if !stillOwned || current == nil || current.FencingToken != expectedToken {
		return
	}
	lease, found := renewed[shardID]
	if !found {
		log.Printf("shard %d: lease lost during batch renewal, removing from owned shards", shardID)
		delete(m.ownedShards, shardID)
		delete(m.recovered, shardID)
		return
	}
	m.ownedShards[shardID] = lease
}

func (m *LeaseManager) releaseExcessLeases(ctx context.Context, owned []uint32, target int) {
	// Release highest-numbered shards first; retain failed releases for the next cycle.
	sort.Slice(owned, func(i, j int) bool { return owned[i] > owned[j] })
	excess := len(owned) - target
	for _, shardID := range owned {
		if excess == 0 {
			break
		}
		if m.releaseExcessLease(ctx, shardID) {
			excess--
		}
	}
}

func (m *LeaseManager) releaseExcessLease(ctx context.Context, shardID uint32) bool {
	if err := m.repo.Release(ctx, shardID, m.owner); err != nil {
		log.Printf("shard %d: release excess lease: %v", shardID, err)
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, stillOwned := m.ownedShards[shardID]
	delete(m.ownedShards, shardID)
	delete(m.recovered, shardID)
	return stillOwned
}

func (m *LeaseManager) acquireMissingLeases(ctx context.Context, needed int) {
	for shardID := uint32(0); shardID < m.numShards && needed > 0; shardID++ {
		if m.IsOwner(shardID) {
			continue
		}
		if err := m.acquireShard(ctx, shardID); err == nil {
			needed--
		}
	}
}
