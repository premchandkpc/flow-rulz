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
	stopRenew   chan struct{}
	wg          sync.WaitGroup

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
}

// NewLeaseManager creates a new LeaseManager with default replica count (3).
func NewLeaseManager(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	ttl time.Duration,
) *LeaseManager {
	return NewLeaseManagerWithRenewInterval(repo, clock, owner, numShards, ttl, 0)
}

// NewLeaseManagerWithRenewInterval constructs a lease manager with an explicit renewal interval.
// A non-positive renewal interval defaults to one third of the lease TTL.
func NewLeaseManagerWithRenewInterval(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	ttl time.Duration,
	renewInterval time.Duration,
) *LeaseManager {
	return NewLeaseManagerWithReplicaCountAndRenewInterval(repo, clock, owner, numShards, 3, ttl, renewInterval)
}

// NewLeaseManagerWithReplicaCountAndRenewInterval configures the expected fixed
// worker replica count. Every worker targets ceil(numShards/replicaCount) leases.
// This is a fixed-replica allocation policy, not dynamic membership/rebalancing.
func NewLeaseManagerWithReplicaCountAndRenewInterval(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	replicaCount uint32,
	ttl time.Duration,
	renewInterval time.Duration,
) *LeaseManager {
	if numShards == 0 {
		numShards = 4096
	}
	if replicaCount == 0 {
		replicaCount = 3
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if renewInterval <= 0 {
		renewInterval = ttl / 3
	}
	if renewInterval <= 0 {
		renewInterval = ttl
	}
	return &LeaseManager{
		repo:          repo,
		clock:         clock,
		owner:         owner,
		numShards:     numShards,
		replicaCount:  replicaCount,
		ttl:           ttl,
		renewInterval: renewInterval,
		ownedShards:   make(map[uint32]*domain.ShardLease),
		stopRenew:     make(chan struct{}),
	}
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
	m.started = true

	target := m.targetOwnedShards()
	start := uint32(0)
	if m.owner != "" {
		start = domain.ShardHash(m.owner) % m.numShards
	}

	for i := uint32(0); i < target; i++ {
		shardID := (start + i) % m.numShards
		if err := m.acquireShard(ctx, shardID); err != nil {
			if ctx.Err() != nil {
				m.started = false
				return fmt.Errorf("acquire initial shard %d: %w", shardID, ctx.Err())
			}
			// A shard owned by another worker is expected in a shared deployment.
			log.Printf("shard %d: initial acquire skipped: %v", shardID, err)
		}
	}

	m.wg.Add(1)
	go m.renewLoop(ctx)
	return nil
}

// Stop is idempotent, waits for lease renewal to stop, and releases leases with
// a bounded context that remains usable during signal-driven shutdown.
func (m *LeaseManager) Stop(ctx context.Context) error { //nolint:contextcheck // intentionally assign to parameter for fallback
	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopRenew)
	}
	m.lifecycleMu.Unlock()

	m.wg.Wait()

	m.mu.Lock()
	owned := m.ownedShards
	m.ownedShards = make(map[uint32]*domain.ShardLease)
	m.mu.Unlock()

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var firstErr error
	for shardID := range owned {
		if err := m.repo.Release(releaseCtx, shardID, m.owner); err != nil {
			log.Printf("shard %d: release failed: %v", shardID, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
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
	m.mu.Lock()
	m.ownedShards[shard] = lease
	m.mu.Unlock()
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

func (m *LeaseManager) renewOwnedBatch(
	ctx context.Context,
	renewer batchLeaseRenewer,
	leases map[uint32]int64,
) bool {
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

func (m *LeaseManager) applyBatchRenewals(
	expectedLeases map[uint32]int64,
	renewed map[uint32]*domain.ShardLease,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for shardID, expectedToken := range expectedLeases {
		current, stillOwned := m.ownedShards[shardID]
		if !stillOwned || current == nil || current.FencingToken != expectedToken {
			continue
		}
		lease, found := renewed[shardID]
		if !found {
			log.Printf("shard %d: lease lost during batch renewal, removing from owned shards", shardID)
			delete(m.ownedShards, shardID)
			continue
		}
		m.ownedShards[shardID] = lease
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
		m.mu.Lock()
		m.ownedShards[shardID] = lease
		m.mu.Unlock()
	}
}

func (m *LeaseManager) handleIndividualRenewalError(shardID uint32, err error) {
	if errors.Is(err, domain.ErrLeaseExpired) ||
		errors.Is(err, domain.ErrFencingTokenMismatch) ||
		errors.Is(err, domain.ErrLeaseOwnedByOther) {
		log.Printf("shard %d: lease lost (%v), removing from owned shards", shardID, err)
		m.mu.Lock()
		delete(m.ownedShards, shardID)
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
	targetOwned := int(m.targetOwnedShards())

	m.mu.RLock()
	owned := make([]uint32, 0, len(m.ownedShards))
	for shardID := range m.ownedShards {
		owned = append(owned, shardID)
	}
	m.mu.RUnlock()

	if len(owned) > targetOwned {
		// Release highest-numbered shards first for deterministic behavior. A
		// failed release remains in ownedShards and is retried on a later cycle.
		sort.Slice(owned, func(i, j int) bool { return owned[i] > owned[j] })
		excess := len(owned) - targetOwned
		for _, shardID := range owned {
			if excess == 0 {
				break
			}
			if err := m.repo.Release(ctx, shardID, m.owner); err != nil {
				log.Printf("shard %d: release excess lease: %v", shardID, err)
				continue
			}
			m.mu.Lock()
			if _, stillOwned := m.ownedShards[shardID]; stillOwned {
				delete(m.ownedShards, shardID)
				excess--
			}
			m.mu.Unlock()
		}
	}

	m.mu.RLock()
	currentOwned := len(m.ownedShards)
	m.mu.RUnlock()
	if currentOwned >= targetOwned {
		return
	}

	// Try to acquire shards up to our target.
	needed := targetOwned - currentOwned
	for shardID := uint32(0); shardID < m.numShards && needed > 0; shardID++ {
		m.mu.RLock()
		_, alreadyOwned := m.ownedShards[shardID]
		m.mu.RUnlock()
		if alreadyOwned {
			continue
		}
		if err := m.acquireShard(ctx, shardID); err == nil {
			needed--
		}
	}
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
