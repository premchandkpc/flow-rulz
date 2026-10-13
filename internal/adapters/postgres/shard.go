package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// ShardLeaseRepository manages shard lease persistence.
type ShardLeaseRepository struct {
	db Querier
}

// NewShardLeaseRepository creates a new ShardLeaseRepository.
func NewShardLeaseRepository(db Querier) *ShardLeaseRepository {
	return &ShardLeaseRepository{db: db}
}

// Acquire attempts to acquire a shard lease for the given owner.
func (r *ShardLeaseRepository) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	_, err := r.db.Exec(ctx, `
		INSERT INTO shard_leases (virtual_shard, owner, fencing_token, expires_at, routing_epoch)
		VALUES ($1, $2, 1, NOW() + ($3 * INTERVAL '1 millisecond'), 0)
		ON CONFLICT (virtual_shard) DO UPDATE SET
			owner = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.owner ELSE shard_leases.owner END,
			fencing_token = CASE WHEN shard_leases.expires_at < NOW() THEN shard_leases.fencing_token + 1 ELSE shard_leases.fencing_token END,
			expires_at = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.expires_at ELSE shard_leases.expires_at END
	`, shard, owner, ttl.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	return r.GetOwner(ctx, shard)
}

// Renew renews a shard lease if the fencing token matches.
func (r *ShardLeaseRepository) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	tag, err := r.db.Exec(ctx, `
		UPDATE shard_leases
		SET expires_at = NOW() + ($4 * INTERVAL '1 millisecond')
		WHERE virtual_shard = $1 AND owner = $2 AND fencing_token = $3 AND expires_at > NOW()
	`, shard, owner, fencingToken, ttl.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, domain.ErrLeaseExpired
	}
	return r.GetOwner(ctx, shard)
}

// RenewOwned renews the supplied leases in one database round trip. The expected
// fencing token is part of each row match so a stale local snapshot cannot renew
// a lease that has already been reassigned. Rows omitted from the result are lost.
func (r *ShardLeaseRepository) RenewOwned(ctx context.Context, owner string, leases map[uint32]int64, ttl time.Duration) (map[uint32]*domain.ShardLease, error) {
	renewed := make(map[uint32]*domain.ShardLease, len(leases))
	if len(leases) == 0 {
		return renewed, nil
	}

	args := make([]any, 0, 2+2*len(leases))
	args = append(args, owner, ttl.Milliseconds())
	values := make([]string, 0, len(leases))
	for shardID, token := range leases {
		base := len(args) + 1
		values = append(values, fmt.Sprintf("($%d::bigint, $%d::bigint)", base, base+1))
		args = append(args, int64(shardID), token)
	}

	query := fmt.Sprintf(`
		UPDATE shard_leases AS lease
		SET expires_at = NOW() + ($2 * INTERVAL '1 millisecond')
		FROM (VALUES %s) AS requested(virtual_shard, fencing_token)
		WHERE lease.virtual_shard = requested.virtual_shard
		  AND lease.fencing_token = requested.fencing_token
		  AND lease.owner = $1
		  AND lease.expires_at > NOW()
		RETURNING lease.virtual_shard, lease.owner, lease.fencing_token, lease.expires_at, lease.routing_epoch
	`, strings.Join(values, ","))
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("renew owned leases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		lease := &domain.ShardLease{}
		if err := rows.Scan(&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch); err != nil {
			return nil, fmt.Errorf("scan renewed lease: %w", err)
		}
		renewed[lease.VirtualShard] = lease
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate renewed leases: %w", err)
	}
	return renewed, nil
}

// Release expires a shard lease immediately.
func (r *ShardLeaseRepository) Release(ctx context.Context, shard uint32, owner string) error {
	// Preserve the row so the fencing token remains monotonic across release/reacquire.
	// Deleting it would let the next INSERT reset the token to 1 and revive a zombie.
	_, err := r.db.Exec(ctx, `
		UPDATE shard_leases SET expires_at = NOW() - INTERVAL '1 millisecond'
		WHERE virtual_shard = $1 AND owner = $2
	`, shard, owner)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// GetOwner retrieves the current owner of a shard.
func (r *ShardLeaseRepository) GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error) {
	lease := &domain.ShardLease{}
	err := r.db.QueryRow(ctx, `
		SELECT virtual_shard, owner, fencing_token, expires_at, routing_epoch
		FROM shard_leases
		WHERE virtual_shard = $1
	`, shard).Scan(
		&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("get owner: %w", err)
	}
	return lease, nil
}

// ValidateFencingToken checks if the fencing token matches the current lease.
func (r *ShardLeaseRepository) ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	var currentToken int64
	err := r.db.QueryRow(ctx, `
		SELECT fencing_token FROM shard_leases
		WHERE virtual_shard = $1 AND owner = $2 AND expires_at > NOW()
	`, shard, owner).Scan(&currentToken)
	if err != nil {
		return fmt.Errorf("validate fencing token: %w", err)
	}
	if currentToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

// AssertHeld validates the lease is held by the owner with the given fencing token.
// Uses row lock (FOR SHARE) to prevent stale holders from proceeding. Fails closed.
// AssertHeld validates the lease is held by the owner with the given fencing token.
func (r *ShardLeaseRepository) AssertHeld(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	var currentToken int64
	var expiresAt time.Time
	var active bool
	err := r.db.QueryRow(ctx, `
		SELECT fencing_token, expires_at, expires_at > NOW()
		FROM shard_leases
		WHERE virtual_shard = $1 AND owner = $2
		FOR SHARE
	`, shard, owner).Scan(&currentToken, &expiresAt, &active)
	if err != nil {
		return fmt.Errorf("assert held: %w", err)
	}
	// Fail closed: token <= 0 is invalid, lease must not be expired
	if fencingToken <= 0 {
		return domain.ErrFencingTokenMismatch
	}
	if !active {
		return domain.ErrLeaseExpired
	}
	if currentToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

// ListOwned returns all active shards owned by the given owner.
func (r *ShardLeaseRepository) ListOwned(ctx context.Context, owner string) ([]*domain.ShardLease, error) {
	rows, err := r.db.Query(ctx, `
		SELECT virtual_shard, owner, fencing_token, expires_at, routing_epoch
		FROM shard_leases
		WHERE owner = $1 AND expires_at > NOW()
	`, owner)
	if err != nil {
		return nil, fmt.Errorf("list owned leases: %w", err)
	}
	defer rows.Close()

	var leases []*domain.ShardLease
	for rows.Next() {
		lease := &domain.ShardLease{}
		if err := rows.Scan(
			&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch,
		); err != nil {
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate owned leases: %w", err)
	}
	return leases, nil
}
