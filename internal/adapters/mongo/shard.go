package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// ShardLeaseRepository manages distributed shard leases in MongoDB.
type ShardLeaseRepository struct {
	querier Querier
}

// NewShardLeaseRepository creates a shard lease repository.
func NewShardLeaseRepository(querier Querier) *ShardLeaseRepository {
	return &ShardLeaseRepository{querier: querier}
}

func (r *ShardLeaseRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Acquire claims a shard lease for an owner for the requested duration.
func (r *ShardLeaseRepository) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	coll := r.collection(collectionShardLeases)
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	filter := bson.M{
		"virtual_shard": shard,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}},
			{"owner": owner},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"owner":      owner,
			"expires_at": expiresAt,
		},
		"$inc": bson.M{
			"fencing_token": 1,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var doc LeaseDoc
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, domain.ErrLeaseOwnedByOther
		}
		return nil, fmt.Errorf("acquire lease: %w", err)
	}

	if doc.Owner != owner {
		return nil, domain.ErrLeaseOwnedByOther
	}

	return doc.ToDomain(), nil
}

// Renew extends a lease if the owner and fencing token still match.
func (r *ShardLeaseRepository) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	coll := r.collection(collectionShardLeases)
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	filter := bson.M{
		"virtual_shard": shard,
		"owner":         owner,
		"fencing_token": fencingToken,
		"expires_at":    bson.M{"$gt": now},
	}

	update := bson.M{
		"$set": bson.M{
			"expires_at": expiresAt,
		},
	}

	result, err := coll.UpdateOne(ctx, filter, bson.M{"$set": update["$set"]})
	if err != nil {
		return nil, fmt.Errorf("renew lease: %w", err)
	}
	if result.MatchedCount == 0 {
		return nil, domain.ErrLeaseExpired
	}

	return r.GetOwner(ctx, shard)
}

// Release relinquishes a shard lease held by the specified owner.
func (r *ShardLeaseRepository) Release(ctx context.Context, shard uint32, owner string) error {
	coll := r.collection(collectionShardLeases)
	_, err := coll.UpdateOne(ctx, bson.M{
		"virtual_shard": shard,
		"owner":         owner,
	}, bson.M{"$set": bson.M{
		"owner":      "",
		"expires_at": time.Now().UTC(),
	}})
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// GetOwner returns the current lease for a shard, if one exists.
func (r *ShardLeaseRepository) GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error) {
	coll := r.collection(collectionShardLeases)
	var doc LeaseDoc
	err := coll.FindOne(ctx, bson.M{
		"virtual_shard": shard,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get owner: %w", err)
	}
	return doc.ToDomain(), nil
}

// ValidateFencingToken verifies that the owner and fencing token match the current lease.
func (r *ShardLeaseRepository) ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	coll := r.collection(collectionShardLeases)
	var doc LeaseDoc
	err := coll.FindOne(ctx, bson.M{
		"virtual_shard": shard,
		"owner":         owner,
		"fencing_token": fencingToken,
		"expires_at":    bson.M{"$gt": time.Now().UTC()},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return domain.ErrFencingTokenMismatch
		}
		return fmt.Errorf("validate fencing token: %w", err)
	}
	return nil
}

// AssertHeld validates the lease is held by the owner with the given fencing token.
// Uses find with filter to ensure lease is valid. Fails closed.
func (r *ShardLeaseRepository) AssertHeld(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	coll := r.collection(collectionShardLeases)
	var doc LeaseDoc
	err := coll.FindOne(ctx, bson.M{
		"virtual_shard": shard,
		"owner":         owner,
		"fencing_token": fencingToken,
		"expires_at":    bson.M{"$gt": time.Now().UTC()},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return domain.ErrFencingTokenMismatch
		}
		return fmt.Errorf("assert held: %w", err)
	}
	if fencingToken <= 0 {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

// LeaseDoc is the MongoDB representation of a shard lease.
type LeaseDoc struct {
	VirtualShard uint32    `bson:"virtual_shard"`
	Owner        string    `bson:"owner"`
	FencingToken int64     `bson:"fencing_token"`
	ExpiresAt    time.Time `bson:"expires_at"`
	RoutingEpoch int64     `bson:"routing_epoch"`
}

// ToDomain converts the MongoDB lease document to a domain shard lease.
func (d *LeaseDoc) ToDomain() *domain.ShardLease {
	return &domain.ShardLease{
		VirtualShard: d.VirtualShard,
		Owner:        d.Owner,
		FencingToken: d.FencingToken,
		ExpiresAt:    d.ExpiresAt,
		RoutingEpoch: d.RoutingEpoch,
	}
}
