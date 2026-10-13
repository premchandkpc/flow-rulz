package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// OutboxRepository stores and manages pending side effects.
type OutboxRepository struct {
	querier Querier
}

// NewOutboxRepository creates and returns the configured repository.
func NewOutboxRepository(q Querier) *OutboxRepository {
	return &OutboxRepository{querier: q}
}

func (r *OutboxRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Insert persists or updates the corresponding record.
func (r *OutboxRepository) Insert(ctx context.Context, effects []domain.OutboxEffect) error {
	coll := r.collection(collectionOutboxEffects)
	if len(effects) == 0 {
		return nil
	}

	docs := make([]interface{}, len(effects))
	for i, ef := range effects {
		if ef.TenantID == "" {
			return fmt.Errorf("insert outbox: tenant_id is required")
		}
		docs[i] = OutboxDocFromDomain(&ef)
	}

	_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		// Check for duplicate key errors (idempotent insert)
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// ClaimPending claims pending records for processing.
func (r *OutboxRepository) ClaimPending(ctx context.Context, batchSize int, owner string, claimTTL time.Duration) ([]domain.OutboxEffect, error) {
	if err := validateClaimArguments(batchSize, owner, claimTTL); err != nil {
		return nil, err
	}
	collection := r.collection(collectionOutboxEffects)
	now := time.Now().UTC()
	filter, update, options := outboxClaimOperation(owner, now, claimTTL)
	effects := make([]domain.OutboxEffect, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		effect, claimed, err := r.claimNextEffect(ctx, collection, filter, update, options)
		if err != nil {
			return nil, err
		}
		if !claimed {
			break
		}
		if effect.TenantID == "" {
			if err := r.resolveLegacyTenant(ctx, effect); err != nil {
				return nil, fmt.Errorf("resolve tenant for legacy outbox effect %s: %w", effect.ID, err)
			}
		}
		effects = append(effects, *effect)
	}
	return effects, nil
}

func outboxClaimOperation(
	owner string,
	now time.Time,
	claimTTL time.Duration,
) (bson.M, bson.M, *options.FindOneAndUpdateOptions) {
	filter := bson.M{"$or": []bson.M{
		{"status": domain.OutboxStatusPending, "available_at": bson.M{"$lte": now}},
		{"status": domain.OutboxStatusClaimed, "claim_expires_at": bson.M{"$lte": now}},
	}}
	update := bson.M{"$set": bson.M{
		"status": domain.OutboxStatusClaimed, "claimed_by": owner,
		"claimed_at": now, "claim_expires_at": now.Add(claimTTL), "updated_at": now,
	}}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "available_at", Value: 1}, {Key: "created_at", Value: 1}}).
		SetReturnDocument(options.After)
	return filter, update, opts
}

func (r *OutboxRepository) claimNextEffect(
	ctx context.Context,
	collection *mongo.Collection,
	filter, update bson.M,
	opts *options.FindOneAndUpdateOptions,
) (*domain.OutboxEffect, bool, error) {
	var doc OutboxDoc
	err := collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim pending: %w", err)
	}
	effect := doc.ToDomain()
	return effect, true, nil
}

// resolveLegacyTenant backfills tenant identity for effects written before the
// outbox schema carried tenant_id. The execution record is the authoritative source.
func (r *OutboxRepository) resolveLegacyTenant(ctx context.Context, effect *domain.OutboxEffect) error {
	var execution struct {
		TenantID string `bson:"tenant_id"`
	}
	if err := r.collection(collectionExecutions).FindOne(ctx, bson.M{
		"execution_id": effect.ExecutionID,
	}).Decode(&execution); err != nil {
		return fmt.Errorf("load source execution: %w", err)
	}
	if execution.TenantID == "" {
		return fmt.Errorf("source execution has no tenant identity")
	}

	_, err := r.collection(collectionOutboxEffects).UpdateOne(ctx,
		bson.M{"effect_id": effect.ID, "tenant_id": bson.M{"$exists": false}},
		bson.M{"$set": bson.M{"tenant_id": execution.TenantID}},
	)
	if err != nil {
		return fmt.Errorf("persist tenant identity: %w", err)
	}
	effect.TenantID = execution.TenantID
	return nil
}

// MarkDelivered persists or updates the corresponding record.
func (r *OutboxRepository) MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error {
	if len(effectIDs) == 0 {
		return nil
	}
	coll := r.collection(collectionOutboxEffects)
	now := time.Now().UTC()

	result, err := coll.UpdateMany(ctx,
		bson.M{
			"effect_id":        bson.M{"$in": effectIDs},
			"status":           domain.OutboxStatusClaimed,
			"claimed_by":       claimant,
			"claim_expires_at": bson.M{"$gt": now},
		},
		bson.M{
			"$set": bson.M{
				"status":           domain.OutboxStatusDelivered,
				"claimed_by":       "",
				"claimed_at":       nil,
				"claim_expires_at": nil,
				"updated_at":       now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	if int(result.MatchedCount) != len(effectIDs) {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// ScheduleRetry persists or updates the corresponding record.
func (r *OutboxRepository) ScheduleRetry(ctx context.Context, effectID, claimant string, delay time.Duration, attempts int, errMsg string) error {
	coll := r.collection(collectionOutboxEffects)
	now := time.Now().UTC()
	availableAt := now.Add(delay)

	result, err := coll.UpdateOne(ctx,
		bson.M{
			"effect_id":        effectID,
			"status":           domain.OutboxStatusClaimed,
			"claimed_by":       claimant,
			"claim_expires_at": bson.M{"$gt": now},
		},
		bson.M{
			"$set": bson.M{
				"status":           domain.OutboxStatusPending,
				"attempts":         attempts,
				"available_at":     availableAt,
				"last_error":       errMsg,
				"claimed_by":       "",
				"claimed_at":       nil,
				"claim_expires_at": nil,
				"updated_at":       now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	if result.MatchedCount != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// Quarantine persists or updates the corresponding record.
func (r *OutboxRepository) Quarantine(ctx context.Context, effectID, claimant, errMsg string) error {
	coll := r.collection(collectionOutboxEffects)
	now := time.Now().UTC()

	result, err := coll.UpdateOne(ctx,
		bson.M{
			"effect_id":        effectID,
			"status":           domain.OutboxStatusClaimed,
			"claimed_by":       claimant,
			"claim_expires_at": bson.M{"$gt": now},
		},
		bson.M{
			"$set": bson.M{
				"status":           domain.OutboxStatusQuarantined,
				"last_error":       errMsg,
				"claimed_by":       "",
				"claimed_at":       nil,
				"claim_expires_at": nil,
				"updated_at":       now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("quarantine effect: %w", err)
	}
	if result.MatchedCount != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// OutboxDoc represents an outbox record stored in MongoDB.
type OutboxDoc struct {
	EffectID       string     `bson:"effect_id"`
	ExecutionID    string     `bson:"execution_id"`
	TenantID       string     `bson:"tenant_id"`
	Destination    string     `bson:"destination"`
	Name           string     `bson:"name"`
	Payload        []byte     `bson:"payload"`
	EffectType     string     `bson:"effect_type"`
	Status         string     `bson:"status"`
	Attempts       int        `bson:"attempts"`
	MaxAttempts    int        `bson:"max_attempts"`
	AvailableAt    time.Time  `bson:"available_at"`
	ClaimedBy      string     `bson:"claimed_by,omitempty"`
	ClaimedAt      *time.Time `bson:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time `bson:"claim_expires_at,omitempty"`
	LastError      string     `bson:"last_error,omitempty"`
	CreatedAt      time.Time  `bson:"created_at"`
	UpdatedAt      time.Time  `bson:"updated_at"`
}

// OutboxDocFromDomain converts a domain value to its MongoDB representation.
func OutboxDocFromDomain(ef *domain.OutboxEffect) *OutboxDoc {
	return &OutboxDoc{
		EffectID:       ef.ID,
		ExecutionID:    ef.ExecutionID,
		TenantID:       ef.TenantID,
		Destination:    ef.Destination,
		Name:           ef.Name,
		Payload:        append([]byte(nil), ef.Payload...),
		EffectType:     string(ef.EffectType),
		Status:         string(ef.Status),
		Attempts:       ef.Attempts,
		MaxAttempts:    ef.MaxAttempts,
		AvailableAt:    ef.AvailableAt,
		ClaimedBy:      ef.ClaimedBy,
		ClaimedAt:      ef.ClaimedAt,
		ClaimExpiresAt: ef.ClaimExpiresAt,
		LastError:      ef.LastError,
		CreatedAt:      ef.CreatedAt,
		UpdatedAt:      ef.UpdatedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *OutboxDoc) ToDomain() *domain.OutboxEffect {
	return &domain.OutboxEffect{
		ID:             d.EffectID,
		ExecutionID:    d.ExecutionID,
		TenantID:       d.TenantID,
		Destination:    d.Destination,
		Name:           d.Name,
		Payload:        json.RawMessage(append([]byte(nil), d.Payload...)),
		EffectType:     domain.EffectType(d.EffectType),
		Status:         domain.OutboxStatus(d.Status),
		Attempts:       d.Attempts,
		MaxAttempts:    d.MaxAttempts,
		AvailableAt:    d.AvailableAt,
		ClaimedBy:      d.ClaimedBy,
		ClaimedAt:      d.ClaimedAt,
		ClaimExpiresAt: d.ClaimExpiresAt,
		LastError:      d.LastError,
		CreatedAt:      d.CreatedAt,
		UpdatedAt:      d.UpdatedAt,
	}
}

// GetByExecution retrieves records matching the supplied criteria.
func (r *OutboxRepository) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	coll := r.collection(collectionOutboxEffects)

	filter := bson.M{"execution_id": executionID}

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get outbox by execution: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var effects []domain.OutboxEffect
	for cursor.Next(ctx) {
		var doc OutboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode outbox effect: %w", err)
		}
		effects = append(effects, *doc.ToDomain())
	}

	return effects, nil
}

// GetPending retrieves records matching the supplied criteria.
func (r *OutboxRepository) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	limit = normalizeOutboxLimit(limit)
	coll := r.collection(collectionOutboxEffects)
	now := time.Now().UTC()

	filter := bson.M{
		"$or": []bson.M{
			{
				"status":       domain.OutboxStatusPending,
				"available_at": bson.M{"$lte": now},
			},
			{
				"status":           domain.OutboxStatusClaimed,
				"claim_expires_at": bson.M{"$lte": now},
			},
		},
	}

	opts := options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "available_at", Value: 1}, {Key: "created_at", Value: 1}})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("get pending outbox: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var effects []domain.OutboxEffect
	for cursor.Next(ctx) {
		var doc OutboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode outbox effect: %w", err)
		}
		effects = append(effects, *doc.ToDomain())
	}

	return effects, nil
}

func validateClaimArguments(batchSize int, owner string, claimTTL time.Duration) error {
	if batchSize <= 0 || batchSize > 1000 {
		return fmt.Errorf("claim batch size must be between 1 and 1000")
	}
	if strings.TrimSpace(owner) == "" {
		return fmt.Errorf("claim owner is required")
	}
	if claimTTL <= 0 {
		return fmt.Errorf("claim TTL must be > 0")
	}
	return nil
}

func normalizeOutboxLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 500 {
		return 500
	}
	return limit
}
