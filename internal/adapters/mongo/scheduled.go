package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ScheduledEventRepository stores and manages scheduled events.
type ScheduledEventRepository struct {
	querier Querier
}

// NewScheduledEventRepository creates and returns the configured repository.
func NewScheduledEventRepository(q Querier) *ScheduledEventRepository {
	return &ScheduledEventRepository{querier: q}
}

func (r *ScheduledEventRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Save persists or updates the corresponding record.
func (r *ScheduledEventRepository) Save(ctx context.Context, event *domain.ScheduledEvent) error {
	coll := r.collection(collectionScheduledEvents)
	doc := ScheduledDocFromDomain(event)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save scheduled event: %w", err)
	}
	return nil
}

// GetDue retrieves records matching the supplied criteria.
func (r *ScheduledEventRepository) GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	coll := r.collection(collectionScheduledEvents)
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "status", Value: domain.ScheduledEventStatusPending}, {Key: "$or", Value: bson.A{
			bson.D{{Key: "retry_at", Value: bson.D{{Key: "$lte", Value: before}}}},
			bson.D{{Key: "retry_at", Value: bson.D{{Key: "$exists", Value: false}}}, {Key: "scheduled_at", Value: bson.D{{Key: "$lte", Value: before}}}},
		}}},
		bson.D{{Key: "status", Value: domain.ScheduledEventStatusClaimed}, {Key: "claim_expires_at", Value: bson.D{{Key: "$lte", Value: before}}}},
	}}}
	cursor, err := coll.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "scheduled_at", Value: 1}}).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("get due scheduled events: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var events []*domain.ScheduledEvent
	for cursor.Next(ctx) {
		var doc ScheduledDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode scheduled event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled events: %w", err)
	}
	return events, nil
}

// Claim persists or updates the corresponding record.
func (r *ScheduledEventRepository) Claim(
	ctx context.Context,
	eventIDs []string,
	claimant string,
	leaseTTL time.Duration,
) ([]*domain.ScheduledEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if err := validateScheduledClaim(eventIDs, claimant, leaseTTL); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expiresAt := now.Add(leaseTTL)
	collection := r.collection(collectionScheduledEvents)
	events := make([]*domain.ScheduledEvent, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		event, claimed, err := claimScheduledEvent(ctx, collection, eventID, claimant, now, expiresAt)
		if err != nil {
			return nil, err
		}
		if claimed {
			events = append(events, event)
		}
	}
	return events, nil
}

func validateScheduledClaim(eventIDs []string, claimant string, leaseTTL time.Duration) error {
	if len(eventIDs) == 0 {
		return nil
	}
	if claimant == "" {
		return fmt.Errorf("claim scheduled events: claimant is required")
	}
	if leaseTTL <= 0 {
		return fmt.Errorf("claim scheduled events: lease TTL must be positive")
	}
	return nil
}

func claimScheduledEvent(
	ctx context.Context,
	collection *mongo.Collection,
	eventID, claimant string,
	now, expiresAt time.Time,
) (*domain.ScheduledEvent, bool, error) {
	var doc ScheduledDoc
	err := collection.FindOneAndUpdate(
		ctx,
		scheduledClaimFilter(eventID, now),
		scheduledClaimUpdate(claimant, now, expiresAt),
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim scheduled event: %w", err)
	}
	return doc.ToDomain(), true, nil
}

func scheduledClaimFilter(eventID string, now time.Time) bson.D {
	return bson.D{
		{Key: "event_id", Value: eventID},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "status", Value: domain.ScheduledEventStatusPending}, {Key: "$or", Value: bson.A{
				bson.D{{Key: "retry_at", Value: bson.D{{Key: "$lte", Value: now}}}},
				bson.D{{Key: "retry_at", Value: bson.D{{Key: "$exists", Value: false}}}, {Key: "scheduled_at", Value: bson.D{{Key: "$lte", Value: now}}}},
			}}},
			bson.D{{Key: "status", Value: domain.ScheduledEventStatusClaimed}, {Key: "claim_expires_at", Value: bson.D{{Key: "$lte", Value: now}}}},
		}},
	}
}

func scheduledClaimUpdate(claimant string, now, expiresAt time.Time) bson.D {
	return bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: domain.ScheduledEventStatusClaimed},
		{Key: "claimant", Value: claimant},
		{Key: "claimed_at", Value: now},
		{Key: "claim_expires_at", Value: expiresAt},
	}}}
}

// MarkReleased persists or updates the corresponding record.
func (r *ScheduledEventRepository) MarkReleased(ctx context.Context, eventID, claimant string, releasedAt time.Time) error {
	coll := r.collection(collectionScheduledEvents)
	result, err := coll.UpdateOne(ctx,
		bson.M{"event_id": eventID, "status": domain.ScheduledEventStatusClaimed, "claimant": claimant, "claim_expires_at": bson.M{"$gt": releasedAt}},
		bson.M{
			"$set": bson.M{"status": domain.ScheduledEventStatusReleased, "released_at": releasedAt},
			"$unset": bson.M{
				"claimant": "", "claimed_at": "", "claim_expires_at": "", "retry_at": "",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark scheduled event released: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("mark scheduled event released: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// ScheduleRetry persists or updates the corresponding record.
func (r *ScheduledEventRepository) ScheduleRetry(ctx context.Context, eventID, claimant string, retryAt time.Time, errMsg string) error {
	coll := r.collection(collectionScheduledEvents)
	now := time.Now().UTC()
	result, err := coll.UpdateOne(ctx,
		bson.D{
			{Key: "event_id", Value: eventID},
			{Key: "status", Value: domain.ScheduledEventStatusClaimed},
			{Key: "claimant", Value: claimant},
			{Key: "claim_expires_at", Value: bson.D{{Key: "$gt", Value: now}}},
		},
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "status", Value: domain.ScheduledEventStatusPending},
				{Key: "retry_at", Value: retryAt},
				{Key: "last_error", Value: errMsg},
			}},
			{Key: "$inc", Value: bson.D{{Key: "retry_count", Value: 1}}},
			{Key: "$unset", Value: bson.D{
				{Key: "claimant", Value: ""},
				{Key: "claimed_at", Value: ""},
				{Key: "claim_expires_at", Value: ""},
			}},
		},
	)
	if err != nil {
		return fmt.Errorf("schedule scheduled event retry: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("schedule scheduled event retry: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// MarkFailed persists or updates the corresponding record.
func (r *ScheduledEventRepository) MarkFailed(ctx context.Context, eventID, claimant, errMsg string) error {
	coll := r.collection(collectionScheduledEvents)
	now := time.Now().UTC()
	result, err := coll.UpdateOne(ctx,
		bson.M{"event_id": eventID, "status": domain.ScheduledEventStatusClaimed, "claimant": claimant, "claim_expires_at": bson.M{"$gt": now}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "status", Value: domain.ScheduledEventStatusFailed}, {Key: "last_error", Value: errMsg}}},
			{Key: "$inc", Value: bson.D{{Key: "retry_count", Value: 1}}},
			{Key: "$unset", Value: bson.D{{Key: "claimant", Value: ""}, {Key: "claimed_at", Value: ""}, {Key: "claim_expires_at", Value: ""}}},
		},
	)
	if err != nil {
		return fmt.Errorf("mark scheduled event failed: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("mark scheduled event failed: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// Get retrieves records matching the supplied criteria.
func (r *ScheduledEventRepository) Get(ctx context.Context, tenantID, eventID string) (*domain.ScheduledEvent, error) {
	coll := r.collection(collectionScheduledEvents)
	var doc ScheduledDoc
	err := coll.FindOne(ctx, bson.M{"tenant_id": tenantID, "event_id": eventID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get scheduled event: %w", err)
	}
	return doc.ToDomain(), nil
}

// List returns records matching the supplied criteria.
func (r *ScheduledEventRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	coll := r.collection(collectionScheduledEvents)
	cursor, err := coll.Find(ctx,
		bson.M{"tenant_id": tenantID},
		options.Find().
			SetSort(bson.D{{Key: "scheduled_at", Value: -1}, {Key: "created_at", Value: -1}}).
			SetSkip(int64(offset)).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("list scheduled events: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var events []*domain.ScheduledEvent
	for cursor.Next(ctx) {
		var doc ScheduledDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode scheduled event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled events: %w", err)
	}
	return events, nil
}

// Cancel persists or updates the corresponding record.
func (r *ScheduledEventRepository) Cancel(ctx context.Context, tenantID, eventID string) error {
	coll := r.collection(collectionScheduledEvents)
	result, err := coll.UpdateOne(ctx,
		bson.M{"tenant_id": tenantID, "event_id": eventID, "status": domain.ScheduledEventStatusPending},
		bson.M{"$set": bson.M{"status": domain.ScheduledEventStatusCancelled}},
	)
	if err != nil {
		return fmt.Errorf("cancel scheduled event: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("cancel scheduled event: event %q not found or not pending for tenant %q", eventID, tenantID)
	}
	return nil
}

// ScheduledDoc represents a scheduled event stored in MongoDB.
type ScheduledDoc struct {
	EventID        string            `bson:"event_id"`
	TenantID       string            `bson:"tenant_id"`
	EventType      string            `bson:"event_type"`
	PartitionKey   string            `bson:"partition_key"`
	WorkflowID     string            `bson:"workflow_id,omitempty"`
	Payload        []byte            `bson:"payload"`
	Headers        map[string]string `bson:"headers,omitempty"`
	ScheduledAt    time.Time         `bson:"scheduled_at"`
	Status         string            `bson:"status"`
	CreatedAt      time.Time         `bson:"created_at"`
	ReleasedAt     *time.Time        `bson:"released_at,omitempty"`
	Claimant       string            `bson:"claimant,omitempty"`
	ClaimedAt      *time.Time        `bson:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time        `bson:"claim_expires_at,omitempty"`
	LastError      string            `bson:"last_error,omitempty"`
	RetryAt        *time.Time        `bson:"retry_at,omitempty"`
	RetryCount     int               `bson:"retry_count,omitempty"`
}

// ScheduledDocFromDomain converts a domain value to its MongoDB representation.
func ScheduledDocFromDomain(event *domain.ScheduledEvent) *ScheduledDoc {
	return &ScheduledDoc{
		EventID:      event.EventID,
		TenantID:     event.TenantID,
		EventType:    event.EventType,
		PartitionKey: event.PartitionKey,
		WorkflowID:   event.WorkflowID,
		Payload:      append([]byte(nil), event.Payload...),
		Headers:      event.Headers,
		ScheduledAt:  event.ScheduledAt,
		Status:       string(event.Status),
		CreatedAt:    event.CreatedAt,
		ReleasedAt:   event.ReleasedAt,
		RetryAt:      event.RetryAt,
		RetryCount:   event.RetryCount,
		LastError:    event.LastError,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *ScheduledDoc) ToDomain() *domain.ScheduledEvent {
	var payload json.RawMessage
	if d.Payload != nil {
		payload = json.RawMessage(append([]byte(nil), d.Payload...))
	}
	return &domain.ScheduledEvent{
		EventID:      d.EventID,
		TenantID:     d.TenantID,
		EventType:    d.EventType,
		PartitionKey: d.PartitionKey,
		WorkflowID:   d.WorkflowID,
		Payload:      payload,
		Headers:      d.Headers,
		ScheduledAt:  d.ScheduledAt,
		Status:       domain.ScheduledEventStatus(d.Status),
		CreatedAt:    d.CreatedAt,
		ReleasedAt:   d.ReleasedAt,
		RetryAt:      d.RetryAt,
		RetryCount:   d.RetryCount,
		LastError:    d.LastError,
	}
}
