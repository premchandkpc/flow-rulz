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
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// QuarantineRepository persists quarantined events and effects.
type QuarantineRepository struct {
	querier Querier
}

// NewQuarantineRepository creates and returns the configured value.
func NewQuarantineRepository(querier Querier) *QuarantineRepository {
	return &QuarantineRepository{querier: querier}
}

func (r *QuarantineRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// BackfillEffectTenantIDs repairs legacy effect quarantine records using their
// outbox effect and execution as the authoritative tenant source. Records whose
// source chain no longer exists are left unscoped and remain invisible to tenants.
func (r *QuarantineRepository) BackfillEffectTenantIDs(ctx context.Context) error {
	coll := r.collection(collectionQuarantine)
	cursor, err := coll.Find(ctx, bson.D{
		{Key: "source_type", Value: "effect"},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "tenant_id", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "tenant_id", Value: ""}},
		}},
	})
	if err != nil {
		return fmt.Errorf("find legacy effect quarantine entries: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var entry QuarantineDoc
		if err := cursor.Decode(&entry); err != nil {
			return fmt.Errorf("decode legacy effect quarantine entry: %w", err)
		}
		var effect struct {
			ExecutionID string `bson:"execution_id"`
		}
		if err := r.collection(collectionOutboxEffects).FindOne(ctx, bson.D{
			{Key: "effect_id", Value: entry.SourceID},
		}).Decode(&effect); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				continue
			}
			return fmt.Errorf("find outbox effect %s: %w", entry.SourceID, err)
		}
		var execution struct {
			TenantID string `bson:"tenant_id"`
		}
		if err := r.collection(collectionExecutions).FindOne(ctx, bson.D{
			{Key: "execution_id", Value: effect.ExecutionID},
		}).Decode(&execution); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				continue
			}
			return fmt.Errorf("find execution %s: %w", effect.ExecutionID, err)
		}
		if execution.TenantID == "" {
			continue
		}
		_, err := coll.UpdateOne(ctx,
			bson.D{
				{Key: "quarantine_id", Value: entry.QuarantineID},
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "tenant_id", Value: bson.D{{Key: "$exists", Value: false}}}},
					bson.D{{Key: "tenant_id", Value: ""}},
				}},
			},
			bson.D{{Key: "$set", Value: bson.D{{Key: "tenant_id", Value: execution.TenantID}}}},
		)
		if err != nil {
			return fmt.Errorf("backfill tenant for quarantine entry %s: %w", entry.QuarantineID, err)
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("iterate legacy effect quarantine entries: %w", err)
	}
	return nil
}

// Save persists the supplied value.
func (r *QuarantineRepository) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	coll := r.collection(collectionQuarantine)
	doc := QuarantineDocFromDomain(entry)
	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "quarantine_id", Value: entry.ID}},
		bson.D{{Key: "$setOnInsert", Value: doc}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save quarantine entry: %w", err)
	}
	return nil
}

// Get retrieves the requested value.
func (r *QuarantineRepository) Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error) {
	if tenantID == "" {
		return nil, nil
	}
	coll := r.collection(collectionQuarantine)
	var doc QuarantineDoc
	err := coll.FindOne(ctx, bson.D{
		{Key: "quarantine_id", Value: id},
		{Key: "tenant_id", Value: tenantID},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get quarantine entry: %w", err)
	}
	return doc.ToDomain(), nil
}

// Delete deletes the requested record.
func (r *QuarantineRepository) Delete(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	coll := r.collection(collectionQuarantine)
	result, err := coll.DeleteOne(ctx, bson.D{{Key: "quarantine_id", Value: id}, {Key: "tenant_id", Value: tenantID}})
	if err != nil {
		return fmt.Errorf("delete quarantine entry: %w", err)
	}
	if result.DeletedCount == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

// List returns matching records.
func (r *QuarantineRepository) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.TenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	filter.Limit, filter.Offset = normalizeQuarantinePagination(filter.Limit, filter.Offset)
	coll := r.collection(collectionQuarantine)

	// Normalize the optional filters to values selected from fixed allowlists.
	// Never pass raw HTTP query parameters into the MongoDB filter.
	sourceType, err := normalizeQuarantineSourceType(filter.SourceType)
	if err != nil {
		return nil, err
	}
	errorClass, err := normalizeQuarantineErrorClass(filter.ErrorClass)
	if err != nil {
		return nil, err
	}

	f := bson.D{{Key: "tenant_id", Value: mongoEquality(filter.TenantID)}}
	if sourceType != "" {
		f = append(f, bson.E{Key: "source_type", Value: mongoEquality(sourceType)})
	}
	if errorClass != "" {
		f = append(f, bson.E{Key: "error_class", Value: mongoEquality(errorClass)})
	}
	if !filter.From.IsZero() || !filter.To.IsZero() {
		dateRange := bson.D{}
		if !filter.From.IsZero() {
			dateRange = append(dateRange, bson.E{Key: "$gte", Value: filter.From})
		}
		if !filter.To.IsZero() {
			dateRange = append(dateRange, bson.E{Key: "$lte", Value: filter.To})
		}
		f = append(f, bson.E{Key: "created_at", Value: dateRange})
	}

	cursor, err := coll.Find(ctx, f, options.Find().
		SetLimit(int64(filter.Limit)).
		SetSkip(int64(filter.Offset)).
		SetSort(bson.D{{Key: "created_at", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list quarantine: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*domain.QuarantineEntry
	for cursor.Next(ctx) {
		var doc QuarantineDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode quarantine: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate quarantine: %w", err)
	}

	return entries, nil
}

// Replay replays the selected quarantined entry.
func (r *QuarantineRepository) Replay(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	coll := r.collection(collectionQuarantine)
	now := time.Now().UTC()

	result, err := coll.UpdateOne(ctx,
		bson.D{
			{Key: "quarantine_id", Value: id},
			{Key: "tenant_id", Value: tenantID},
		},
		bson.D{{Key: "$set", Value: bson.D{{Key: "updated_at", Value: now}}}},
	)
	if err != nil {
		return fmt.Errorf("record quarantine replay: %w", err)
	}
	if result.MatchedCount == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

// QuarantineDoc represents a quarantined entry stored in MongoDB.
type QuarantineDoc struct {
	QuarantineID string    `bson:"quarantine_id"`
	SourceType   string    `bson:"source_type"`
	SourceID     string    `bson:"source_id"`
	ErrorClass   string    `bson:"error_class"`
	PayloadRef   string    `bson:"payload_ref,omitempty"`
	EventID      string    `bson:"event_id,omitempty"`
	TenantID     string    `bson:"tenant_id,omitempty"`
	Revision     *int64    `bson:"revision,omitempty"`
	DecisionHash string    `bson:"decision_hash,omitempty"`
	ErrorMessage string    `bson:"error_message,omitempty"`
	CreatedAt    time.Time `bson:"created_at"`
	UpdatedAt    time.Time `bson:"updated_at"`
}

// QuarantineDocFromDomain converts a domain value to its MongoDB representation.
func QuarantineDocFromDomain(entry *domain.QuarantineEntry) *QuarantineDoc {
	return &QuarantineDoc{
		QuarantineID: entry.ID,
		SourceType:   entry.SourceType,
		SourceID:     entry.SourceID,
		ErrorClass:   entry.ErrorClass,
		PayloadRef:   entry.PayloadRef,
		EventID:      entry.EventID,
		TenantID:     entry.TenantID,
		ErrorMessage: entry.Error,
		CreatedAt:    entry.CreatedAt,
		UpdatedAt:    entry.CreatedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *QuarantineDoc) ToDomain() *domain.QuarantineEntry {
	return &domain.QuarantineEntry{
		ID:         d.QuarantineID,
		SourceType: d.SourceType,
		SourceID:   d.SourceID,
		ErrorClass: d.ErrorClass,
		PayloadRef: d.PayloadRef,
		EventID:    d.EventID,
		TenantID:   d.TenantID,
		Error:      d.ErrorMessage,
		CreatedAt:  d.CreatedAt,
	}
}

// normalizeQuarantineSourceType maps an optional filter to a known source type.
// Returning only literals keeps caller input out of the MongoDB query structure.
func normalizeQuarantineSourceType(value string) (string, error) {
	switch value {
	case "":
		return "", nil
	case "event":
		return "event", nil
	case "effect":
		return "effect", nil
	default:
		return "", fmt.Errorf("invalid source_type filter")
	}
}

// normalizeQuarantineErrorClass maps an optional filter to a known domain class.
func normalizeQuarantineErrorClass(value string) (string, error) {
	switch value {
	case "":
		return "", nil
	case string(domain.ErrorClassValidation):
		return string(domain.ErrorClassValidation), nil
	case string(domain.ErrorClassRule):
		return string(domain.ErrorClassRule), nil
	case string(domain.ErrorClassEvaluation):
		return string(domain.ErrorClassEvaluation), nil
	case string(domain.ErrorClassExecution):
		return string(domain.ErrorClassExecution), nil
	case string(domain.ErrorClassEffect):
		return string(domain.ErrorClassEffect), nil
	case string(domain.ErrorClassBroker):
		return string(domain.ErrorClassBroker), nil
	case string(domain.ErrorClassDatabase):
		return string(domain.ErrorClassDatabase), nil
	case string(domain.ErrorClassAuthorization):
		return string(domain.ErrorClassAuthorization), nil
	case string(domain.ErrorClassQuarantine):
		return string(domain.ErrorClassQuarantine), nil
	case string(domain.ErrorClassInternal):
		return string(domain.ErrorClassInternal), nil
	default:
		return "", fmt.Errorf("invalid error_class filter")
	}
}

// mongoEquality wraps a normalized scalar in a fixed equality expression so it
// is always interpreted as data, never as caller-controlled query syntax.
func mongoEquality(value any) bson.D {
	return bson.D{{Key: "$eq", Value: value}}
}

func normalizeQuarantinePagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
