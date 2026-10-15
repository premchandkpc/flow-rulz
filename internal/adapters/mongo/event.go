package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// EventRepository persists and retrieves event envelopes in MongoDB.
type EventRepository struct {
	querier Querier
}

// NewEventRepository creates an event repository using the supplied querier.
func NewEventRepository(q Querier) *EventRepository {
	return &EventRepository{querier: q}
}

func (r *EventRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Save stores an event envelope, treating duplicate events as already saved.
func (r *EventRepository) Save(ctx context.Context, event *domain.EventEnvelope) error {
	coll := r.collection(collectionEvents)
	doc := EventDocFromDomain(event)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save event: %w", err)
	}
	return nil
}

// Get retrieves an event by tenant and event ID.
func (r *EventRepository) Get(ctx context.Context, tenantID, eventID string) (*domain.EventEnvelope, error) {
	coll := r.collection(collectionEvents)
	var doc EventDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get event: %w", err)
	}
	return doc.ToDomain(), nil
}

// Exists reports whether an event exists for the tenant and event ID.
func (r *EventRepository) Exists(ctx context.Context, tenantID, eventID string) (bool, error) {
	coll := r.collection(collectionEvents)
	count, err := coll.CountDocuments(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}, options.Count().SetLimit(1))
	if err != nil {
		return false, fmt.Errorf("check event exists: %w", err)
	}
	return count > 0, nil
}

// MarkProcessed records the execution that processed an event.
func (r *EventRepository) MarkProcessed(ctx context.Context, tenantID, eventID, executionID string) error {
	coll := r.collection(collectionEvents)
	now := time.Now().UTC()
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"tenant_id": tenantID,
			"event_id":  eventID,
		},
		bson.M{
			"$set": bson.M{
				"processed":    true,
				"execution_id": executionID,
				"processed_at": now,
			},
		},
	)
	return err
}

// GetUnprocessed lists unprocessed events for a tenant up to the given limit.
func (r *EventRepository) GetUnprocessed(ctx context.Context, tenantID string, limit int) ([]*domain.EventEnvelope, error) {
	coll := r.collection(collectionEvents)

	filter := bson.M{
		"tenant_id": tenantID,
		"processed": bson.M{"$ne": true},
	}

	cursor, err := coll.Find(ctx, filter, options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "occurred_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("get unprocessed events: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var events []*domain.EventEnvelope
	for cursor.Next(ctx) {
		var doc EventDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}

	return events, nil
}

// EventDoc is the MongoDB representation of an event envelope.
type EventDoc struct {
	EventID      string            `bson:"event_id"`
	TenantID     string            `bson:"tenant_id"`
	Type         string            `bson:"type"`
	PartitionKey string            `bson:"partition_key"`
	WorkflowID   string            `bson:"workflow_id,omitempty"`
	OccurredAt   time.Time         `bson:"occurred_at"`
	ScheduledAt  *time.Time        `bson:"scheduled_at,omitempty"`
	Data         []byte            `bson:"data"`
	Headers      map[string]string `bson:"headers,omitempty"`
	Meta         map[string]any    `bson:"meta,omitempty"`
	Contract     *domain.ContractRef `bson:"contract,omitempty"`
	MessageKind  string            `bson:"message_kind,omitempty"`
	CorrelationID string           `bson:"correlation_id,omitempty"`
	CausationID  string            `bson:"causation_id,omitempty"`
	Processed    bool              `bson:"processed"`
	ExecutionID  string            `bson:"execution_id,omitempty"`
	ProcessedAt  *time.Time        `bson:"processed_at,omitempty"`
	CreatedAt    time.Time         `bson:"created_at"`
}

// EventDocFromDomain converts a domain event envelope to a MongoDB document.
func EventDocFromDomain(event *domain.EventEnvelope) *EventDoc {
	return &EventDoc{
		EventID:      event.ID,
		TenantID:     event.TenantID,
		Type:         event.Type,
		PartitionKey: event.PartitionKey,
		WorkflowID:   event.WorkflowID,
		OccurredAt:   event.OccurredAt,
		Data:         append([]byte(nil), event.Data...),
		Headers:      event.Headers,
		Meta:         rawMetadataToBSON(event.Meta),
		Contract:     event.Contract,
		MessageKind:  event.MessageKind,
		CorrelationID: event.CorrelationID,
		CausationID:  event.CausationID,
		Processed:    false,
		CreatedAt:    time.Now().UTC(),
	}
}

// ToDomain converts the MongoDB event document to a domain event envelope.
func (d *EventDoc) ToDomain() *domain.EventEnvelope {
	var data json.RawMessage
	if d.Data != nil {
		data = json.RawMessage(append([]byte(nil), d.Data...))
	}

	return &domain.EventEnvelope{
		ID:           d.EventID,
		Type:         d.Type,
		TenantID:     d.TenantID,
		PartitionKey: d.PartitionKey,
		WorkflowID:   d.WorkflowID,
		OccurredAt:   d.OccurredAt,
		Data:         data,
		Headers:      d.Headers,
		Meta:         bsonMetadataToRaw(d.Meta),
		Contract:     d.Contract,
		MessageKind:  d.MessageKind,
		CorrelationID: d.CorrelationID,
		CausationID:  d.CausationID,
	}
}

func rawMetadataToBSON(meta map[string]json.RawMessage) map[string]any {
	if len(meta) == 0 { return nil }
	result := make(map[string]any, len(meta))
	for key, raw := range meta {
		var value any
		if json.Unmarshal(raw, &value) == nil { result[key] = value }
	}
	return result
}

func bsonMetadataToRaw(meta map[string]any) map[string]json.RawMessage {
	if len(meta) == 0 { return nil }
	result := make(map[string]json.RawMessage, len(meta))
	for key, value := range meta {
		if raw, err := json.Marshal(normalizeBSONMetadata(value)); err == nil { result[key] = raw }
	}
	return result
}

func normalizeBSONMetadata(value any) any {
	switch typed := value.(type) {
	case bson.D:
		result := make(map[string]any, len(typed))
		for _, element := range typed { result[element.Key] = normalizeBSONMetadata(element.Value) }
		return result
	case bson.M:
		result := make(map[string]any, len(typed))
		for key, item := range typed { result[key] = normalizeBSONMetadata(item) }
		return result
	case bson.A:
		result := make([]any, len(typed))
		for i, item := range typed { result[i] = normalizeBSONMetadata(item) }
		return result
	case []any:
		result := make([]any, len(typed))
		for i, item := range typed { result[i] = normalizeBSONMetadata(item) }
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed { result[key] = normalizeBSONMetadata(item) }
		return result
	default:
		return value
	}
}
