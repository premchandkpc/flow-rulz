package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// InboxRepository persists inbox entries used for event deduplication.
type InboxRepository struct {
	querier Querier
}

// NewInboxRepository creates and returns the configured value.
func NewInboxRepository(q Querier) *InboxRepository {
	return &InboxRepository{querier: q}
}

func (r *InboxRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Insert persists the supplied value.
func (r *InboxRepository) Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error) {
	coll := r.collection(collectionInbox)
	doc := InboxDocFromDomain(entry)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert inbox: %w", err)
	}
	return true, nil
}

// Get retrieves the requested value.
func (r *InboxRepository) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	coll := r.collection(collectionInbox)
	var doc InboxDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return doc.ToDomain(), nil
}

// GetByRuleSet retrieves the requested value by tenant, event ID, and ruleset.
func (r *InboxRepository) GetByRuleSet(ctx context.Context, tenantID, eventID, ruleSet string) (*domain.InboxEntry, error) {
	coll := r.collection(collectionInbox)
	var doc InboxDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
		"rule_set":  ruleSet,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox by ruleset: %w", err)
	}
	return doc.ToDomain(), nil
}

// MarkCommitted persists the supplied value.
func (r *InboxRepository) MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error {
	coll := r.collection(collectionInbox)
	now := time.Now().UTC()
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"tenant_id": tenantID,
			"event_id":  eventID,
			"status":    domain.InboxStatusProcessing,
		},
		bson.M{
			"$set": bson.M{
				"status":       domain.InboxStatusCommitted,
				"execution_id": executionID,
				"committed_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark committed: %w", err)
	}
	return nil
}

// InboxDoc represents an inbox entry stored in MongoDB.
type InboxDoc struct {
	TenantID     string     `bson:"tenant_id"`
	EventID      string     `bson:"event_id"`
	Status       string     `bson:"status"`
	ExecutionID  string     `bson:"execution_id,omitempty"`
	FirstSeenAt  time.Time  `bson:"first_seen_at"`
	CommittedAt  *time.Time `bson:"committed_at,omitempty"`
	PartitionKey string     `bson:"partition_key,omitempty"`
	RuleSet      string     `bson:"rule_set,omitempty"`
	Payload      []byte     `bson:"payload,omitempty"`
	BatchID      string     `bson:"batch_id,omitempty"`
}

// InboxDocFromDomain converts a domain value to its MongoDB representation.
func InboxDocFromDomain(entry *domain.InboxEntry) *InboxDoc {
	return &InboxDoc{
		TenantID:     entry.TenantID,
		EventID:      entry.EventID,
		Status:       string(entry.Status),
		ExecutionID:  entry.ExecutionID,
		FirstSeenAt:  entry.FirstSeenAt,
		CommittedAt:  entry.CommittedAt,
		PartitionKey: entry.PartitionKey,
		RuleSet:      entry.RuleSet,
		Payload:      append([]byte(nil), entry.Payload...),
		BatchID:      entry.BatchID,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *InboxDoc) ToDomain() *domain.InboxEntry {
	var payload json.RawMessage
	if d.Payload != nil {
		payload = json.RawMessage(append([]byte(nil), d.Payload...))
	}
	return &domain.InboxEntry{
		TenantID:     d.TenantID,
		EventID:      d.EventID,
		Status:       domain.InboxStatus(d.Status),
		ExecutionID:  d.ExecutionID,
		FirstSeenAt:  d.FirstSeenAt,
		CommittedAt:  d.CommittedAt,
		PartitionKey: d.PartitionKey,
		RuleSet:      d.RuleSet,
		Payload:      payload,
		BatchID:      d.BatchID,
	}
}
