package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// ExecutionRepository persists execution records.
type ExecutionRepository struct {
	querier Querier
}

// NewExecutionRepository creates and returns the configured value.
func NewExecutionRepository(q Querier) *ExecutionRepository {
	return &ExecutionRepository{querier: q}
}

func (r *ExecutionRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Save persists the supplied value.
func (r *ExecutionRepository) Save(ctx context.Context, execution *domain.Execution) error {
	coll := r.collection(collectionExecutions)
	doc := ExecutionDocFromDomain(execution)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save execution: %w", err)
	}
	return nil
}

// Get retrieves the requested value.
func (r *ExecutionRepository) Get(ctx context.Context, executionID string) (*domain.Execution, error) {
	coll := r.collection(collectionExecutions)
	var doc ExecutionDoc
	err := coll.FindOne(ctx, bson.M{
		"execution_id": executionID,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get execution: %w", err)
	}
	return doc.ToDomain(), nil
}

// UpdateStatus persists the supplied value.
func (r *ExecutionRepository) UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	coll := r.collection(collectionExecutions)
	now := time.Now().UTC()

	update := bson.M{
		"$set": bson.M{
			"status": status,
			"error":  errMsg,
		},
	}

	if status == domain.ExecutionStatusCompleted ||
		status == domain.ExecutionStatusFailed ||
		status == domain.ExecutionStatusQuarantined {
		update["$set"].(bson.M)["completed_at"] = now
	}

	result, err := coll.UpdateOne(ctx,
		bson.M{
			"execution_id": executionID,
			"status":       domain.ExecutionStatusPending,
		},
		bson.M{"$set": update["$set"]},
	)
	if err != nil {
		return fmt.Errorf("update execution status: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("%w: execution not found or not in pending state", domain.ErrInvalidExecutionStatus)
	}
	return nil
}

// ExecutionDoc represents an execution record stored in MongoDB.
type ExecutionDoc struct {
	ExecutionID  string     `bson:"execution_id"`
	EventID      string     `bson:"event_id"`
	TenantID     string     `bson:"tenant_id"`
	RuleSet      string     `bson:"rule_set"`
	Revision     int64      `bson:"revision"`
	DecisionHash string     `bson:"decision_hash"`
	Status       string     `bson:"status"`
	Error        string     `bson:"error,omitempty"`
	TraceID      string     `bson:"trace_id,omitempty"`
	CreatedAt    time.Time  `bson:"created_at"`
	CompletedAt  *time.Time `bson:"completed_at,omitempty"`
}

// ExecutionDocFromDomain converts a domain value to its MongoDB representation.
func ExecutionDocFromDomain(exec *domain.Execution) *ExecutionDoc {
	return &ExecutionDoc{
		ExecutionID:  exec.ID,
		EventID:      exec.EventID,
		TenantID:     exec.TenantID,
		RuleSet:      exec.RuleSet,
		Revision:     exec.Revision,
		DecisionHash: exec.DecisionHash,
		Status:       string(exec.Status),
		Error:        exec.Error,
		TraceID:      exec.TraceID,
		CreatedAt:    exec.CreatedAt,
		CompletedAt:  exec.CompletedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *ExecutionDoc) ToDomain() *domain.Execution {
	return &domain.Execution{
		ID:           d.ExecutionID,
		EventID:      d.EventID,
		TenantID:     d.TenantID,
		RuleSet:      d.RuleSet,
		Revision:     d.Revision,
		DecisionHash: d.DecisionHash,
		Status:       domain.ExecutionStatus(d.Status),
		Error:        d.Error,
		TraceID:      d.TraceID,
		CreatedAt:    d.CreatedAt,
		CompletedAt:  d.CompletedAt,
	}
}
