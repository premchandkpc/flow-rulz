package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

var workflowTypeNamePattern = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

func validateWorkflowTypeName(value string) error {
	if !workflowTypeNamePattern.MatchString(value) {
		return fmt.Errorf("invalid workflow type: expected 1-128 letters, digits, dots, underscores or hyphens")
	}
	return nil
}

// WorkflowRepository stores and retrieves workflow instances.
type WorkflowRepository struct {
	querier Querier
}

// NewWorkflowRepository creates and returns the configured repository.
func NewWorkflowRepository(q Querier) *WorkflowRepository {
	return &WorkflowRepository{querier: q}
}

func (r *WorkflowRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Save persists or updates the corresponding record.
func (r *WorkflowRepository) Save(ctx context.Context, workflow *domain.WorkflowInstance) error {
	if workflow == nil {
		return fmt.Errorf("workflow is required")
	}
	coll := r.collection(collectionWorkflowInstances)
	doc := WorkflowDocFromDomain(workflow)
	_, err := coll.InsertOne(ctx, doc)
	if err == nil {
		return nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("save workflow: %w", err)
	}

	// A duplicate is idempotent only when the existing workflow has the same
	// immutable identity. In particular, a workflow ID owned by another tenant
	// must not be reported as a successful create.
	existing, lookupErr := r.Get(ctx, workflow.TenantID, workflow.WorkflowID)
	if lookupErr != nil {
		return fmt.Errorf("verify duplicate workflow: %w", lookupErr)
	}
	if existing == nil || !sameWorkflowCreateIdentity(existing.TenantID, existing.WorkflowType, workflow) {
		return domain.ErrWorkflowConflict
	}
	return nil
}

func sameWorkflowCreateIdentity(existingTenantID, existingWorkflowType string, requested *domain.WorkflowInstance) bool {
	return requested != nil &&
		existingTenantID == requested.TenantID &&
		existingWorkflowType == requested.WorkflowType
}

// Get retrieves records matching the supplied criteria.
func (r *WorkflowRepository) Get(ctx context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error) {
	coll := r.collection(collectionWorkflowInstances)
	var doc WorkflowDoc
	err := coll.FindOne(ctx, bson.D{
		{Key: "workflow_id", Value: workflowID},
		{Key: "tenant_id", Value: tenantID},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	return doc.ToDomain(), nil
}

// Update persists or updates the corresponding record.
func (r *WorkflowRepository) Update(ctx context.Context, workflow *domain.WorkflowInstance) error {
	coll := r.collection(collectionWorkflowInstances)
	updatedAt := workflow.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	filter := bson.D{
		{Key: "workflow_id", Value: workflow.WorkflowID},
		{Key: "tenant_id", Value: workflow.TenantID},
		{Key: "version", Value: workflow.Version - 1},
	}

	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "state", Value: workflow.State},
			{Key: "current_revision", Value: workflow.CurrentRevision},
			{Key: "context", Value: workflow.Context},
			{Key: "updated_at", Value: updatedAt},
			{Key: "completed_at", Value: workflow.CompletedAt},
		}},
		{Key: "$inc", Value: bson.D{{Key: "version", Value: 1}}},
	}

	result, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	if result.MatchedCount == 0 {
		return domain.ErrWorkflowConflict
	}

	workflow.UpdatedAt = updatedAt
	return nil
}

// normalizeWorkflowListLimit matches the PostgreSQL adapter's default list limit.
func normalizeWorkflowListLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	return limit
}

// GetByTenantAndState retrieves records matching the supplied criteria.
func (r *WorkflowRepository) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	limit = normalizeWorkflowListLimit(limit)
	coll := r.collection(collectionWorkflowInstances)

	filter := bson.D{{Key: "tenant_id", Value: tenantID}}
	if state != "" {
		filter = append(filter, bson.E{Key: "state", Value: state})
	}

	cursor, err := coll.Find(ctx, filter, options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "updated_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("get workflows by tenant and state: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var workflows []*domain.WorkflowInstance
	for cursor.Next(ctx) {
		var doc WorkflowDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode workflow: %w", err)
		}
		workflows = append(workflows, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflows: %w", err)
	}

	return workflows, nil
}

// GetByCorrelation retrieves records matching the supplied criteria.
func (r *WorkflowRepository) GetByCorrelation(ctx context.Context, tenantID, correlationID string) ([]*domain.WorkflowInstance, error) {
	coll := r.collection(collectionWorkflowInstances)

	filter := bson.D{
		{Key: "tenant_id", Value: tenantID},
		{Key: "correlation_id", Value: correlationID},
	}

	cursor, err := coll.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("get workflows by correlation: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var workflows []*domain.WorkflowInstance
	for cursor.Next(ctx) {
		var doc WorkflowDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode workflow: %w", err)
		}
		workflows = append(workflows, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflows: %w", err)
	}

	return workflows, nil
}

// WorkflowDoc represents a workflow instance stored in MongoDB.
type WorkflowDoc struct {
	WorkflowID      string     `bson:"workflow_id"`
	TenantID        string     `bson:"tenant_id"`
	CorrelationID   string     `bson:"correlation_id,omitempty"`
	WorkflowType    string     `bson:"workflow_type"`
	State           string     `bson:"state"`
	Version         int64      `bson:"version"`
	CurrentRevision int64      `bson:"current_revision,omitempty"`
	Context         []byte     `bson:"context,omitempty"`
	CreatedAt       time.Time  `bson:"created_at"`
	UpdatedAt       time.Time  `bson:"updated_at"`
	CompletedAt     *time.Time `bson:"completed_at,omitempty"`
}

// WorkflowDocFromDomain converts a domain value to its MongoDB representation.
func WorkflowDocFromDomain(w *domain.WorkflowInstance) *WorkflowDoc {
	var contextFields struct {
		CorrelationID string `json:"correlation_id"`
	}
	if len(w.Context) > 0 {
		_ = json.Unmarshal(w.Context, &contextFields)
	}
	return &WorkflowDoc{
		WorkflowID:      w.WorkflowID,
		TenantID:        w.TenantID,
		CorrelationID:   contextFields.CorrelationID,
		WorkflowType:    w.WorkflowType,
		State:           w.State,
		Version:         w.Version,
		CurrentRevision: w.CurrentRevision,
		Context:         append([]byte(nil), w.Context...),
		CreatedAt:       w.CreatedAt,
		UpdatedAt:       w.UpdatedAt,
		CompletedAt:     w.CompletedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *WorkflowDoc) ToDomain() *domain.WorkflowInstance {
	var context json.RawMessage
	if d.Context != nil {
		context = json.RawMessage(append([]byte(nil), d.Context...))
	}
	return &domain.WorkflowInstance{
		WorkflowID:      d.WorkflowID,
		TenantID:        d.TenantID,
		WorkflowType:    d.WorkflowType,
		State:           d.State,
		Version:         d.Version,
		CurrentRevision: d.CurrentRevision,
		Context:         context,
		CreatedAt:       d.CreatedAt,
		UpdatedAt:       d.UpdatedAt,
		CompletedAt:     d.CompletedAt,
	}
}

// WorkflowDefinitionRepository stores and retrieves workflow definitions.
type WorkflowDefinitionRepository struct {
	querier Querier
}

// NewWorkflowDefinitionRepository creates and returns the configured repository.
func NewWorkflowDefinitionRepository(q Querier) *WorkflowDefinitionRepository {
	return &WorkflowDefinitionRepository{querier: q}
}

func (r *WorkflowDefinitionRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Save persists or updates the corresponding record.
func (r *WorkflowDefinitionRepository) Save(ctx context.Context, def *domain.WorkflowDefinition) error {
	if def == nil {
		return fmt.Errorf("workflow definition is required")
	}
	if err := validateWorkflowTypeName(def.WorkflowType); err != nil {
		return err
	}
	coll := r.collection(collectionWorkflowDefinitions)
	doc := WorkflowDefinitionDocFromDomain(def)
	// WorkflowType is validated above and both values are BSON scalars under fixed keys.
	// No query syntax is constructed by concatenating user-controlled strings.
	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "workflow_type", Value: def.WorkflowType}, {Key: "version", Value: def.Version}},
		bson.D{{Key: "$set", Value: doc}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}
	return nil
}

// Get retrieves records matching the supplied criteria.
func (r *WorkflowDefinitionRepository) Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	if err := validateWorkflowTypeName(workflowType); err != nil {
		return nil, err
	}
	coll := r.collection(collectionWorkflowDefinitions)
	var doc WorkflowDefinitionDoc
	err := coll.FindOne(ctx, bson.D{
		{Key: "workflow_type", Value: workflowType},
		{Key: "version", Value: version},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	return doc.ToDomain(), nil
}

// List returns records matching the supplied criteria.
func (r *WorkflowDefinitionRepository) List(ctx context.Context) ([]*domain.WorkflowDefinition, error) {
	coll := r.collection(collectionWorkflowDefinitions)
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "workflow_type", Value: 1}, {Key: "version", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list workflow definitions: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var defs []*domain.WorkflowDefinition
	for cursor.Next(ctx) {
		var doc WorkflowDefinitionDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode workflow definition: %w", err)
		}
		defs = append(defs, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow definitions: %w", err)
	}
	return defs, nil
}

// Delete deletes the requested record.
func (r *WorkflowDefinitionRepository) Delete(ctx context.Context, workflowType string, version int64) error {
	if err := validateWorkflowTypeName(workflowType); err != nil {
		return err
	}
	coll := r.collection(collectionWorkflowDefinitions)
	_, err := coll.DeleteOne(ctx, bson.D{{Key: "workflow_type", Value: workflowType}, {Key: "version", Value: version}})
	if err != nil {
		return fmt.Errorf("delete workflow definition: %w", err)
	}
	return nil
}

// WorkflowDefinitionDoc provides the corresponding repository operation.
type WorkflowDefinitionDoc struct {
	WorkflowType string                      `bson:"workflow_type"`
	Version      int64                       `bson:"version"`
	States       []domain.WorkflowState      `bson:"states"`
	Transitions  []domain.WorkflowTransition `bson:"transitions"`
	Rules        map[string]string           `bson:"rules,omitempty"`
	CreatedAt    time.Time                   `bson:"created_at"`
	UpdatedAt    time.Time                   `bson:"updated_at"`
}

// WorkflowDefinitionDocFromDomain converts a domain value to its MongoDB representation.
func WorkflowDefinitionDocFromDomain(d *domain.WorkflowDefinition) *WorkflowDefinitionDoc {
	return &WorkflowDefinitionDoc{
		WorkflowType: d.WorkflowType,
		Version:      d.Version,
		States:       d.States,
		Transitions:  d.Transitions,
		Rules:        d.Rules,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *WorkflowDefinitionDoc) ToDomain() *domain.WorkflowDefinition {
	return &domain.WorkflowDefinition{
		WorkflowType: d.WorkflowType,
		Version:      d.Version,
		States:       d.States,
		Transitions:  d.Transitions,
		Rules:        d.Rules,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}
