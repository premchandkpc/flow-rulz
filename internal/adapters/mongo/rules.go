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

// RuleRepository stores and retrieves versioned rule revisions.
type RuleRepository struct {
	querier Querier
}

// Repository is retained as a compatibility alias for legacy internal callers.
type Repository = RuleRepository

// NewRuleRepository creates and returns the configured value.
func NewRuleRepository(q Querier) *RuleRepository {
	return &RuleRepository{querier: q}
}

// NewRepository is retained for compatibility; new code should use NewRuleRepository.
func NewRepository(q Querier) *RuleRepository {
	return NewRuleRepository(q)
}

func (r *RuleRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// GetActive retrieves the requested value.
func (r *Repository) GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	coll := r.collection(collectionRuleRevisions)

	var activation ActivationDoc
	err := r.collection(collectionRuleActivations).FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	}).Decode(&activation)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}

	var revision RevisionDoc
	err = coll.FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      activation.RuleSet,
		"revision":     activation.Revision,
	}).Decode(&revision)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get revision: %w", err)
	}

	return revision.ToDomain(), nil
}

// Get retrieves the requested value.
func (r *Repository) Get(ctx context.Context, tenantScope string, ruleSet string, revisionNum int64) (*domain.RuleRevision, error) {
	coll := r.collection(collectionRuleRevisions)

	var revision RevisionDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      ruleSet,
		"revision":     revisionNum,
	}).Decode(&revision)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get revision: %w", err)
	}

	return revision.ToDomain(), nil
}

// Save persists the supplied value.
func (r *Repository) Save(ctx context.Context, revision *domain.RuleRevision) error {
	coll := r.collection(collectionRuleRevisions)

	doc := RevisionDocFromDomain(revision.TenantScope, revision)
	_, err := coll.InsertOne(ctx, doc)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

// List returns matching records.
func (r *Repository) List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	coll := r.collection(collectionRuleRevisions)
	cursor, err := coll.Find(ctx, bson.M{"tenant_scope": tenantScope})
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var revisions []*domain.RuleRevision
	for cursor.Next(ctx) {
		var doc RevisionDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode revision: %w", err)
		}
		revisions = append(revisions, doc.ToDomain())
	}
	return revisions, nil
}

// Delete deletes the requested record.
func (r *Repository) Delete(ctx context.Context, tenantScope string, ruleSet string, revision int64) error {
	coll := r.collection(collectionRuleRevisions)
	_, err := coll.DeleteOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      ruleSet,
		"revision":     revision,
	})
	if err != nil {
		return fmt.Errorf("delete revision: %w", err)
	}
	return nil
}

// ActivationDoc provides the corresponding repository operation.
type ActivationDoc struct {
	TenantScope string    `bson:"tenant_scope"`
	RuleSet     string    `bson:"rule_set"`
	Revision    int64     `bson:"revision"`
	Version     int64     `bson:"version"`
	Actor       string    `bson:"actor"`
	ActivatedAt time.Time `bson:"activated_at"`
}

// RevisionDoc provides the corresponding repository operation.
type RevisionDoc struct {
	TenantScope     string              `bson:"tenant_scope"`
	RuleID          string              `bson:"rule_id"`
	Revision        int64               `bson:"revision"`
	Source          []byte              `bson:"source"`
	Compiled        []byte              `bson:"compiled"`
	ContentHash     string              `bson:"content_hash"`
	CompilerVersion string              `bson:"compiler_version"`
	MatchMode       string              `bson:"match_mode"`
	InputContract   *domain.ContractRef `bson:"input_contract,omitempty"`
	CreatedAt       time.Time           `bson:"created_at"`
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *RevisionDoc) ToDomain() *domain.RuleRevision {
	var compiled []domain.CompiledRule
	_ = json.Unmarshal(d.Compiled, &compiled)

	return &domain.RuleRevision{
		TenantScope:     d.TenantScope,
		RuleID:          d.RuleID,
		Revision:        d.Revision,
		Source:          json.RawMessage(append([]byte(nil), d.Source...)),
		Compiled:        compiled,
		ContentHash:     d.ContentHash,
		CompilerVersion: d.CompilerVersion,
		MatchMode:       domain.MatchMode(d.MatchMode),
		InputContract:   d.InputContract,
		CreatedAt:       d.CreatedAt,
	}
}

// RevisionDocFromDomain converts a domain value to its MongoDB representation.
func RevisionDocFromDomain(tenantScope string, revision *domain.RuleRevision) *RevisionDoc {
	compiled, _ := json.Marshal(revision.Compiled)

	return &RevisionDoc{
		TenantScope:     tenantScope,
		RuleID:          revision.RuleID,
		Revision:        revision.Revision,
		Source:          append([]byte(nil), revision.Source...),
		Compiled:        compiled,
		ContentHash:     revision.ContentHash,
		CompilerVersion: revision.CompilerVersion,
		MatchMode:       string(revision.MatchMode),
		InputContract:   revision.InputContract,
		CreatedAt:       revision.CreatedAt,
	}
}

// ActivationRepository stores and retrieves active rule revisions.
type ActivationRepository struct {
	querier Querier
}

// NewActivationRepository creates and returns the configured value.
func NewActivationRepository(q Querier) *ActivationRepository {
	return &ActivationRepository{querier: q}
}

func (r *ActivationRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Get retrieves the requested value.
func (r *ActivationRepository) Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	var doc ActivationDoc
	err := r.collection(collectionRuleActivations).FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}
	return doc.ToDomain(), nil
}

// Set persists the supplied value.
func (r *ActivationRepository) Set(ctx context.Context, activation *domain.RuleActivation) error {
	coll := r.collection(collectionRuleActivations)
	filter := bson.M{"tenant_scope": activation.TenantScope, "rule_set": activation.RuleSet}
	update := bson.M{
		"$set": bson.M{
			"revision":     activation.Revision,
			"version":      activation.Version,
			"actor":        activation.Actor,
			"activated_at": activation.ActivatedAt,
		},
		"$setOnInsert": bson.M{
			"tenant_scope": activation.TenantScope,
			"rule_set":     activation.RuleSet,
		},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var result ActivationDoc
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	return err
}

// SetWithVersion atomically sets the activation only if the current version matches expectedVersion.
// Returns error if version mismatch (optimistic concurrency conflict).
func (r *ActivationRepository) SetWithVersion(ctx context.Context, activation *domain.RuleActivation, expectedVersion int64) error {
	coll := r.collection(collectionRuleActivations)
	filter := bson.M{
		"tenant_scope": activation.TenantScope,
		"rule_set":     activation.RuleSet,
		"version":      expectedVersion,
	}
	update := bson.M{
		"$set": bson.M{
			"revision":     activation.Revision,
			"version":      expectedVersion + 1,
			"actor":        activation.Actor,
			"activated_at": activation.ActivatedAt,
		},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var result ActivationDoc
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return domain.ErrActivationConflict
		}
		return fmt.Errorf("set activation with version: %w", err)
	}
	if result.Version != expectedVersion+1 {
		return domain.ErrActivationConflict
	}
	return nil
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *ActivationDoc) ToDomain() *domain.RuleActivation {
	return &domain.RuleActivation{
		TenantScope: d.TenantScope,
		RuleSet:     d.RuleSet,
		Revision:    d.Revision,
		Version:     d.Version,
		Actor:       d.Actor,
		ActivatedAt: d.ActivatedAt,
	}
}

// Delete deletes the requested record.
func (r *ActivationRepository) Delete(ctx context.Context, tenantScope string, ruleSet string) error {
	coll := r.collection(collectionRuleActivations)
	_, err := coll.DeleteOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	})
	if err != nil {
		return fmt.Errorf("delete activation: %w", err)
	}
	return nil
}

// List returns matching records.
func (r *ActivationRepository) List(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error) {
	coll := r.collection(collectionRuleActivations)
	cursor, err := coll.Find(ctx, bson.M{"tenant_scope": tenantScope})
	if err != nil {
		return nil, fmt.Errorf("list activations: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var activations []*domain.RuleActivation
	for cursor.Next(ctx) {
		var doc ActivationDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode activation: %w", err)
		}
		activations = append(activations, doc.ToDomain())
	}
	return activations, nil
}
