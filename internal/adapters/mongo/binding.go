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

// BindingRepository provides MongoDB implementation of contract-to-ruleset bindings.
type BindingRepository struct {
	querier Querier
}

// NewBindingRepository creates a new MongoDB binding repository.
func NewBindingRepository(q Querier) *BindingRepository {
	return &BindingRepository{querier: q}
}

func (r *BindingRepository) collection() *mongo.Collection {
	return r.querier.Collection(collectionContractBindings)
}

// BindingDoc represents a contract binding in MongoDB.
type BindingDoc struct {
	ID               string                    `bson:"_id"`
	TenantID         string                    `bson:"tenant_id"`
	ContractName     string                    `bson:"contract_name"`
	ContractVersion  string                    `bson:"contract_version"`
	ContractHash     string                    `bson:"contract_hash"`
	RuleSet          string                    `bson:"rule_set"`
	Priority         int                       `bson:"priority"`
	ActivationPolicy string                    `bson:"activation_policy"`
	CreatedAt        time.Time                 `bson:"created_at"`
	UpdatedAt        time.Time                 `bson:"updated_at"`
}

// ToDomain converts to domain.ContractBinding.
func (d *BindingDoc) ToDomain() *domain.ContractBinding {
	return &domain.ContractBinding{
		ID:               d.ID,
		TenantID:         d.TenantID,
		ContractName:     d.ContractName,
		ContractVersion:  d.ContractVersion,
		ContractHash:     d.ContractHash,
		RuleSet:          d.RuleSet,
		Priority:         d.Priority,
		ActivationPolicy: domain.BindingActivationPolicy(d.ActivationPolicy),
		CreatedAt:        d.CreatedAt,
		UpdatedAt:        d.UpdatedAt,
	}
}

// BindingDocFromDomain converts from domain.ContractBinding.
func BindingDocFromDomain(b *domain.ContractBinding) *BindingDoc {
	if b.ID == "" {
		b.ID = domain.NewID()
	}
	now := time.Now().UTC()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	b.UpdatedAt = now
	return &BindingDoc{
		ID:               b.ID,
		TenantID:         b.TenantID,
		ContractName:     b.ContractName,
		ContractVersion:  b.ContractVersion,
		ContractHash:     b.ContractHash,
		RuleSet:          b.RuleSet,
		Priority:         b.Priority,
		ActivationPolicy: string(b.ActivationPolicy),
		CreatedAt:        b.CreatedAt,
		UpdatedAt:        b.UpdatedAt,
	}
}

// Save inserts or updates a binding.
func (r *BindingRepository) Save(ctx context.Context, binding *domain.ContractBinding) error {
	doc := BindingDocFromDomain(binding)
	coll := r.collection()
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": doc.ID}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("save binding: %w", err)
	}
	return nil
}

// Get retrieves a binding by ID.
func (r *BindingRepository) Get(ctx context.Context, tenantID, bindingID string) (*domain.ContractBinding, error) {
	coll := r.collection()
	var doc BindingDoc
	err := coll.FindOne(ctx, bson.M{"_id": bindingID, "tenant_id": tenantID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get binding: %w", err)
	}
	return doc.ToDomain(), nil
}

// ListByContract returns all bindings for a specific contract.
func (r *BindingRepository) ListByContract(ctx context.Context, tenantID, contractName, contractVersion string) ([]*domain.ContractBinding, error) {
	coll := r.collection()
	cursor, err := coll.Find(ctx, bson.M{
		"tenant_id":       tenantID,
		"contract_name":   contractName,
		"contract_version": contractVersion,
	}, options.Find().SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list bindings by contract: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var bindings []*domain.ContractBinding
	for cursor.Next(ctx) {
		var doc BindingDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode binding: %w", err)
		}
		bindings = append(bindings, doc.ToDomain())
	}
	return bindings, nil
}

// ListByRuleSet returns all bindings for a specific ruleset.
func (r *BindingRepository) ListByRuleSet(ctx context.Context, tenantID, ruleSet string) ([]*domain.ContractBinding, error) {
	coll := r.collection()
	cursor, err := coll.Find(ctx, bson.M{
		"tenant_id": tenantID,
		"rule_set":  ruleSet,
	}, options.Find().SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list bindings by ruleset: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var bindings []*domain.ContractBinding
	for cursor.Next(ctx) {
		var doc BindingDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode binding: %w", err)
		}
		bindings = append(bindings, doc.ToDomain())
	}
	return bindings, nil
}

// Delete removes a binding by ID.
func (r *BindingRepository) Delete(ctx context.Context, tenantID, bindingID string) error {
	coll := r.collection()
	_, err := coll.DeleteOne(ctx, bson.M{"_id": bindingID, "tenant_id": tenantID})
	if err != nil {
		return fmt.Errorf("delete binding: %w", err)
	}
	return nil
}

var _ ports.BindingRepository = (*BindingRepository)(nil)