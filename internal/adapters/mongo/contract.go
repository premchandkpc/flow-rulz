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

// ContractRegistry registers and retrieves contract schemas.
type ContractRegistry struct {
	querier Querier
}

// NewContractRegistry creates and returns the configured value.
func NewContractRegistry(q Querier) *ContractRegistry {
	return &ContractRegistry{querier: q}
}

func (r *ContractRegistry) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// Register publishes an immutable contract version. A semantic change requires a new version.
func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	if err := schema.NormalizeAndValidate(); err != nil { return fmt.Errorf("validate contract: %w", err) }
	existing, err := r.Get(ctx, schema.Name, schema.Version)
	if err != nil { return err }
	if existing != nil {
		if existing.SchemaHash == "" { if err := existing.NormalizeAndValidate(); err != nil { return err } }
		if existing.SchemaHash != schema.SchemaHash { return fmt.Errorf("contract %s@%s is immutable; publish a new version", schema.Name, schema.Version) }
		return nil
	}
	coll := r.collection(collectionContractSchemas)
	doc := ContractSchemaDocFromDomain(schema)
	_, err = coll.InsertOne(ctx, doc)
	if err != nil {
		// A concurrent registration can win the unique-index race. Treat identical content as idempotent.
		existing, getErr := r.Get(ctx, schema.Name, schema.Version)
		if getErr == nil && existing != nil {
			if existing.SchemaHash == "" { _ = existing.NormalizeAndValidate() }
			if existing.SchemaHash == schema.SchemaHash { return nil }
		}
		return fmt.Errorf("register contract: %w", err)
	}
	return nil
}

// Get retrieves the requested value.
func (r *ContractRegistry) Get(ctx context.Context, name, version string) (*domain.ContractSchema, error) {
	coll := r.collection(collectionContractSchemas)
	var doc ContractSchemaDoc

	// lgtm[go/sql-injection] Name and version are scalar BSON values under fixed keys.
	err := coll.FindOne(ctx, bson.M{
		"name":    name,
		"version": version,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get contract: %w", err)
	}
	return doc.ToDomain(), nil
}

// List returns matching records.
func (r *ContractRegistry) List(ctx context.Context, name string) ([]*domain.ContractSchema, error) {
	coll := r.collection(collectionContractSchemas)
	filter := bson.M{}
	if name != "" {
		filter["name"] = name
	}
	cursor, err := coll.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "name", Value: 1}, {Key: "version", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list contracts: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var schemas []*domain.ContractSchema
	for cursor.Next(ctx) {
		var doc ContractSchemaDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode contract: %w", err)
		}
		schemas = append(schemas, doc.ToDomain())
	}
	return schemas, nil
}

// Delete deletes the requested record.
func (r *ContractRegistry) Delete(ctx context.Context, name, version string) error {
	coll := r.collection(collectionContractSchemas)
	_, err := coll.DeleteOne(ctx, bson.M{"name": name, "version": version})
	if err != nil {
		return fmt.Errorf("delete contract: %w", err)
	}
	return nil
}

// ContractSchemaDoc represents a contract schema stored in MongoDB.
type ContractSchemaDoc struct {
	Name          string                          `bson:"name"`
	Version       string                          `bson:"version"`
	Namespace     string                          `bson:"namespace,omitempty"`
	Description   string                          `bson:"description,omitempty"`
	Fields          map[string]domain.ContractField `bson:"fields"`
	Kind            domain.ContractKind             `bson:"kind,omitempty"`
	Parent          *domain.ContractRef              `bson:"parent,omitempty"`
	AllowedChildren []domain.ContractRef             `bson:"allowed_children,omitempty"`
	SchemaHash      string                           `bson:"schema_hash,omitempty"`
	Compatibility string                          `bson:"compatibility,omitempty"`
	Owner         string                          `bson:"owner,omitempty"`
	Deprecated    bool                            `bson:"deprecated,omitempty"`
	CreatedAt     time.Time                       `bson:"created_at"`
	UpdatedAt     time.Time                       `bson:"updated_at"`
}

// ContractSchemaDocFromDomain converts a domain value to its MongoDB representation.
func ContractSchemaDocFromDomain(s *domain.ContractSchema) *ContractSchemaDoc {
	return &ContractSchemaDoc{
		Name:          s.Name,
		Version:       s.Version,
		Namespace:     s.Namespace,
		Description:   s.Description,
		Fields:          s.Fields,
		Kind:            s.Kind,
		Parent:          s.Parent,
		AllowedChildren: s.AllowedChildren,
		SchemaHash:      s.SchemaHash,
		Compatibility: s.Compatibility,
		Owner:         s.Owner,
		Deprecated:    s.Deprecated,
		CreatedAt:     s.CreatedAt,
		UpdatedAt:     s.UpdatedAt,
	}
}

// ToDomain converts the MongoDB representation to a domain value.
func (d *ContractSchemaDoc) ToDomain() *domain.ContractSchema {
	return &domain.ContractSchema{
		Name:          d.Name,
		Version:       d.Version,
		Namespace:     d.Namespace,
		Description:   d.Description,
		Fields:          d.Fields,
		Kind:            d.Kind,
		Parent:          d.Parent,
		AllowedChildren: d.AllowedChildren,
		SchemaHash:      d.SchemaHash,
		Compatibility: d.Compatibility,
		Owner:         d.Owner,
		Deprecated:    d.Deprecated,
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
	}
}

var _ ports.ContractRegistry = (*ContractRegistry)(nil)
