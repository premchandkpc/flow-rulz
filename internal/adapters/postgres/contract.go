package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/jackc/pgx/v5"
)

// ContractRegistry stores and retrieves contract schemas in PostgreSQL.
type ContractRegistry struct {
	db Querier
}

// NewContractRegistry creates a contract registry backed by a PostgreSQL query interface.
func NewContractRegistry(db Querier) *ContractRegistry {
	return &ContractRegistry{db: db}
}

type contractSchemaRow struct {
	Name          string          `db:"name"`
	Version       string          `db:"version"`
	Namespace     string          `db:"namespace"`
	Description   string          `db:"description"`
	Fields          json.RawMessage `db:"fields"`
	Kind            string          `db:"kind"`
	ParentName      *string         `db:"parent_contract_name"`
	ParentVersion   *string         `db:"parent_contract_version"`
	ParentSchemaHash *string        `db:"parent_contract_schema_hash"`
	AllowedChildren json.RawMessage `db:"allowed_children"`
	SchemaHash      *string         `db:"schema_hash"`
	Compatibility string          `db:"compatibility"`
	Owner         string          `db:"owner"`
	Deprecated    bool            `db:"deprecated"`
	CreatedAt     time.Time       `db:"created_at"`
	UpdatedAt     time.Time       `db:"updated_at"`
}

func (cr contractSchemaRow) toDomain() (*domain.ContractSchema, error) {
	var fields map[string]domain.ContractField
	if err := json.Unmarshal(cr.Fields, &fields); err != nil { return nil, fmt.Errorf("unmarshal fields: %w", err) }
	var children []domain.ContractRef
	if len(cr.AllowedChildren) > 0 { if err := json.Unmarshal(cr.AllowedChildren, &children); err != nil { return nil, fmt.Errorf("unmarshal allowed children: %w", err) } }
	var parent *domain.ContractRef
	if cr.ParentName != nil && cr.ParentVersion != nil { parent = &domain.ContractRef{Name:*cr.ParentName, Version:*cr.ParentVersion}
		if cr.ParentSchemaHash != nil { parent.SchemaHash = *cr.ParentSchemaHash } }
	kind := domain.ContractKind(cr.Kind)
	if kind == "" { kind = domain.ContractKindEvent }
	hash := ""
	if cr.SchemaHash != nil { hash = *cr.SchemaHash }

	return &domain.ContractSchema{
		Name:          cr.Name,
		Version:       cr.Version,
		Namespace:     cr.Namespace,
		Description:   cr.Description,
		Fields:          fields,
		Kind:            kind,
		Parent:          parent,
		AllowedChildren: children,
		SchemaHash:      hash,
		Compatibility: cr.Compatibility,
		Owner:         cr.Owner,
		Deprecated:    cr.Deprecated,
		CreatedAt:     cr.CreatedAt,
		UpdatedAt:     cr.UpdatedAt,
	}, nil
}

// Register publishes an immutable contract version. A semantic change requires a new version.
func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	if schema == nil { return fmt.Errorf("contract is required") }
	var parent *domain.ContractSchema
	if schema.Parent != nil {
		var err error
		parent, err = r.Get(ctx, schema.Parent.Name, schema.Parent.Version)
		if err != nil { return fmt.Errorf("resolve parent contract: %w", err) }
		if parent == nil { return fmt.Errorf("parent contract %s@%s not found", schema.Parent.Name, schema.Parent.Version) }
		if err := parent.NormalizeAndValidate(); err != nil { return fmt.Errorf("invalid parent contract: %w", err) }
		if schema.Parent.SchemaHash != "" && schema.Parent.SchemaHash != parent.SchemaHash { return fmt.Errorf("parent contract schema hash mismatch") }
		schema.Parent.SchemaHash = parent.SchemaHash
	}
	if err := schema.NormalizeAndValidate(); err != nil { return fmt.Errorf("validate contract: %w", err) }
	if parent != nil && !parent.AllowsChild(domain.ContractRef{Name:schema.Name, Version:schema.Version, SchemaHash:schema.SchemaHash}) {
		return fmt.Errorf("parent contract %s@%s does not allow child %s@%s", parent.Name, parent.Version, schema.Name, schema.Version)
	}
	existing, err := r.Get(ctx, schema.Name, schema.Version)
	if err != nil { return err }
	if existing != nil {
		if existing.SchemaHash == "" { if err := existing.NormalizeAndValidate(); err != nil { return fmt.Errorf("validate stored contract: %w", err) } }
		if existing.SchemaHash != schema.SchemaHash { return fmt.Errorf("contract %s@%s is immutable; publish a new version", schema.Name, schema.Version) }
		return nil
	}
	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil { return fmt.Errorf("marshal fields: %w", err) }
	childrenJSON, err := json.Marshal(schema.AllowedChildren)
	if err != nil { return fmt.Errorf("marshal allowed children: %w", err) }
	var parentName, parentVersion, parentSchemaHash interface{}
	if schema.Parent != nil { parentName, parentVersion, parentSchemaHash = schema.Parent.Name, schema.Parent.Version, schema.Parent.SchemaHash }
	_, err = r.db.Exec(ctx, `
		INSERT INTO contract_schemas (name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (name, version) DO NOTHING
	`, schema.Name, schema.Version, schema.Namespace, schema.Description, fieldsJSON, string(schema.Kind), parentName, parentVersion, parentSchemaHash, childrenJSON, schema.SchemaHash, schema.Compatibility, schema.Owner, schema.Deprecated, schema.CreatedAt, schema.UpdatedAt)
	if err != nil { return fmt.Errorf("register contract: %w", err) }
	// A concurrent registration may have won the insert race. Verify it did not publish a different schema.
	existing, err = r.Get(ctx, schema.Name, schema.Version)
	if err != nil { return err }
	if existing == nil { return fmt.Errorf("contract registration was not persisted") }
	if existing.SchemaHash == "" { if err := existing.NormalizeAndValidate(); err != nil { return err } }
	if existing.SchemaHash != schema.SchemaHash { return fmt.Errorf("contract %s@%s is immutable; publish a new version", schema.Name, schema.Version) }
	return nil
}

// Get retrieves a contract schema by name and version, or returns nil when it does not exist.
func (r *ContractRegistry) Get(ctx context.Context, name, version string) (*domain.ContractSchema, error) {
	row := r.db.QueryRow(ctx, `
		SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
		FROM contract_schemas
		WHERE name = $1 AND version = $2
	`, name, version)

	var cr contractSchemaRow
	err := row.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Kind, &cr.ParentName, &cr.ParentVersion, &cr.ParentSchemaHash, &cr.AllowedChildren, &cr.SchemaHash, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get contract: %w", err)
	}

	schema, err := cr.toDomain()
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// List returns contract schemas, optionally filtered by contract name.
func (r *ContractRegistry) List(ctx context.Context, name string) ([]*domain.ContractSchema, error) {
	var rows pgx.Rows
	var err error

	if name != "" {
		rows, err = r.db.Query(ctx, `
			SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
			FROM contract_schemas
			WHERE name = $1
			ORDER BY version DESC
		`, name)
	} else {
		rows, err = r.db.Query(ctx, `
			SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
			FROM contract_schemas
			ORDER BY name, version DESC
		`)
	}
	if err != nil {
		return nil, fmt.Errorf("list contracts: %w", err)
	}
	defer rows.Close()

	var schemas []*domain.ContractSchema
	for rows.Next() {
		var cr contractSchemaRow
		if err := rows.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Kind, &cr.ParentName, &cr.ParentVersion, &cr.AllowedChildren, &cr.SchemaHash, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan contract: %w", err)
		}

		schema, err := cr.toDomain()
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contracts: %w", err)
	}
	return schemas, nil
}

// Delete removes a contract schema by name and version.
func (r *ContractRegistry) Delete(ctx context.Context, name, version string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM contract_schemas WHERE name = $1 AND version = $2`, name, version)
	if err != nil {
		return fmt.Errorf("delete contract: %w", err)
	}
	return nil
}

var _ ports.ContractRegistry = (*ContractRegistry)(nil)
