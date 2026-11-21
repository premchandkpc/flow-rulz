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
	Name             string          `db:"name"`
	Version          string          `db:"version"`
	Namespace        string          `db:"namespace"`
	Description      string          `db:"description"`
	Fields           json.RawMessage `db:"fields"`
	Kind             string          `db:"kind"`
	ParentName       *string         `db:"parent_contract_name"`
	ParentVersion    *string         `db:"parent_contract_version"`
	ParentSchemaHash *string         `db:"parent_contract_schema_hash"`
	AllowedChildren  json.RawMessage `db:"allowed_children"`
	SchemaHash       *string         `db:"schema_hash"`
	Compatibility    string          `db:"compatibility"`
	Owner            string          `db:"owner"`
	Deprecated       bool            `db:"deprecated"`
	CreatedAt        time.Time       `db:"created_at"`
	UpdatedAt        time.Time       `db:"updated_at"`
}

func (cr contractSchemaRow) toDomain() (*domain.ContractSchema, error) {
	schema := cr.metadata()
	if err := json.Unmarshal(cr.Fields, &schema.Fields); err != nil {
		return nil, fmt.Errorf("unmarshal fields: %w", err)
	}
	if len(cr.AllowedChildren) > 0 {
		if err := json.Unmarshal(cr.AllowedChildren, &schema.AllowedChildren); err != nil {
			return nil, fmt.Errorf("unmarshal allowed children: %w", err)
		}
	}
	schema.Parent = cr.parentReference()
	schema.SchemaHash = optionalString(cr.SchemaHash)
	return schema, nil
}

// Register publishes an immutable contract version. A semantic change requires a new version.
func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	if err := r.validateRegistration(ctx, schema); err != nil {
		return err
	}
	existing, err := r.Get(ctx, schema.Name, schema.Version)
	if err != nil {
		return err
	}
	if existing != nil {
		return validateContractIdentity(existing, schema)
	}
	return r.insertContract(ctx, schema)
}

// Get retrieves a contract schema by name and version, or returns nil when it does not exist.
func (r *ContractRegistry) Get(ctx context.Context, name, version string) (*domain.ContractSchema, error) {
	row := r.db.QueryRow(ctx, contractRegistryGetSQL, name, version)

	var cr contractSchemaRow
	err := row.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Kind, &cr.ParentName, &cr.ParentVersion, &cr.ParentSchemaHash, &cr.AllowedChildren, &cr.SchemaHash, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get contract: %w", err)
	}

	return cr.toDomain()
}

// List returns contract schemas, optionally filtered by contract name.
func (r *ContractRegistry) List(ctx context.Context, name string) ([]*domain.ContractSchema, error) {
	rows, err := r.queryContracts(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("list contracts: %w", err)
	}
	defer rows.Close()
	return scanContracts(rows)
}

// Delete removes a contract schema by name and version.
// Returns ErrContractReferenced if the contract is referenced by rule revisions or bindings.
func (r *ContractRegistry) Delete(ctx context.Context, name, version string) error {
	if err := r.checkRevisionReferences(ctx, name, version); err != nil {
		return err
	}
	if err := r.checkBindingReferences(ctx, name, version); err != nil {
		return err
	}
	_, err := r.db.Exec(ctx, `DELETE FROM contract_schemas WHERE name = $1 AND version = $2`, name, version)
	if err != nil {
		return fmt.Errorf("delete contract: %w", err)
	}
	return nil
}

var _ ports.ContractRegistry = (*ContractRegistry)(nil)

const contractRegistryRegisterSQL = `
		INSERT INTO contract_schemas (name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (name, version) DO NOTHING
	`

const contractRegistryGetSQL = `
		SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
		FROM contract_schemas
		WHERE name = $1 AND version = $2
	`

const contractRegistryListSQL = `
			SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
			FROM contract_schemas
			WHERE name = $1
			ORDER BY version DESC
		`

const contractRegistryListSQL2 = `
			SELECT name, version, namespace, description, fields, kind, parent_contract_name, parent_contract_version, parent_contract_schema_hash, allowed_children, schema_hash, compatibility, owner, deprecated, created_at, updated_at
			FROM contract_schemas
			ORDER BY name, version DESC
		`

const contractRegistryDeleteSQL = `
		SELECT COUNT(*) FROM rule_revisions
		WHERE input_contract IS NOT NULL
		  AND input_contract->>'name' = $1
		  AND input_contract->>'version' = $2
	`

const contractRegistryDeleteSQL2 = `
		SELECT COUNT(*) FROM contract_bindings
		WHERE contract_name = $1 AND contract_version = $2
	`

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (cr contractSchemaRow) metadata() *domain.ContractSchema {
	kind := domain.ContractKind(cr.Kind)
	if kind == "" {
		kind = domain.ContractKindEvent
	}
	return &domain.ContractSchema{
		Name: cr.Name, Version: cr.Version, Namespace: cr.Namespace,
		Description: cr.Description, Kind: kind, Compatibility: cr.Compatibility,
		Owner: cr.Owner, Deprecated: cr.Deprecated, CreatedAt: cr.CreatedAt, UpdatedAt: cr.UpdatedAt,
	}
}

func (cr contractSchemaRow) parentReference() *domain.ContractRef {
	if cr.ParentName == nil || cr.ParentVersion == nil {
		return nil
	}
	return &domain.ContractRef{Name: *cr.ParentName, Version: *cr.ParentVersion, SchemaHash: optionalString(cr.ParentSchemaHash)}
}

func (r *ContractRegistry) validateRegistration(ctx context.Context, schema *domain.ContractSchema) error {
	if schema == nil {
		return fmt.Errorf("contract is required")
	}
	parent, err := r.resolveParent(ctx, schema.Parent)
	if err != nil {
		return err
	}
	if err := schema.NormalizeAndValidate(); err != nil {
		return fmt.Errorf("validate contract: %w", err)
	}
	return validateContractChild(parent, schema)
}

func (r *ContractRegistry) resolveParent(ctx context.Context, ref *domain.ContractRef) (*domain.ContractSchema, error) {
	if ref == nil {
		return nil, nil
	}
	parent, err := r.Get(ctx, ref.Name, ref.Version)
	if err != nil {
		return nil, fmt.Errorf("resolve parent contract: %w", err)
	}
	if parent == nil {
		return nil, fmt.Errorf("parent contract %s@%s not found", ref.Name, ref.Version)
	}
	return validatedParent(parent, ref)
}

func validatedParent(parent *domain.ContractSchema, ref *domain.ContractRef) (*domain.ContractSchema, error) {
	if err := parent.NormalizeAndValidate(); err != nil {
		return nil, fmt.Errorf("invalid parent contract: %w", err)
	}
	if ref.SchemaHash != "" && ref.SchemaHash != parent.SchemaHash {
		return nil, fmt.Errorf("parent contract schema hash mismatch")
	}
	ref.SchemaHash = parent.SchemaHash
	return parent, nil
}

func validateContractChild(parent, schema *domain.ContractSchema) error {
	child := domain.ContractRef{Name: schema.Name, Version: schema.Version, SchemaHash: schema.SchemaHash}
	if parent != nil && !parent.AllowsChild(child) {
		return fmt.Errorf("parent contract %s@%s does not allow child %s@%s", parent.Name, parent.Version, schema.Name, schema.Version)
	}
	return nil
}

func validateContractIdentity(existing, schema *domain.ContractSchema) error {
	if existing.SchemaHash == "" {
		if err := existing.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("validate stored contract: %w", err)
		}
	}
	if existing.SchemaHash != schema.SchemaHash {
		return fmt.Errorf("contract %s@%s is immutable; publish a new version", schema.Name, schema.Version)
	}
	return nil
}

func contractParentColumns(schema *domain.ContractSchema) (any, any, any) {
	if schema.Parent == nil {
		return nil, nil, nil
	}
	return schema.Parent.Name, schema.Parent.Version, schema.Parent.SchemaHash
}

func marshalContract(schema *domain.ContractSchema) ([]byte, []byte, error) {
	fields, err := json.Marshal(schema.Fields)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal fields: %w", err)
	}
	children, err := json.Marshal(schema.AllowedChildren)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal allowed children: %w", err)
	}
	return fields, children, nil
}

func (r *ContractRegistry) insertContract(ctx context.Context, schema *domain.ContractSchema) error {
	fields, children, err := marshalContract(schema)
	if err != nil {
		return err
	}
	parentName, parentVersion, parentHash := contractParentColumns(schema)
	_, err = r.db.Exec(ctx, contractRegistryRegisterSQL, schema.Name, schema.Version, schema.Namespace, schema.Description, fields, string(schema.Kind), parentName, parentVersion, parentHash, children, schema.SchemaHash, schema.Compatibility, schema.Owner, schema.Deprecated, schema.CreatedAt, schema.UpdatedAt)
	if err != nil {
		return fmt.Errorf("register contract: %w", err)
	}
	return r.verifyRegistration(ctx, schema)
}

func (r *ContractRegistry) verifyRegistration(ctx context.Context, schema *domain.ContractSchema) error {
	existing, err := r.Get(ctx, schema.Name, schema.Version)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("contract registration was not persisted")
	}
	if existing.SchemaHash == "" {
		if err := existing.NormalizeAndValidate(); err != nil {
			return err
		}
	}
	return validateContractIdentity(existing, schema)
}

func (r *ContractRegistry) queryContracts(ctx context.Context, name string) (pgx.Rows, error) {
	if name != "" {
		return r.db.Query(ctx, contractRegistryListSQL, name)
	}
	return r.db.Query(ctx, contractRegistryListSQL2)
}

func scanContract(row pgx.Row) (*domain.ContractSchema, error) {
	var cr contractSchemaRow
	if err := row.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Kind, &cr.ParentName, &cr.ParentVersion, &cr.ParentSchemaHash, &cr.AllowedChildren, &cr.SchemaHash, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt); err != nil {
		return nil, fmt.Errorf("scan contract: %w", err)
	}
	return cr.toDomain()
}

func scanContracts(rows pgx.Rows) ([]*domain.ContractSchema, error) {
	var schemas []*domain.ContractSchema
	for rows.Next() {
		schema, err := scanContract(rows)
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

func (r *ContractRegistry) checkRevisionReferences(ctx context.Context, name, version string) error {
	var count int
	if err := r.db.QueryRow(ctx, contractRegistryDeleteSQL, name, version).Scan(&count); err != nil {
		return fmt.Errorf("check rule_revisions references: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("%w: contract %s@%s is referenced by %d rule revision(s)", domain.ErrContractReferenced, name, version, count)
	}
	return nil
}

func (r *ContractRegistry) checkBindingReferences(ctx context.Context, name, version string) error {
	var count int
	if err := r.db.QueryRow(ctx, contractRegistryDeleteSQL2, name, version).Scan(&count); err != nil {
		return fmt.Errorf("check contract_bindings references: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("%w: contract %s@%s is referenced by %d binding(s)", domain.ErrContractReferenced, name, version, count)
	}
	return nil
}
