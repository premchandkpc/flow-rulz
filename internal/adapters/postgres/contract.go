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
	Fields        json.RawMessage `db:"fields"`
	Compatibility string          `db:"compatibility"`
	Owner         string          `db:"owner"`
	Deprecated    bool            `db:"deprecated"`
	CreatedAt     time.Time       `db:"created_at"`
	UpdatedAt     time.Time       `db:"updated_at"`
}

func (cr contractSchemaRow) toDomain() (*domain.ContractSchema, error) {
	var fields map[string]domain.ContractField
	if err := json.Unmarshal(cr.Fields, &fields); err != nil {
		return nil, fmt.Errorf("unmarshal fields: %w", err)
	}

	return &domain.ContractSchema{
		Name:          cr.Name,
		Version:       cr.Version,
		Namespace:     cr.Namespace,
		Description:   cr.Description,
		Fields:        fields,
		Compatibility: cr.Compatibility,
		Owner:         cr.Owner,
		Deprecated:    cr.Deprecated,
		CreatedAt:     cr.CreatedAt,
		UpdatedAt:     cr.UpdatedAt,
	}, nil
}

// Register inserts or updates a contract schema.
func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil {
		return fmt.Errorf("marshal fields: %w", err)
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO contract_schemas (name, version, namespace, description, fields, compatibility, owner, deprecated, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (name, version) DO UPDATE SET
			namespace = EXCLUDED.namespace,
			description = EXCLUDED.description,
			fields = EXCLUDED.fields,
			compatibility = EXCLUDED.compatibility,
			owner = EXCLUDED.owner,
			deprecated = EXCLUDED.deprecated,
			updated_at = EXCLUDED.updated_at
	`, schema.Name, schema.Version, schema.Namespace, schema.Description, fieldsJSON, schema.Compatibility, schema.Owner, schema.Deprecated, schema.CreatedAt, schema.UpdatedAt)
	if err != nil {
		return fmt.Errorf("register contract: %w", err)
	}
	return nil
}

// Get retrieves a contract schema by name and version, or returns nil when it does not exist.
func (r *ContractRegistry) Get(ctx context.Context, name, version string) (*domain.ContractSchema, error) {
	row := r.db.QueryRow(ctx, `
		SELECT name, version, namespace, description, fields, compatibility, owner, deprecated, created_at, updated_at
		FROM contract_schemas
		WHERE name = $1 AND version = $2
	`, name, version)

	var cr contractSchemaRow
	err := row.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt)
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
			SELECT name, version, namespace, description, fields, compatibility, owner, deprecated, created_at, updated_at
			FROM contract_schemas
			WHERE name = $1
			ORDER BY version DESC
		`, name)
	} else {
		rows, err = r.db.Query(ctx, `
			SELECT name, version, namespace, description, fields, compatibility, owner, deprecated, created_at, updated_at
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
		if err := rows.Scan(&cr.Name, &cr.Version, &cr.Namespace, &cr.Description, &cr.Fields, &cr.Compatibility, &cr.Owner, &cr.Deprecated, &cr.CreatedAt, &cr.UpdatedAt); err != nil {
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
