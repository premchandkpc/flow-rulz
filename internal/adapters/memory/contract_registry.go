// Package memory provides in-memory adapters used for tests and local development.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// ContractRegistry stores contract schemas in memory.
type ContractRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*domain.ContractSchema
}

// NewContractRegistry creates an empty in-memory contract registry.
func NewContractRegistry() *ContractRegistry {
	return &ContractRegistry{
		schemas: make(map[string]*domain.ContractSchema),
	}
}

func (r *ContractRegistry) key(name, version string) string {
	return name + ":" + version
}

// Get retrieves a contract schema by name and version.
func (r *ContractRegistry) Get(_ context.Context, name string, version string) (*domain.ContractSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.schemas[r.key(name, version)]
	if !ok {
		return nil, nil
	}
	return schema, nil
}

// Register stores a contract schema.
func (r *ContractRegistry) Register(_ context.Context, schema *domain.ContractSchema) error {
	if err := schema.NormalizeAndValidate(); err != nil { return fmt.Errorf("validate contract: %w", err) }
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.key(schema.Name, schema.Version)
	if existing, ok := r.schemas[key]; ok {
		if existing.SchemaHash == "" { if err := existing.NormalizeAndValidate(); err != nil { return err } }
		if existing.SchemaHash != schema.SchemaHash { return fmt.Errorf("contract %s@%s is immutable; publish a new version", schema.Name, schema.Version) }
		return nil
	}
	if schema.Parent != nil {
		parent, ok := r.schemas[r.key(schema.Parent.Name, schema.Parent.Version)]
		if !ok { return fmt.Errorf("parent contract %s@%s not found", schema.Parent.Name, schema.Parent.Version) }
		if err := parent.NormalizeAndValidate(); err != nil { return err }
		if schema.Parent.SchemaHash != "" && schema.Parent.SchemaHash != parent.SchemaHash { return fmt.Errorf("parent contract schema hash mismatch") }
		schema.Parent.SchemaHash = parent.SchemaHash
		if err := schema.NormalizeAndValidate(); err != nil { return err }
		if !parent.AllowsChild(domain.ContractRef{Name:schema.Name, Version:schema.Version, SchemaHash:schema.SchemaHash}) {
			return fmt.Errorf("parent contract %s@%s does not allow child %s@%s", parent.Name, parent.Version, schema.Name, schema.Version)
		}
	}
	copy := *schema
	r.schemas[key] = &copy
	return nil
}

// List returns all contract schemas matching the given name.
func (r *ContractRegistry) List(_ context.Context, name string) ([]*domain.ContractSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*domain.ContractSchema
	for _, s := range r.schemas {
		if name == "" || s.Name == name {
			copy := *s
			result = append(result, &copy)
		}
	}
	return result, nil
}
