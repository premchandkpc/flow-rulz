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
		return nil, fmt.Errorf("contract %s@%s not found", name, version)
	}
	return schema, nil
}

// Register stores a contract schema.
func (r *ContractRegistry) Register(_ context.Context, schema *domain.ContractSchema) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[r.key(schema.Name, schema.Version)] = schema
	return nil
}

// List returns all contract schemas matching the given name.
func (r *ContractRegistry) List(_ context.Context, name string) ([]domain.ContractSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []domain.ContractSchema
	for _, s := range r.schemas {
		if s.Name == name {
			result = append(result, *s)
		}
	}
	return result, nil
}
