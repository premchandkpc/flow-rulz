package flowrulz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ContractRegistry manages schema contracts for validation
type ContractRegistry struct {
	mu        sync.RWMutex
	contracts map[string]*Contract
}

// Contract represents a JSON schema contract
type Contract struct {
	Name         string                 `json:"name"`
	Version      string                 `json:"version"`
	Schema       map[string]interface{} `json:"schema"`
	Required     []string               `json:"required,omitempty"`
	PartitionKey string                 `json:"partition_key,omitempty"`
}

// NewContractRegistry creates a new registry, optionally loading from a directory
func NewContractRegistry(registryPath string) (*ContractRegistry, error) {
	reg := &ContractRegistry{
		contracts: make(map[string]*Contract),
	}

	if registryPath != "" {
		if err := reg.LoadFromDir(registryPath); err != nil {
			return nil, err
		}
	}

	return reg, nil
}

// Register adds a contract to the registry
func (r *ContractRegistry) Register(contract *Contract) error {
	if contract == nil {
		return ErrInvalidArgument("contract is nil")
	}
	if contract.Name == "" {
		return ErrInvalidArgument("contract name is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.contractKey(contract.Name, contract.Version)
	r.contracts[key] = contract
	return nil
}

// Get retrieves a contract by name and version
func (r *ContractRegistry) Get(name, version string) (*Contract, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.contractKey(name, version)
	c, ok := r.contracts[key]
	return c, ok
}

// Validate validates a payload against a contract
func (r *ContractRegistry) Validate(ruleSet string, payload any) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Try exact match first
	key := r.contractKey(ruleSet, "")
	if c, ok := r.contracts[key]; ok {
		return r.validatePayload(c, payload)
	}

	// Try with version suffix (e.g., "order.created.v1")
	for k, c := range r.contracts {
		if strings.HasPrefix(k, ruleSet+".") || strings.HasPrefix(k, ruleSet+"_") {
			return r.validatePayload(c, payload)
		}
	}

	// No contract found - allow if no strict mode
	return nil
}

// validatePayload validates payload against contract schema
func (r *ContractRegistry) validatePayload(c *Contract, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// Simple validation: check required fields
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	for _, req := range c.Required {
		if _, ok := obj[req]; !ok {
			return fmt.Errorf("required field missing: %s", req)
		}
	}

	// TODO: Full JSON Schema validation
	return nil
}

// LoadFromDir loads contracts from a directory
func (r *ContractRegistry) LoadFromDir(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("read contract dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		path := filepath.Join(dirPath, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read contract %s: %w", entry.Name(), err)
		}

		var contract Contract
		if err := json.Unmarshal(data, &contract); err != nil {
			return fmt.Errorf("parse contract %s: %w", entry.Name(), err)
		}

		if err := r.Register(&contract); err != nil {
			return fmt.Errorf("register contract %s: %w", entry.Name(), err)
		}
	}

	return nil
}

func (r *ContractRegistry) contractKey(name, version string) string {
	if version == "" {
		return name
	}
	return name + "." + version
}
