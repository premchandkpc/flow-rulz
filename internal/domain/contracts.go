package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

// ContractKind distinguishes event, command, effect, and common envelope schemas.
type ContractKind string

const (
	// ContractKindEnvelope identifies the common message envelope schema.
	ContractKindEnvelope ContractKind = "envelope"
	// ContractKindEvent identifies an event payload schema.
	ContractKindEvent ContractKind = "event"
	// ContractKindCommand identifies a command payload schema.
	ContractKindCommand ContractKind = "command"
	// ContractKindEffect identifies an effect payload schema.
	ContractKindEffect ContractKind = "effect"
)

// NormalizeAndValidate validates a contract definition and computes its stable schema hash.
// A published (name, version) must be immutable; descriptions and timestamps are not hashed.
func (s *ContractSchema) NormalizeAndValidate() error {
	if s == nil { return fmt.Errorf("contract is required") }
	s.Name = strings.TrimSpace(s.Name)
	s.Version = strings.TrimSpace(s.Version)
	if s.Name == "" || s.Version == "" { return fmt.Errorf("contract name and version are required") }
	if s.Kind == "" { s.Kind = ContractKindEvent }
	switch s.Kind {
	case ContractKindEnvelope, ContractKindEvent, ContractKindCommand, ContractKindEffect:
	default: return fmt.Errorf("unsupported contract kind %q", s.Kind)
	}
	if len(s.Fields) == 0 { return fmt.Errorf("contract %s@%s must define at least one field", s.Name, s.Version) }
	for name, field := range s.Fields {
		if strings.TrimSpace(name) == "" { return fmt.Errorf("contract field name cannot be empty") }
		if err := validateContractField(name, field, 0); err != nil { return err }
	}
	if s.Parent != nil && (strings.TrimSpace(s.Parent.Name) == "" || strings.TrimSpace(s.Parent.Version) == "") {
		return fmt.Errorf("parent contract must include name and version")
	}
	seen := make(map[string]struct{}, len(s.AllowedChildren))
	for _, child := range s.AllowedChildren {
		if child.Name == "" || child.Version == "" { return fmt.Errorf("allowed child contract must include name and version") }
		key := child.Name + "@" + child.Version
		if _, ok := seen[key]; ok { return fmt.Errorf("duplicate allowed child contract %s", key) }
		seen[key] = struct{}{}
	}
	hash, err := s.ComputeSchemaHash()
	if err != nil { return err }
	if s.SchemaHash != "" && s.SchemaHash != hash { return fmt.Errorf("schema_hash mismatch: supplied %s, computed %s", s.SchemaHash, hash) }
	s.SchemaHash = hash
	return nil
}

// ComputeSchemaHash hashes the contract's semantic schema, excluding mutable descriptive metadata.
func (s *ContractSchema) ComputeSchemaHash() (string, error) {
	canonical := struct {
		Name string `json:"name"`
		Version string `json:"version"`
		Kind ContractKind `json:"kind"`
		Parent *ContractRef `json:"parent,omitempty"`
		AllowedChildren []ContractRef `json:"allowed_children,omitempty"`
		Fields map[string]ContractField `json:"fields"`
	}{s.Name, s.Version, s.Kind, s.Parent, s.AllowedChildren, s.Fields}
	raw, err := json.Marshal(canonical)
	if err != nil { return "", fmt.Errorf("marshal contract schema: %w", err) }
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ValidatePayload validates an incoming JSON payload against a registered contract schema.
func (s *ContractSchema) ValidatePayload(payload json.RawMessage) error {
	if s == nil { return fmt.Errorf("contract schema is required") }
	var value map[string]interface{}
	if err := json.Unmarshal(payload, &value); err != nil { return fmt.Errorf("payload must be a JSON object: %w", err) }
	if value == nil { return fmt.Errorf("payload must be a JSON object") }
	return validateObject("$", value, s.Fields, 0)
}

func validateContractField(path string, f ContractField, depth int) error {
	if depth > 32 { return fmt.Errorf("%s exceeds maximum schema nesting depth", path) }
	switch f.Type {
	case "string", "number", "integer", "boolean", "object", "array", "null":
	default: return fmt.Errorf("%s has unsupported type %q", path, f.Type)
	}
	if f.Type == "object" {
		for name, child := range f.Properties {
			if err := validateContractField(path+"."+name, child, depth+1); err != nil { return err }
		}
	}
	if f.Type == "array" {
		if f.Items == nil { return fmt.Errorf("%s array must define items", path) }
		if err := validateContractField(path+"[]", *f.Items, depth+1); err != nil { return err }
	}
	return nil
}

func validateObject(path string, value map[string]interface{}, fields map[string]ContractField, depth int) error {
	if depth > 32 { return fmt.Errorf("%s exceeds maximum payload nesting depth", path) }
	for name, field := range fields {
		v, exists := value[name]
		if !exists {
			if field.Required { return fmt.Errorf("%s.%s is required", path, name) }
			continue
		}
		if err := validateValue(path+"."+name, v, field, depth+1); err != nil { return err }
	}
	for name := range value {
		if _, ok := fields[name]; !ok { return fmt.Errorf("%s.%s is not declared by the contract", path, name) }
	}
	return nil
}

func validateValue(path string, value interface{}, field ContractField, depth int) error {
	valid := false
	switch field.Type {
	case "string": _, valid = value.(string)
	case "boolean": _, valid = value.(bool)
	case "number":
		_, valid = value.(float64)
	case "integer":
		var n float64
		n, valid = value.(float64)
		valid = valid && math.Trunc(n) == n
	case "object":
		obj, ok := value.(map[string]interface{})
		if ok { valid = true; if err := validateObject(path, obj, field.Properties, depth+1); err != nil { return err } }
	case "array":
		items, ok := value.([]interface{})
		if ok {
			valid = true
			for i, item := range items { if err := validateValue(fmt.Sprintf("%s[%d]", path, i), item, *field.Items, depth+1); err != nil { return err } }
		}
	case "null": valid = value == nil
	}
	if !valid { return fmt.Errorf("%s must be %s", path, field.Type) }
	if len(field.Enum) > 0 {
		match := false
		for _, candidate := range field.Enum { if reflect.DeepEqual(candidate, value) { match = true; break } }
		if !match { values := make([]string, 0, len(field.Enum)); for _, v := range field.Enum { raw, _ := json.Marshal(v); values = append(values, string(raw)) }; sort.Strings(values); return fmt.Errorf("%s must be one of [%s]", path, strings.Join(values, ", ")) }
	}
	return nil
}

// AllowsChild reports whether this parent contract explicitly permits a child contract identity.
func (s *ContractSchema) AllowsChild(child ContractRef) bool {
	if s == nil { return false }
	for _, allowed := range s.AllowedChildren {
		if allowed.Name == child.Name && allowed.Version == child.Version && (allowed.SchemaHash == "" || allowed.SchemaHash == child.SchemaHash) { return true }
	}
	return false
}
