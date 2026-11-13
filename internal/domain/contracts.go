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
	if s == nil {
		return fmt.Errorf("contract is required")
	}
	if err := s.normalizeIdentity(); err != nil {
		return err
	}
	if err := s.validateFields(); err != nil {
		return err
	}
	if err := s.validateRelations(); err != nil {
		return err
	}
	return s.applySchemaHash()
}

// normalizeIdentity trims the contract identity, checks it is present, then
// resolves and checks its kind before any field validation runs.
func (s *ContractSchema) normalizeIdentity() error {
	s.Name = strings.TrimSpace(s.Name)
	s.Version = strings.TrimSpace(s.Version)
	if s.Name == "" || s.Version == "" {
		return fmt.Errorf("contract name and version are required")
	}
	return s.normalizeKind()
}

// normalizeKind defaults the contract kind and rejects unsupported values.
func (s *ContractSchema) normalizeKind() error {
	if s.Kind == "" {
		s.Kind = ContractKindEvent
	}
	switch s.Kind {
	case ContractKindEnvelope, ContractKindEvent, ContractKindCommand, ContractKindEffect:
	default:
		return fmt.Errorf("unsupported contract kind %q", s.Kind)
	}
	return nil
}

// validateFields requires a non-empty field map and validates every declared field.
func (s *ContractSchema) validateFields() error {
	if len(s.Fields) == 0 {
		return fmt.Errorf("contract %s@%s must define at least one field", s.Name, s.Version)
	}
	for name, field := range s.Fields {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("contract field name cannot be empty")
		}
		if err := validateContractField(name, field, 0); err != nil {
			return err
		}
	}
	return nil
}

// validateRelations checks the optional parent and every allowed child contract.
func (s *ContractSchema) validateRelations() error {
	if s.Parent != nil && (strings.TrimSpace(s.Parent.Name) == "" || strings.TrimSpace(s.Parent.Version) == "") {
		return fmt.Errorf("parent contract must include name and version")
	}
	seen := make(map[string]struct{}, len(s.AllowedChildren))
	for _, child := range s.AllowedChildren {
		if err := validateAllowedChild(child, seen); err != nil {
			return err
		}
	}
	return nil
}

// validateAllowedChild checks one allowed child contract and records its identity.
func validateAllowedChild(child ContractRef, seen map[string]struct{}) error {
	if child.Name == "" || child.Version == "" {
		return fmt.Errorf("allowed child contract must include name and version")
	}
	key := child.Name + "@" + child.Version
	if _, ok := seen[key]; ok {
		return fmt.Errorf("duplicate allowed child contract %s", key)
	}
	seen[key] = struct{}{}
	return nil
}

// applySchemaHash verifies a supplied hash against the computed one and stores it.
func (s *ContractSchema) applySchemaHash() error {
	hash, err := s.ComputeSchemaHash()
	if err != nil {
		return err
	}
	if s.SchemaHash != "" && s.SchemaHash != hash {
		return fmt.Errorf("schema_hash mismatch: supplied %s, computed %s", s.SchemaHash, hash)
	}
	s.SchemaHash = hash
	return nil
}

// ComputeSchemaHash hashes the contract's semantic schema, excluding mutable descriptive metadata.
func (s *ContractSchema) ComputeSchemaHash() (string, error) {
	canonical := struct {
		Name            string                   `json:"name"`
		Version         string                   `json:"version"`
		Kind            ContractKind             `json:"kind"`
		Parent          *ContractRef             `json:"parent,omitempty"`
		AllowedChildren []ContractRef            `json:"allowed_children,omitempty"`
		Fields          map[string]ContractField `json:"fields"`
	}{s.Name, s.Version, s.Kind, s.Parent, s.AllowedChildren, s.Fields}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal contract schema: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ValidatePayload validates an incoming JSON payload against a registered contract schema.
func (s *ContractSchema) ValidatePayload(payload json.RawMessage) error {
	if s == nil {
		return fmt.Errorf("contract schema is required")
	}
	var value map[string]interface{}
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if value == nil {
		return fmt.Errorf("payload must be a JSON object")
	}
	return validateObject("$", value, s.Fields, 0)
}

func validateContractField(path string, f ContractField, depth int) error {
	if depth > 32 {
		return fmt.Errorf("%s exceeds maximum schema nesting depth", path)
	}
	if err := validateContractFieldType(path, f.Type); err != nil {
		return err
	}
	if f.Type == "object" {
		return validateContractProperties(path, f.Properties, depth)
	}
	if f.Type == "array" {
		return validateContractItems(path, f.Items, depth)
	}
	return nil
}

// validateContractFieldType rejects schema types the contract language does not define.
func validateContractFieldType(path, fieldType string) error {
	switch fieldType {
	case "string", "number", "integer", "boolean", "object", "array", "null":
	default:
		return fmt.Errorf("%s has unsupported type %q", path, fieldType)
	}
	return nil
}

// validateContractProperties validates each declared object property recursively.
func validateContractProperties(path string, properties map[string]ContractField, depth int) error {
	for name, child := range properties {
		if err := validateContractField(path+"."+name, child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// validateContractItems requires and validates an array's item schema.
func validateContractItems(path string, items *ContractField, depth int) error {
	if items == nil {
		return fmt.Errorf("%s array must define items", path)
	}
	return validateContractField(path+"[]", *items, depth+1)
}

func validateObject(path string, value map[string]interface{}, fields map[string]ContractField, depth int) error {
	if depth > 32 {
		return fmt.Errorf("%s exceeds maximum payload nesting depth", path)
	}
	for name, field := range fields {
		if err := validateObjectField(path, name, field, value, depth); err != nil {
			return err
		}
	}
	return validateUndeclaredFields(path, value, fields)
}

// validateObjectField checks one declared field against the payload object,
// rejecting a missing required member before validating its value.
func validateObjectField(path, name string, field ContractField, value map[string]interface{}, depth int) error {
	v, exists := value[name]
	if !exists {
		if field.Required {
			return fmt.Errorf("%s.%s is required", path, name)
		}
		return nil
	}
	return validateValue(path+"."+name, v, field, depth+1)
}

// validateUndeclaredFields rejects payload keys the contract does not declare.
func validateUndeclaredFields(path string, value map[string]interface{}, fields map[string]ContractField) error {
	for name := range value {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%s.%s is not declared by the contract", path, name)
		}
	}
	return nil
}

func validateValue(path string, value interface{}, field ContractField, depth int) error {
	if err := validateValueType(path, value, field, depth); err != nil {
		return err
	}
	return validateValueEnum(path, value, field)
}

// validateValueType checks the JSON type of one value against the declared
// contract type, recursing into object and array members.
func validateValueType(path string, value interface{}, field ContractField, depth int) error {
	switch field.Type {
	case "object":
		return validateObjectValue(path, value, field, depth)
	case "array":
		return validateArrayValue(path, value, field, depth)
	}
	if !matchesContractType(value, field.Type) {
		return fmt.Errorf("%s must be %s", path, field.Type)
	}
	return nil
}

// matchesContractType reports whether a scalar JSON value has the declared type.
func matchesContractType(value interface{}, fieldType string) bool {
	switch fieldType {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number", "integer":
		n, ok := value.(float64)
		return ok && (fieldType == "number" || math.Trunc(n) == n)
	case "null":
		return value == nil
	}
	return false
}

// validateObjectValue validates a nested object payload against its properties.
func validateObjectValue(path string, value interface{}, field ContractField, depth int) error {
	obj, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("%s must be %s", path, field.Type)
	}
	return validateObject(path, obj, field.Properties, depth+1)
}

// validateArrayValue validates each array member against the item schema.
func validateArrayValue(path string, value interface{}, field ContractField, depth int) error {
	items, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("%s must be %s", path, field.Type)
	}
	for i, item := range items {
		if err := validateValue(fmt.Sprintf("%s[%d]", path, i), item, *field.Items, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// validateValueEnum rejects values outside the field's declared enum.
func validateValueEnum(path string, value interface{}, field ContractField) error {
	if len(field.Enum) == 0 {
		return nil
	}
	for _, candidate := range field.Enum {
		if reflect.DeepEqual(candidate, value) {
			return nil
		}
	}
	return fmt.Errorf("%s must be one of [%s]", path, strings.Join(formatEnumValues(field.Enum), ", "))
}

// formatEnumValues renders enum candidates as sorted canonical JSON strings.
func formatEnumValues(enum []interface{}) []string {
	values := make([]string, 0, len(enum))
	for _, v := range enum {
		raw, _ := json.Marshal(v)
		values = append(values, string(raw))
	}
	sort.Strings(values)
	return values
}

// AllowsChild reports whether this parent contract explicitly permits a child contract identity.
func (s *ContractSchema) AllowsChild(child ContractRef) bool {
	if s == nil {
		return false
	}
	for _, allowed := range s.AllowedChildren {
		if allowed.Name == child.Name && allowed.Version == child.Version && (allowed.SchemaHash == "" || allowed.SchemaHash == child.SchemaHash) {
			return true
		}
	}
	return false
}
