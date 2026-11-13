package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
)

// eventEnvelopeWire shares the envelope's field tags without its JSON methods.
type eventEnvelopeWire EventEnvelope

// MarshalJSON keeps runtime identity and extensible event metadata inside meta.
func (e EventEnvelope) MarshalJSON() ([]byte, error) {
	wire := eventEnvelopeWire(e)
	meta, err := e.canonicalMetadata()
	if err != nil {
		return nil, err
	}
	wire.Meta = meta
	return json.Marshal(wire)
}

func (e EventEnvelope) canonicalMetadata() (map[string]any, error) {
	meta := make(map[string]any, len(e.Meta)+2)
	maps.Copy(meta, e.Meta)
	tenantID, partitionKey, err := canonicalMetaIdentityPair(meta, e.TenantID, e.PartitionKey)
	if err != nil {
		return nil, err
	}
	meta["tenant_id"] = tenantID
	meta["partition_key"] = partitionKey
	return meta, nil
}

// canonicalMetaIdentityPair rejects conflicting tenant or partition identities.
func canonicalMetaIdentityPair(meta map[string]any, tenantID, partitionKey string) (string, string, error) {
	tenant, err := canonicalMetaIdentity(meta, "tenant_id", tenantID)
	if err != nil {
		return "", "", err
	}
	partition, err := canonicalMetaIdentity(meta, "partition_key", partitionKey)
	if err != nil {
		return "", "", err
	}
	return tenant, partition, nil
}

func canonicalMetaIdentity(meta map[string]any, key, fieldValue string) (string, error) {
	metaValue, present := meta[key]
	if !present {
		if strings.TrimSpace(fieldValue) == "" {
			return "", fmt.Errorf("meta.%s is required", key)
		}
		return fieldValue, nil
	}
	return validateMetaIdentity(metaValue, key, fieldValue)
}

func validateMetaIdentity(metaValue any, key, fieldValue string) (string, error) {
	value, ok := metaValue.(string)
	if !ok {
		return "", fmt.Errorf("meta.%s must be a string", key)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("meta.%s must be a non-empty string", key)
	}
	if fieldValue != "" && fieldValue != value {
		return "", fmt.Errorf("event field and meta.%s disagree", key)
	}
	return value, nil
}

// UnmarshalJSON accepts runtime identity only inside meta.
func (e *EventEnvelope) UnmarshalJSON(data []byte) error {
	wire, err := decodeEventEnvelope(data)
	if err != nil {
		return err
	}
	tenantID, partitionKey, err := canonicalMetaIdentityPair(wire.Meta, "", "")
	if err != nil {
		return err
	}
	wire.TenantID, wire.PartitionKey = tenantID, partitionKey
	*e = EventEnvelope(wire)
	return nil
}

func decodeEventEnvelope(data []byte) (eventEnvelopeWire, error) {
	var wire eventEnvelopeWire
	if err := validateEnvelopeFields(data); err != nil {
		return wire, err
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return wire, err
	}
	if wire.Meta == nil {
		return wire, fmt.Errorf("meta is required")
	}
	return wire, nil
}

func validateEnvelopeFields(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		switch key {
		case "id", "type", "occurred_at", "workflow_id", "data", "contract", "message_kind", "correlation_id", "causation_id", "headers", "meta":
		default:
			return fmt.Errorf("unknown event envelope field %q; runtime metadata belongs under meta", key)
		}
	}
	return nil
}
