package runtime

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope is a stable event envelope. Runtime identity and extensible metadata
// are serialized inside Meta, never as top-level event fields.
type Envelope struct {
	ID           string                     `json:"-"`
	Type         string                     `json:"-"`
	TenantID     string                     `json:"-"`
	PartitionKey string                     `json:"-"`
	WorkflowID   string                     `json:"-"`
	OccurredAt   time.Time                  `json:"-"`
	Data         json.RawMessage            `json:"-"`
	Headers      map[string]string          `json:"-"`
	Meta         map[string]json.RawMessage `json:"-"`
}

type envelopeWire struct {
	ID         string                     `json:"id"`
	Type       string                     `json:"type"`
	OccurredAt time.Time                  `json:"occurred_at"`
	Data       json.RawMessage            `json:"data"`
	Meta       map[string]json.RawMessage `json:"meta"`
}

func (e Envelope) MarshalJSON() ([]byte, error) {
	meta := make(map[string]json.RawMessage, len(e.Meta)+4)
	for key, value := range e.Meta {
		meta[key] = append(json.RawMessage(nil), value...)
	}
	for key, value := range map[string]string{
		"tenant_id": e.TenantID, "partition_key": e.PartitionKey, "workflow_id": e.WorkflowID,
	} {
		if value == "" { continue }
		encoded, err := json.Marshal(value)
		if err != nil { return nil, err }
		meta[key] = encoded
	}
	if len(e.Headers) > 0 {
		encoded, err := json.Marshal(e.Headers)
		if err != nil { return nil, err }
		meta["headers"] = encoded
	}
	return json.Marshal(envelopeWire{ID:e.ID, Type:e.Type, OccurredAt:e.OccurredAt, Data:e.Data, Meta:meta})
}

func (e *Envelope) UnmarshalJSON(data []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil { return err }
	allowed := map[string]bool{"id":true, "type":true, "occurred_at":true, "data":true, "meta":true}
	for key := range top {
		if !allowed[key] { return fmt.Errorf("unknown event envelope field %q; put runtime metadata inside meta", key) }
	}
	var wire envelopeWire
	if err := json.Unmarshal(data, &wire); err != nil { return err }
	if wire.Meta == nil { return fmt.Errorf("meta is required") }
	*e = Envelope{ID:wire.ID, Type:wire.Type, OccurredAt:wire.OccurredAt, Data:wire.Data, Meta:wire.Meta}
	readString := func(key string, target *string) error {
		raw, ok := e.Meta[key]
		if !ok { return nil }
		if err := json.Unmarshal(raw, target); err != nil { return fmt.Errorf("meta.%s must be a string: %w", key, err) }
		delete(e.Meta, key)
		return nil
	}
	for key, target := range map[string]*string{
		"tenant_id": &e.TenantID, "partition_key": &e.PartitionKey, "workflow_id": &e.WorkflowID,
	} {
		if err := readString(key, target); err != nil { return err }
	}
	if raw, ok := e.Meta["headers"]; ok {
		if err := json.Unmarshal(raw, &e.Headers); err != nil { return fmt.Errorf("meta.headers must be an object of strings: %w", err) }
		delete(e.Meta, "headers")
	}
	return nil
}

// NewEnvelope creates a new envelope with a generated ID and current timestamp.
func NewEnvelope(eventType, tenantID, partitionKey string, payload any) (*Envelope, error) {
	data, err := json.Marshal(payload)
	if err != nil { return nil, err }
	return &Envelope{
		ID: uuid.New().String(), Type: eventType, TenantID: tenantID,
		PartitionKey: partitionKey, OccurredAt: time.Now().UTC(), Data: data,
	}, nil
}

// Payload unmarshals the envelope data into the provided pointer.
func (e *Envelope) Payload(v any) error { return json.Unmarshal(e.Data, v) }

// VirtualShard computes the virtual shard for this envelope.
func (e *Envelope) VirtualShard(numShards uint32) uint32 {
	return ComputeShard(e.TenantID, e.PartitionKey, numShards)
}

// Marshal serializes the envelope to JSON.
func (e *Envelope) Marshal() ([]byte, error) { return json.Marshal(e) }

// ComputeShard computes a deterministic shard from tenant and partition key.
func ComputeShard(tenantID, partitionKey string, numShards uint32) uint32 {
	key := tenantID + ":" + partitionKey
	h := fnv32a(key)
	return h % numShards
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ { h ^= uint32(s[i]); h *= 16777619 }
	return h
}
