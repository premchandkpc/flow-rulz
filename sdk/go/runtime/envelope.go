package runtime

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Envelope wraps a typed payload with metadata for event processing
type Envelope struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	TenantID     string            `json:"tenant_id"`
	PartitionKey string            `json:"partition_key"`
	WorkflowID   string            `json:"workflow_id,omitempty"`
	OccurredAt   time.Time         `json:"occurred_at"`
	Data         json.RawMessage   `json:"data"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// NewEnvelope creates a new envelope with a generated ID and current timestamp
func NewEnvelope(eventType, tenantID, partitionKey string, payload any) (*Envelope, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Envelope{
		ID:           uuid.New().String(),
		Type:         eventType,
		TenantID:     tenantID,
		PartitionKey: partitionKey,
		OccurredAt:   time.Now().UTC(),
		Data:         data,
	}, nil
}

// Payload unmarshals the envelope data into the provided pointer
func (e *Envelope) Payload(v any) error {
	return json.Unmarshal(e.Data, v)
}

// VirtualShard computes the virtual shard for this envelope
func (e *Envelope) VirtualShard(numShards uint32) uint32 {
	return ComputeShard(e.TenantID, e.PartitionKey, numShards)
}

// Marshal serializes the envelope to JSON
func (e *Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// ComputeShard computes a deterministic shard from tenant and partition key
func ComputeShard(tenantID, partitionKey string, numShards uint32) uint32 {
	key := tenantID + ":" + partitionKey
	h := fnv32a(key)
	return h % numShards
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
