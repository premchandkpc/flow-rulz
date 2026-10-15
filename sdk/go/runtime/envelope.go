package runtime

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope wraps a typed payload with metadata for event processing
type Envelope struct {
	ID           string                     `json:"id"`
	Type         string                     `json:"type"`
	TenantID     string                     `json:"-"`
	PartitionKey string                     `json:"-"`
	WorkflowID   string                     `json:"-"`
	OccurredAt   time.Time                  `json:"occurred_at"`
	Data         json.RawMessage            `json:"data"`
	Headers      map[string]string          `json:"-"`
	Meta         map[string]json.RawMessage `json:"-"`
}

type envelopeWire struct {
	ID string `json:"id"`
	Type string `json:"type"`
	OccurredAt time.Time `json:"occurred_at"`
	Data json.RawMessage `json:"data"`
	Meta map[string]json.RawMessage `json:"meta"`
	TenantID string `json:"tenant_id,omitempty"`
	PartitionKey string `json:"partition_key,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (e Envelope) MarshalJSON() ([]byte, error) {
	meta := cloneMeta(e.Meta)
	setMeta(meta, "tenant_id", e.TenantID)
	setMeta(meta, "partition_key", e.PartitionKey)
	setMeta(meta, "workflow_id", e.WorkflowID)
	if len(e.Headers) > 0 { meta["headers"], _ = json.Marshal(e.Headers) }
	return json.Marshal(struct {
		ID string `json:"id"`; Type string `json:"type"`; OccurredAt time.Time `json:"occurred_at"`
		Data json.RawMessage `json:"data"`; Meta map[string]json.RawMessage `json:"meta"`
	}{e.ID,e.Type,e.OccurredAt,e.Data,meta})
}

func (e *Envelope) UnmarshalJSON(data []byte) error {
	var wire envelopeWire
	if err := json.Unmarshal(data,&wire); err != nil { return err }
	e.ID,e.Type,e.OccurredAt,e.Data,e.Meta=wire.ID,wire.Type,wire.OccurredAt,wire.Data,cloneMeta(wire.Meta)
	e.TenantID=metaString(e.Meta,"tenant_id"); if e.TenantID=="" { e.TenantID=wire.TenantID }
	e.PartitionKey=metaString(e.Meta,"partition_key"); if e.PartitionKey=="" { e.PartitionKey=wire.PartitionKey }
	e.WorkflowID=metaString(e.Meta,"workflow_id"); if e.WorkflowID=="" { e.WorkflowID=wire.WorkflowID }
	if raw:=e.Meta["headers"];len(raw)>0 { if err:=json.Unmarshal(raw,&e.Headers);err!=nil{return fmt.Errorf("meta.headers: %w",err)} }
	if e.Headers==nil { e.Headers=wire.Headers }
	return nil
}

func cloneMeta(input map[string]json.RawMessage) map[string]json.RawMessage {
	out:=make(map[string]json.RawMessage,len(input)+4)
	for k,v:=range input { out[k]=append(json.RawMessage(nil),v...) }
	return out
}
func setMeta(meta map[string]json.RawMessage,key,value string) {
	if value=="" { return }; encoded,_:=json.Marshal(value); meta[key]=encoded
}
func metaString(meta map[string]json.RawMessage,key string) string {
	var value string; _=json.Unmarshal(meta[key],&value); return value
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
