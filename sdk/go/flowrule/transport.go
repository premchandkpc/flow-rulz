package flowrulz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Transport defines the interface for sending events
type Transport interface {
	// Send sends an event envelope
	Send(ctx context.Context, env *Envelope) error
	// SendBatch sends multiple events atomically if possible
	SendBatch(ctx context.Context, envs []*Envelope) error
	// Close closes the transport
	Close() error
	// Name returns the transport name for logging
	Name() string
}

// Envelope wraps a stable event envelope with extensible runtime metadata.
type Envelope struct {
	ID            string                     `json:"-"`
	Type          string                     `json:"-"`
	TenantID      string                     `json:"-"`
	PartitionKey  string                     `json:"-"`
	WorkflowID    string                     `json:"-"`
	CorrelationID string                     `json:"-"`
	CausationID   string                     `json:"-"`
	OccurredAt    time.Time                  `json:"-"`
	Data          json.RawMessage            `json:"-"`
	Contract      *ContractRef               `json:"-"`
	MessageKind   string                     `json:"-"`
	Headers       map[string]string          `json:"-"`
	SchemaVersion string                     `json:"-"`
	Meta          map[string]json.RawMessage `json:"-"`
}

type envelopeWire struct {
	ID         string                     `json:"id"`
	Type       string                     `json:"type"`
	OccurredAt time.Time                  `json:"occurred_at"`
	Data       json.RawMessage            `json:"data"`
	Contract   *ContractRef               `json:"contract,omitempty"`
	Meta       map[string]json.RawMessage `json:"meta"`
}

// MarshalJSON serializes the envelope with runtime metadata nested under meta.
func (e Envelope) MarshalJSON() ([]byte, error) {
	meta := make(map[string]json.RawMessage, len(e.Meta)+8)
	for key, value := range e.Meta {
		meta[key] = append(json.RawMessage(nil), value...)
	}
	for key, value := range map[string]string{
		"tenant_id": e.TenantID, "partition_key": e.PartitionKey, "workflow_id": e.WorkflowID,
		"correlation_id": e.CorrelationID, "causation_id": e.CausationID,
		"message_kind": e.MessageKind, "schema_version": e.SchemaVersion,
	} {
		if value == "" {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		meta[key] = encoded
	}
	if len(e.Headers) > 0 {
		encoded, err := json.Marshal(e.Headers)
		if err != nil {
			return nil, err
		}
		meta["headers"] = encoded
	}
	return json.Marshal(envelopeWire{ID: e.ID, Type: e.Type, OccurredAt: e.OccurredAt, Data: e.Data, Contract: e.Contract, Meta: meta})
}

// UnmarshalJSON decodes the envelope and preserves unknown metadata fields.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return err
	}
	allowed := map[string]bool{"id": true, "type": true, "occurred_at": true, "data": true, "contract": true, "meta": true}
	for key := range top {
		if !allowed[key] {
			return fmt.Errorf("unknown event envelope field %q; put runtime metadata inside meta", key)
		}
	}
	var wire envelopeWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Meta == nil {
		return fmt.Errorf("meta is required")
	}
	*e = Envelope{ID: wire.ID, Type: wire.Type, OccurredAt: wire.OccurredAt, Data: wire.Data, Contract: wire.Contract, Meta: wire.Meta}
	readString := func(key string, target *string) error {
		raw, ok := e.Meta[key]
		if !ok {
			return nil
		}
		if err := json.Unmarshal(raw, target); err != nil {
			return fmt.Errorf("meta.%s must be a string: %w", key, err)
		}
		delete(e.Meta, key)
		return nil
	}
	for key, target := range map[string]*string{
		"tenant_id": &e.TenantID, "partition_key": &e.PartitionKey, "workflow_id": &e.WorkflowID,
		"correlation_id": &e.CorrelationID, "causation_id": &e.CausationID, "message_kind": &e.MessageKind, "schema_version": &e.SchemaVersion,
	} {
		if err := readString(key, target); err != nil {
			return err
		}
	}
	if raw, ok := e.Meta["headers"]; ok {
		if err := json.Unmarshal(raw, &e.Headers); err != nil {
			return fmt.Errorf("meta.headers must be an object of strings: %w", err)
		}
		delete(e.Meta, "headers")
	}
	return nil
}

// ContractRef identifies a contract by name, version, and schema hash.
type ContractRef struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	SchemaHash string `json:"schema_hash,omitempty"`
}

// EnvelopeOption configures an envelope
type EnvelopeOption func(*Envelope)

// WithEnvelopePartitionKey sets the partition key explicitly
func WithEnvelopePartitionKey(key string) EnvelopeOption {
	return func(e *Envelope) {
		e.PartitionKey = key
	}
}

// WithEnvelopeWorkflowID sets the workflow ID
func WithEnvelopeWorkflowID(id string) EnvelopeOption {
	return func(e *Envelope) {
		e.WorkflowID = id
	}
}

// WithEnvelopeCorrelationID sets the correlation ID
func WithEnvelopeCorrelationID(id string) EnvelopeOption {
	return func(e *Envelope) {
		e.CorrelationID = id
	}
}

// WithEnvelopeCausationID sets the causation ID
func WithEnvelopeCausationID(id string) EnvelopeOption {
	return func(e *Envelope) {
		e.CausationID = id
	}
}

// WithEnvelopeHeaders adds headers to the envelope
func WithEnvelopeHeaders(headers map[string]string) EnvelopeOption {
	return func(e *Envelope) {
		if e.Headers == nil {
			e.Headers = make(map[string]string)
		}
		for k, v := range headers {
			e.Headers[k] = v
		}
	}
}

// WithEnvelopeSchemaVersion sets the schema version
func WithEnvelopeSchemaVersion(version string) EnvelopeOption {
	return func(e *Envelope) {
		e.SchemaVersion = version
	}
}

// WithEnvelopeOccurredAt sets the occurred at timestamp
func WithEnvelopeOccurredAt(t time.Time) EnvelopeOption {
	return func(e *Envelope) {
		e.OccurredAt = t
	}
}

// WithEnvelopeContract sets the contract reference on the envelope
func WithEnvelopeContract(name, version, schemaHash string) EnvelopeOption {
	return func(e *Envelope) {
		e.Contract = &ContractRef{Name: name, Version: version, SchemaHash: schemaHash}
	}
}

// WithEnvelopeMessageKind sets the message kind (e.g., "event", "command")
func WithEnvelopeMessageKind(kind string) EnvelopeOption {
	return func(e *Envelope) {
		e.MessageKind = kind
	}
}

// TransportConfig holds transport-specific configuration
type TransportConfig struct {
	Config *Config
}

// NewTransport creates a transport from config
func NewTransport(cfg *Config) (Transport, error) {
	tcfg := &TransportConfig{Config: cfg}
	switch cfg.Transport {
	case TransportNATS:
		return newNATSTransport(tcfg)
	case TransportGRPC:
		return newGRPCTransport(tcfg)
	case TransportHTTP:
		return nil, ErrConfig("HTTP event ingestion is not supported by the control-plane API; use the NATS transport")
	case TransportLocal:
		return newLocalTransport(tcfg)
	default:
		return nil, ErrConfig("unknown transport: " + string(cfg.Transport))
	}
}
