package flowrulz

import (
	"context"
	"encoding/json"
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

// Envelope wraps a typed payload with metadata
type Envelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	TenantID      string            `json:"tenant_id"`
	PartitionKey  string            `json:"partition_key"`
	WorkflowID    string            `json:"workflow_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
	Data          json.RawMessage   `json:"data"`
	Headers       map[string]string `json:"headers,omitempty"`
	SchemaVersion string            `json:"schema_version,omitempty"`
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
		return newHTTPTransport(tcfg)
	case TransportLocal:
		return newLocalTransport(tcfg)
	default:
		return nil, ErrConfig("unknown transport: " + string(cfg.Transport))
	}
}
