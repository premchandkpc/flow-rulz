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

// Envelope wraps a typed payload with metadata
type Envelope struct {
	ID            string                     `json:"id"`
	Type          string                     `json:"type"`
	TenantID      string                     `json:"-"`
	PartitionKey  string                     `json:"-"`
	WorkflowID    string                     `json:"-"`
	CorrelationID string                     `json:"-"`
	CausationID   string                     `json:"-"`
	OccurredAt    time.Time                  `json:"occurred_at"`
	Data          json.RawMessage            `json:"data"`
	Contract      *ContractRef               `json:"contract,omitempty"`
	MessageKind   string                     `json:"-"`
	Headers       map[string]string          `json:"-"`
	SchemaVersion string                     `json:"-"`
	Meta          map[string]json.RawMessage `json:"-"`
}

type envelopeWire struct {
	ID string `json:"id"`
	Type string `json:"type"`
	OccurredAt time.Time `json:"occurred_at"`
	Data json.RawMessage `json:"data"`
	Contract *ContractRef `json:"contract,omitempty"`
	Meta map[string]json.RawMessage `json:"meta"`
	TenantID string `json:"tenant_id,omitempty"`
	PartitionKey string `json:"partition_key,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID string `json:"causation_id,omitempty"`
	MessageKind string `json:"message_kind,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

func (e Envelope) MarshalJSON() ([]byte, error) {
	meta := cloneEnvelopeMeta(e.Meta)
	setEnvelopeMeta(meta,"tenant_id",e.TenantID)
	setEnvelopeMeta(meta,"partition_key",e.PartitionKey)
	setEnvelopeMeta(meta,"workflow_id",e.WorkflowID)
	setEnvelopeMeta(meta,"correlation_id",e.CorrelationID)
	setEnvelopeMeta(meta,"causation_id",e.CausationID)
	setEnvelopeMeta(meta,"message_kind",e.MessageKind)
	setEnvelopeMeta(meta,"schema_version",e.SchemaVersion)
	if len(e.Headers)>0 { meta["headers"],_=json.Marshal(e.Headers) }
	return json.Marshal(struct {
		ID string `json:"id"`; Type string `json:"type"`; OccurredAt time.Time `json:"occurred_at"`
		Data json.RawMessage `json:"data"`; Contract *ContractRef `json:"contract,omitempty"`; Meta map[string]json.RawMessage `json:"meta"`
	}{e.ID,e.Type,e.OccurredAt,e.Data,e.Contract,meta})
}

func (e *Envelope) UnmarshalJSON(data []byte) error {
	var wire envelopeWire
	if err:=json.Unmarshal(data,&wire);err!=nil{return err}
	e.ID,e.Type,e.OccurredAt,e.Data,e.Contract,e.Meta=wire.ID,wire.Type,wire.OccurredAt,wire.Data,wire.Contract,cloneEnvelopeMeta(wire.Meta)
	e.TenantID=readEnvelopeMeta(e.Meta,"tenant_id");if e.TenantID==""{e.TenantID=wire.TenantID}
	e.PartitionKey=readEnvelopeMeta(e.Meta,"partition_key");if e.PartitionKey==""{e.PartitionKey=wire.PartitionKey}
	e.WorkflowID=readEnvelopeMeta(e.Meta,"workflow_id");if e.WorkflowID==""{e.WorkflowID=wire.WorkflowID}
	e.CorrelationID=readEnvelopeMeta(e.Meta,"correlation_id");if e.CorrelationID==""{e.CorrelationID=wire.CorrelationID}
	e.CausationID=readEnvelopeMeta(e.Meta,"causation_id");if e.CausationID==""{e.CausationID=wire.CausationID}
	e.MessageKind=readEnvelopeMeta(e.Meta,"message_kind");if e.MessageKind==""{e.MessageKind=wire.MessageKind}
	e.SchemaVersion=readEnvelopeMeta(e.Meta,"schema_version");if e.SchemaVersion==""{e.SchemaVersion=wire.SchemaVersion}
	if raw:=e.Meta["headers"];len(raw)>0{if err:=json.Unmarshal(raw,&e.Headers);err!=nil{return fmt.Errorf("meta.headers: %w",err)}}
	if e.Headers==nil{e.Headers=wire.Headers}
	return nil
}
func cloneEnvelopeMeta(input map[string]json.RawMessage) map[string]json.RawMessage {
	out:=make(map[string]json.RawMessage,len(input)+8)
	for k,v:=range input{out[k]=append(json.RawMessage(nil),v...)}
	return out
}
func setEnvelopeMeta(meta map[string]json.RawMessage,key,value string){if value!=""{encoded,_:=json.Marshal(value);meta[key]=encoded}}
func readEnvelopeMeta(meta map[string]json.RawMessage,key string)string{var value string;_=json.Unmarshal(meta[key],&value);return value}

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
