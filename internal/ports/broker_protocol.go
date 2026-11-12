package ports

import (
	"context"
	"encoding/json"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// BrokerProtocolVersion identifies the shared broker request/response contract.
const BrokerProtocolVersion = 1

// BrokerRequest describes an operation independently of its network transport.
type BrokerRequest struct {
	Version   int                   `json:"version"`
	ID        string                `json:"id,omitempty"`
	Operation string                `json:"operation"`
	TenantID  string                `json:"tenant_id"`
	Token     string                `json:"token,omitempty"`
	Event     *domain.EventEnvelope `json:"event,omitempty"`
	Topic     string                `json:"topic,omitempty"`
	Key       string                `json:"key,omitempty"`
	Payload   json.RawMessage       `json:"payload,omitempty"`
	Group     string                `json:"group,omitempty"`
	Partition uint32                `json:"partition,omitempty"`
	Receipt   string                `json:"receipt,omitempty"`
	WaitMS    int                   `json:"wait_ms,omitempty"`
	DelayMS   int                   `json:"delay_ms,omitempty"`
}

// BrokerResponse returns a stable result code and optional operation data.
type BrokerResponse struct {
	Version int             `json:"version"`
	ID      string          `json:"id,omitempty"`
	Code    string          `json:"code"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// BrokerProtocol executes the same broker operations for every enabled transport.
type BrokerProtocol interface {
	Execute(context.Context, BrokerRequest) BrokerResponse
}

// BrokerCaller is the request/response peer contract used by remote publishers.
type BrokerCaller interface {
	Call(context.Context, BrokerRequest) (BrokerResponse, error)
	Close() error
}
