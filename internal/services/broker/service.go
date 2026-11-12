// Package broker applies shared publication, authentication and settlement policy.
package broker

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	runtimebroker "github.com/flowrulz/flow-rulz/internal/runtime/broker"
)

// Service implements transport-independent broker operations for trusted clients.
type Service struct {
	broker *runtimebroker.Broker
	token  string
}

// New constructs a service around one broker instance and its shared client token.
func New(b *runtimebroker.Broker, token string) *Service { return &Service{broker: b, token: token} }

// Execute validates one request and executes its operation.
func (s *Service) Execute(ctx context.Context, request ports.BrokerRequest) ports.BrokerResponse {
	response := ports.BrokerResponse{Version: ports.BrokerProtocolVersion, ID: request.ID}
	if request.Version != ports.BrokerProtocolVersion || len(request.ID) > 128 {
		response.ID = ""
		response.Code = "invalid"
		return response
	}
	if s.token != "" && subtle.ConstantTimeCompare([]byte(request.Token), []byte(s.token)) != 1 {
		response.Code = "unauthorized"
		return response
	}
	if len(request.TenantID) == 0 || len(request.TenantID) > 128 || strings.TrimSpace(request.TenantID) != request.TenantID {
		response.Code = "invalid"
		return response
	}
	if ctx.Err() != nil {
		response.Code = "unavailable"
		return response
	}
	var data any
	var err error
	switch request.Operation {
	case "publish":
		data, err = s.publish(request.TenantID, request.Event)
	case "append":
		data, err = s.append(request)
	case "consume":
		if request.WaitMS < 0 || request.WaitMS > 20000 {
			err = runtimebroker.ErrInvalid
			break
		}
		var claim *runtimebroker.Claim
		claim, err = s.broker.Next(ctx, request.TenantID, request.Topic, request.Group, time.Duration(request.WaitMS)*time.Millisecond)
		if err != nil {
			break
		}
		if claim == nil {
			response.Code = "empty"
			return response
		}
		data = claim
		response.Code = "message"
	case "ack", "retry":
		if request.DelayMS < 0 || request.DelayMS > 300000 {
			err = runtimebroker.ErrInvalid
			break
		}
		err = s.broker.Settle(request.TenantID, request.Topic, request.Group, request.Partition, request.Receipt, request.Operation == "retry", time.Duration(request.DelayMS)*time.Millisecond)
		response.Code = "ok"
	default:
		err = runtimebroker.ErrInvalid
	}
	if err != nil {
		response.Code = resultCode(err)
		return response
	}
	if response.Code == "" {
		response.Code = "accepted"
	}
	if data != nil {
		response.Data, err = json.Marshal(data)
		if err != nil {
			response.Code = "unavailable"
			response.Data = nil
		}
	}
	return response
}

var errTenant = errors.New("tenant mismatch")

func resultCode(err error) string {
	switch {
	case errors.Is(err, errTenant):
		return "forbidden"
	case errors.Is(err, runtimebroker.ErrInvalid):
		return "invalid"
	case errors.Is(err, runtimebroker.ErrReceipt):
		return "conflict"
	default:
		return "unavailable"
	}
}

func (s *Service) publish(tenant string, event *domain.EventEnvelope) (any, error) {
	if event == nil {
		return nil, runtimebroker.ErrInvalid
	}
	if event.TenantID != tenant {
		return nil, errTenant
	}
	if event.Contract != nil {
		return nil, runtimebroker.ErrInvalid
	}
	if err := event.Validate(); err != nil {
		return nil, runtimebroker.ErrInvalid
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	record, err := s.broker.PublishInput(tenant, event.PartitionKey, payload)
	if err != nil {
		return nil, err
	}
	return map[string]any{"topic": record.Topic, "partition": record.Partition, "offset": record.Offset, "event_id": event.ID}, nil
}

func (s *Service) append(request ports.BrokerRequest) (any, error) {
	if request.Topic == "events" {
		var event domain.EventEnvelope
		if err := json.Unmarshal(request.Payload, &event); err != nil {
			return nil, runtimebroker.ErrInvalid
		}
		if request.Key != "" && request.Key != event.PartitionKey {
			return nil, runtimebroker.ErrInvalid
		}
		return s.publish(request.TenantID, &event)
	}
	record, err := s.broker.Publish(request.Topic, request.TenantID, request.Key, request.Payload)
	if err != nil {
		return nil, err
	}
	return map[string]any{"topic": record.Topic, "partition": record.Partition, "offset": record.Offset}, nil
}

var _ ports.BrokerProtocol = (*Service)(nil)
