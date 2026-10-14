// Package effects provides implementations for sending effects through NATS.
package effects

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const maxDomainServicePayloadBytes = 1 << 20

// DomainServiceSender publishes tenant service deliveries to each service's
// unique NATS subject. Each receiver owns a durable consumer for that subject.
type DomainServiceSender struct {
	services  ports.DomainServiceRepository
	publisher ports.BrokerPublisher
}

// NewDomainServiceSender creates a NATS-backed domain service sender.
func NewDomainServiceSender(services ports.DomainServiceRepository, publisher ports.BrokerPublisher) *DomainServiceSender {
	return &DomainServiceSender{services: services, publisher: publisher}
}

// Send publishes an event delivery after verifying that its tenant service is active.
func (s *DomainServiceSender) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if _, err := validateAndDecodeEffect(effect); err != nil {
		return err
	}
	if s.publisher == nil {
		return errors.New("send domain service event: NATS publisher is not configured")
	}
	service, err := s.loadActiveService(ctx, effect)
	if err != nil {
		return err
	}
	subject := domain.DomainServiceSubject(service.TenantID, service.ID)
	message := *effect
	message.IdempotencyKey = idempotencyKey
	payload, err := json.Marshal(&message)
	if err != nil {
		return fmt.Errorf("encode tenant service delivery: %w", err)
	}
	if err := s.publisher.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish event to tenant service subject %q: %w", subject, err)
	}
	return nil
}

func validateAndDecodeEffect(effect *domain.Effect) (*domain.EventEnvelope, error) {
	if effect == nil {
		return nil, errors.New("send domain service event: effect is nil")
	}
	if effect.TenantID == "" || effect.Destination == "" || effect.Name == "" {
		return nil, errors.New("send domain service event: tenant, service, and event type are required")
	}
	if effect.EffectType != domain.EffectTypeServiceEvent {
		return nil, fmt.Errorf("send domain service event: unsupported effect type %q", effect.EffectType)
	}
	if len(effect.Payload) == 0 || len(effect.Payload) > maxDomainServicePayloadBytes {
		return nil, fmt.Errorf("send domain service event: payload must be between 1 and %d bytes", maxDomainServicePayloadBytes)
	}
	var event domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &event); err != nil {
		return nil, fmt.Errorf("decode tenant service event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("validate tenant service event: %w", err)
	}
	if event.TenantID != effect.TenantID || event.Type != effect.Name {
		return nil, errors.New("send domain service event: event identity does not match outbox effect")
	}
	return &event, nil
}

func (s *DomainServiceSender) loadActiveService(ctx context.Context, effect *domain.Effect) (*domain.DomainService, error) {
	if s.services == nil {
		return nil, errors.New("send domain service event: service repository is not configured")
	}
	service, err := s.services.GetService(ctx, effect.TenantID, effect.Destination)
	if err != nil {
		return nil, fmt.Errorf("get subscribed tenant service: %w", err)
	}
	if service == nil || !service.Active {
		return nil, fmt.Errorf("subscribed tenant service %q is unavailable", effect.Destination)
	}
	return service, nil
}

var _ ports.EffectSender = (*DomainServiceSender)(nil)
