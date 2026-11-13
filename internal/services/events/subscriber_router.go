package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const maxSubscriberFanout = 100

// SubscriberRouter resolves matching tenant subscriptions into durable delivery
// intents. It contains routing policy only; it never evaluates rules or sends
// network messages. Call it with the transaction-scoped repository so routing
// intents commit atomically with the event execution and outbox.
type SubscriberRouter struct{}

// Route creates one stable service_event outbox effect for each active
// subscriber matching the event type. The dispatcher publishes these effects
// after the transaction commits.
func (SubscriberRouter) Route(ctx context.Context, repository ports.SubscriberRepository, envelope *domain.EventEnvelope, executionID string, now time.Time) ([]domain.OutboxEffect, error) {
	subscribers, err := eventSubscribers(ctx, repository, envelope)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal subscribed event envelope: %w", err)
	}
	return subscriberEffects(subscribers, envelope, executionID, payload, now), nil
}

func eventSubscribers(ctx context.Context, repository ports.SubscriberRepository, envelope *domain.EventEnvelope) ([]*domain.DomainService, error) {
	if repository == nil {
		return nil, fmt.Errorf("subscription repository is not configured")
	}
	subscribers, err := repository.ListSubscribersForEvent(ctx, envelope.TenantID, envelope.Type)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for event: %w", err)
	}
	if len(subscribers) > maxSubscriberFanout {
		return nil, fmt.Errorf("event %q exceeds the %d-service fan-out limit", envelope.Type, maxSubscriberFanout)
	}
	return subscribers, nil
}

func subscriberEffects(subscribers []*domain.DomainService, envelope *domain.EventEnvelope, executionID string, payload json.RawMessage, now time.Time) []domain.OutboxEffect {
	effects := make([]domain.OutboxEffect, 0, len(subscribers))
	for _, subscriber := range subscribers {
		if acceptsSubscriber(subscriber, envelope.TenantID) {
			effects = append(effects, newSubscriberEffect(subscriber.ID, envelope, executionID, payload, now))
		}
	}
	return effects
}

// Global subscriptions accept matching events across tenants. Legacy scoped
// subscriptions accept only their own tenant's events.
func acceptsSubscriber(subscriber *domain.DomainService, tenantID string) bool {
	return subscriber != nil && subscriber.Active &&
		(subscriber.TenantID == tenantID || subscriber.TenantID == domain.DomainConfigurationScope)
}

func newSubscriberEffect(subscriberID string, envelope *domain.EventEnvelope, executionID string, payload json.RawMessage, now time.Time) domain.OutboxEffect {
	return domain.OutboxEffect{
		ID:          domain.ComputeEffectID(envelope.TenantID, envelope.ID, "service-subscription", 1, subscriberID, 0),
		ExecutionID: executionID, TenantID: envelope.TenantID,
		Destination: subscriberID,
		Name:        envelope.Type,
		Payload:     payload,
		EffectType:  domain.EffectTypeServiceEvent,
		Status:      domain.OutboxStatusPending,
		MaxAttempts: 5,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
