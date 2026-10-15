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
func (SubscriberRouter) Route(
	ctx context.Context,
	repository ports.SubscriberRepository,
	envelope *domain.EventEnvelope,
	executionID string,
	now time.Time,
) ([]domain.OutboxEffect, error) {
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

	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal subscribed event envelope: %w", err)
	}

	effects := make([]domain.OutboxEffect, 0, len(subscribers))
	for _, subscriber := range subscribers {
		if subscriber == nil || !subscriber.Active {
			continue
		}
		// Global subscriptions are configured without tenant identity and may
		// receive matching events from any tenant. Legacy scoped subscriptions
		// remain isolated to the event tenant during migration.
		if subscriber.TenantID != envelope.TenantID && subscriber.TenantID != domain.DomainConfigurationScope {
			continue
		}
		effects = append(effects, domain.OutboxEffect{
			ID:          domain.ComputeEffectID(envelope.TenantID, envelope.ID, "service-subscription", 1, subscriber.ID, 0),
			ExecutionID: executionID,
			TenantID:    envelope.TenantID,
			Destination: subscriber.ID,
			Name:        envelope.Type,
			Payload:     payload,
			EffectType:  domain.EffectTypeServiceEvent,
			Status:      domain.OutboxStatusPending,
			MaxAttempts: 5,
			AvailableAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}
	return effects, nil
}
