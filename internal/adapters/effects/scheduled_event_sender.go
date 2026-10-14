// Package effects provides infrastructure adapters for effect delivery.
package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// ScheduledEventSender persists scheduled-event intents produced by the transactional outbox.
// The stable event ID makes retries safe when persistence succeeds but outbox settlement fails.
type ScheduledEventSender struct {
	repository ports.ScheduledEventRepository
}

// NewScheduledEventSender creates a sender backed by the worker's scheduled-event repository.
func NewScheduledEventSender(repository ports.ScheduledEventRepository) *ScheduledEventSender {
	return &ScheduledEventSender{repository: repository}
}

// Send persists one scheduled-event intent. Repeated sends are safe because repository Save
// ignores duplicate event IDs, allowing the outbox to retry after a crash.
func (s *ScheduledEventSender) Send(ctx context.Context, effect *domain.Effect, _ string) error {
	if effect == nil {
		return fmt.Errorf("persist scheduled event: effect is nil")
	}
	if effect.EffectType != domain.EffectTypeScheduleEvent {
		return fmt.Errorf("persist scheduled event: unsupported effect type %q", effect.EffectType)
	}
	if s.repository == nil {
		return fmt.Errorf("persist scheduled event: repository is not configured")
	}

	var event domain.ScheduledEvent
	if err := json.Unmarshal(effect.Payload, &event); err != nil {
		return fmt.Errorf("decode scheduled event intent: %w", err)
	}
	if event.EventID == "" || event.TenantID == "" || event.EventType == "" || event.PartitionKey == "" || event.ScheduledAt.IsZero() {
		return fmt.Errorf("scheduled event intent is missing event ID, tenant, type, partition key, or scheduled time")
	}
	if effect.TenantID != "" && effect.TenantID != event.TenantID {
		return fmt.Errorf("scheduled event tenant %q does not match outbox tenant %q", event.TenantID, effect.TenantID)
	}
	if !json.Valid(event.Payload) {
		return fmt.Errorf("scheduled event payload must be valid JSON")
	}
	event.Status = domain.ScheduledEventStatusPending
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if event.Headers == nil {
		event.Headers = make(map[string]string)
	}
	return s.repository.Save(ctx, &event)
}

var _ ports.EffectSender = (*ScheduledEventSender)(nil)
