package effects

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type scheduledEventRepositoryStub struct {
	ports.ScheduledEventRepository
	saved *domain.ScheduledEvent
	err   error
}

func (r *scheduledEventRepositoryStub) Save(_ context.Context, event *domain.ScheduledEvent) error {
	eventCopy := *event
	r.saved = &eventCopy
	return r.err
}

func TestScheduledEventSenderPersistsPendingEvent(t *testing.T) {
	repository := &scheduledEventRepositoryStub{}
	sender := NewScheduledEventSender(repository)
	scheduled := domain.ScheduledEvent{
		EventID: "evt-reminder", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-7", Payload: json.RawMessage(`{"order_id":"order-7"}`),
		ScheduledAt: time.Now().UTC().Add(time.Minute),
	}
	payload, err := json.Marshal(scheduled)
	if err != nil {
		t.Fatal(err)
	}
	err = sender.Send(context.Background(), &domain.Effect{
		EffectType: domain.EffectTypeScheduleEvent, TenantID: "tenant-a", Payload: payload,
	}, "effect-id")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if repository.saved == nil {
		t.Fatal("scheduled event was not persisted")
	}
	if repository.saved.Status != domain.ScheduledEventStatusPending {
		t.Fatalf("status = %q, want pending", repository.saved.Status)
	}
	if repository.saved.CreatedAt.IsZero() {
		t.Fatal("CreatedAt must be set")
	}
}

func TestScheduledEventSenderRejectsTenantMismatch(t *testing.T) {
	repository := &scheduledEventRepositoryStub{}
	sender := NewScheduledEventSender(repository)
	payload, err := json.Marshal(domain.ScheduledEvent{
		EventID: "evt-reminder", TenantID: "tenant-b", EventType: "order.reminder",
		PartitionKey: "order-7", Payload: json.RawMessage(`{}`), ScheduledAt: time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.Send(context.Background(), &domain.Effect{
		EffectType: domain.EffectTypeScheduleEvent, TenantID: "tenant-a", Payload: payload,
	}, "effect-id")
	if err == nil {
		t.Fatal("Send() error = nil, want tenant mismatch")
	}
	if repository.saved != nil {
		t.Fatal("mismatched tenant event must not be persisted")
	}
}

func TestScheduledEventSenderReturnsRepositoryError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	repository := &scheduledEventRepositoryStub{err: wantErr}
	sender := NewScheduledEventSender(repository)
	payload, err := json.Marshal(domain.ScheduledEvent{
		EventID: "evt-reminder", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-7", Payload: json.RawMessage(`{}`), ScheduledAt: time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.Send(context.Background(), &domain.Effect{
		EffectType: domain.EffectTypeScheduleEvent, TenantID: "tenant-a", Payload: payload,
	}, "effect-id")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Send() error = %v, want %v", err, wantErr)
	}
}
