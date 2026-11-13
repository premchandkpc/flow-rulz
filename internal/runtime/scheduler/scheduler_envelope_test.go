package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type recordingEnvelopePublisher struct {
	event *domain.EventEnvelope
	err error
}

func (p *recordingEnvelopePublisher) PublishEvent(_ context.Context, event *domain.EventEnvelope) error {
	p.event = event
	return p.err
}

func TestEnvelopeSchedulerPreservesStoredHeaders(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	headers := map[string]string{"Idempotency-Key": "original", "flowrulz_correlation_id": "corr-1"}
	stored := &domain.ScheduledEvent{
		EventID: "evt-1", EventType: "order.reminder", TenantID: "tenant-a",
		PartitionKey: "order-1", Payload: []byte("{}"), Headers: headers,
	}
	publisher := &recordingEnvelopePublisher{}
	scheduler := NewEnvelopeScheduler(&schedulerTestRepo{}, publisher, schedulerTestClock{now: now}, time.Second, 10)
	if err := scheduler.releaseEvent(context.Background(), stored, "sched-evt-1"); err != nil {
		t.Fatal(err)
	}
	if publisher.event == nil || publisher.event.Headers["Idempotency-Key"] != "sched-evt-1" {
		t.Fatalf("unexpected published event: %+v", publisher.event)
	}
	if headers["Idempotency-Key"] != "original" || publisher.event.CorrelationID != "corr-1" {
		t.Fatalf("stored headers mutated or correlation lost: %+v", headers)
	}
}

func TestEnvelopeSchedulerPropagatesPublishError(t *testing.T) {
	want := errors.New("publisher unavailable")
	publisher := &recordingEnvelopePublisher{err: want}
	scheduler := NewEnvelopeScheduler(&schedulerTestRepo{}, publisher, schedulerTestClock{now: time.Now()}, time.Second, 10)
	stored := &domain.ScheduledEvent{
		EventID: "evt-1", EventType: "order.reminder", TenantID: "tenant-a",
		PartitionKey: "order-1", Payload: []byte("{}"),
	}
	if err := scheduler.releaseEvent(context.Background(), stored, "sched-evt-1"); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
