package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type schedulerTestClock struct{ now time.Time }

func (c schedulerTestClock) Now() time.Time { return c.now }

type schedulerTestRepo struct {
	event       *domain.ScheduledEvent
	claimant    string
	releasedBy  string
	failedBy    string
	failedMsg   string
	claimErr    error
	releasedErr error
	failedErr   error
	retryBy     string
	retryAt     time.Time
	retryMsg    string
	retryErr    error
}

func (r *schedulerTestRepo) Save(context.Context, *domain.ScheduledEvent) error { return nil }
func (r *schedulerTestRepo) GetDue(context.Context, time.Time, int) ([]*domain.ScheduledEvent, error) {
	if r.event == nil {
		return nil, nil
	}
	return []*domain.ScheduledEvent{r.event}, nil
}
func (r *schedulerTestRepo) Claim(_ context.Context, _ []string, claimant string, _ time.Duration) ([]*domain.ScheduledEvent, error) {
	r.claimant = claimant
	if r.claimErr != nil {
		return nil, r.claimErr
	}
	return []*domain.ScheduledEvent{r.event}, nil
}
func (r *schedulerTestRepo) MarkReleased(_ context.Context, _ string, claimant string, _ time.Time) error {
	r.releasedBy = claimant
	return r.releasedErr
}
func (r *schedulerTestRepo) ScheduleRetry(_ context.Context, _ string, claimant string, retryAt time.Time, errMsg string) error {
	r.retryBy, r.retryAt, r.retryMsg = claimant, retryAt, errMsg
	return r.retryErr
}
func (r *schedulerTestRepo) MarkFailed(_ context.Context, _ string, claimant, errMsg string) error {
	r.failedBy, r.failedMsg = claimant, errMsg
	return r.failedErr
}
func (r *schedulerTestRepo) Get(context.Context, string, string) (*domain.ScheduledEvent, error) {
	return r.event, nil
}
func (r *schedulerTestRepo) List(context.Context, string, int, int) ([]*domain.ScheduledEvent, error) {
	return nil, nil
}
func (r *schedulerTestRepo) Cancel(context.Context, string, string) error { return nil }

type schedulerTestPublisher struct {
	publishErr error
	event      *domain.EventEnvelope
}

func (p *schedulerTestPublisher) PublishEvent(_ context.Context, event *domain.EventEnvelope) error {
	p.event = event
	return p.publishErr
}

func TestTickSettlesWithTheUniqueClaimToken(t *testing.T) {
	now := time.Date(2026, time.October, 11, 10, 0, 0, 0, time.UTC)
	repo := &schedulerTestRepo{event: &domain.ScheduledEvent{
		EventID: "scheduled-1", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-1", Payload: []byte(`{"reminder":true}`), ScheduledAt: now.Add(-time.Minute),
	}}
	publisher := &schedulerTestPublisher{}
	s := NewScheduler(repo, publisher, schedulerTestClock{now: now}, time.Second, 10)

	s.tick(context.Background())

	if repo.claimant == "" || repo.claimant == "scheduler" {
		t.Fatalf("claimant = %q, want a unique per-tick token", repo.claimant)
	}
	if repo.releasedBy != repo.claimant {
		t.Fatalf("released by %q, claimed by %q", repo.releasedBy, repo.claimant)
	}
	if publisher.event == nil || publisher.event.ID != repo.event.EventID {
		t.Fatalf("publisher received incorrect envelope: %+v", publisher.event)
	}
}

func TestTickSchedulesRetryForPublishFailure(t *testing.T) {
	now := time.Date(2026, time.October, 11, 10, 0, 0, 0, time.UTC)
	repo := &schedulerTestRepo{event: &domain.ScheduledEvent{
		EventID: "scheduled-2", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-2", Payload: []byte(`{"reminder":true}`), ScheduledAt: now.Add(-time.Minute),
	}}
	publisher := &schedulerTestPublisher{publishErr: errors.New("broker unavailable")}
	s := NewScheduler(repo, publisher, schedulerTestClock{now: now}, time.Second, 10)

	s.tick(context.Background())

	if repo.retryBy != repo.claimant {
		t.Fatalf("retry scheduled by %q, claimed by %q", repo.retryBy, repo.claimant)
	}
	if !repo.retryAt.Equal(now.Add(time.Second)) {
		t.Fatalf("retry at %s, want %s", repo.retryAt, now.Add(time.Second))
	}
	if repo.retryMsg == "" {
		t.Fatal("expected the publish error to be persisted")
	}
	if repo.failedBy != "" {
		t.Fatalf("event should be retried before terminal failure, failed by %q", repo.failedBy)
	}
}

func TestTickMarksEventFailedAfterRetryLimit(t *testing.T) {
	now := time.Date(2026, time.October, 11, 10, 0, 0, 0, time.UTC)
	repo := &schedulerTestRepo{event: &domain.ScheduledEvent{
		EventID: "scheduled-final", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-final", Payload: []byte(`{"reminder":true}`), ScheduledAt: now.Add(-time.Minute),
		RetryCount: maxScheduledEventAttempts - 1,
	}}
	publisher := &schedulerTestPublisher{publishErr: errors.New("permanent publish failure")}
	s := NewScheduler(repo, publisher, schedulerTestClock{now: now}, time.Second, 10)

	s.tick(context.Background())

	if repo.failedBy != repo.claimant {
		t.Fatalf("failed by %q, claimed by %q", repo.failedBy, repo.claimant)
	}
	if repo.failedMsg == "" {
		t.Fatal("expected the terminal publish error to be persisted")
	}
	if repo.retryBy != "" {
		t.Fatalf("event should be terminal after retry limit, retry scheduled by %q", repo.retryBy)
	}
}

func TestScheduledRetryDelayIsBounded(t *testing.T) {
	if got, want := scheduledRetryDelay(1), time.Second; got != want {
		t.Fatalf("scheduledRetryDelay(1) = %s, want %s", got, want)
	}
	if got, want := scheduledRetryDelay(20), time.Minute; got != want {
		t.Fatalf("scheduledRetryDelay(20) = %s, want %s", got, want)
	}
}

func TestReleaseEventPreservesStoredHeaders(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, headers := range []map[string]string{nil, {"flowrulz_correlation_id": "corr-1", "flowrulz_parent_event_id": "parent-1", "Idempotency-Key": "original"}} {
		event := &domain.ScheduledEvent{EventID: "scheduled-1", EventType: "order.reminder", TenantID: "tenant-a", PartitionKey: "order-1", Headers: headers, Payload: []byte(`{}`)}
		publisher := &schedulerTestPublisher{}
		s := NewScheduler(&schedulerTestRepo{}, publisher, schedulerTestClock{now: now}, time.Second, 10)
		if err := s.releaseEvent(context.Background(), event, "sched-scheduled-1"); err != nil {
			t.Fatal(err)
		}
		if got := publisher.event.Headers["Idempotency-Key"]; got != "sched-scheduled-1" {
			t.Fatalf("key = %q", got)
		}
		if headers != nil && event.Headers["Idempotency-Key"] != "original" {
			t.Fatal("stored event headers were mutated")
		}
		if headers == nil && event.Headers != nil {
			t.Fatal("stored nil headers were mutated")
		}
		if publisher.event.CorrelationID != headers["flowrulz_correlation_id"] || publisher.event.CausationID != headers["flowrulz_parent_event_id"] {
			t.Fatal("correlation metadata was lost")
		}
	}
}
