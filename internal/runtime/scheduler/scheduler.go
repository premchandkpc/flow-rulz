// Package scheduler provides scheduled event processing.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const maxScheduledEventAttempts = 8

// Scheduler periodically claims and publishes due scheduled events.
type Scheduler struct {
	repo         ports.ScheduledEventRepository
	publisher    ports.EventPublisher
	clock        ports.Clock
	pollInterval time.Duration
	batchSize    int
	numShards    uint32

	mu       sync.Mutex
	running  bool
	stopped  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewScheduler creates a new Scheduler with the given dependencies and configuration.
func NewScheduler(
	repo ports.ScheduledEventRepository,
	publisher ports.EventPublisher,
	clock ports.Clock,
	pollInterval time.Duration,
	batchSize int,
	numShards uint32,
) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if numShards == 0 {
		numShards = 4096
	}
	return &Scheduler{
		repo:         repo,
		publisher:    publisher,
		clock:        clock,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		numShards:    numShards,
		stopChan:     make(chan struct{}),
	}
}

// Start begins the scheduler once. Scheduler instances cannot be restarted after Stop.
func (s *Scheduler) Start(ctx context.Context) {
	if ctx == nil {
		log.Printf("scheduler: refusing to start with nil context")
		return
	}

	s.mu.Lock()
	if s.running || s.stopped {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.wg.Add(1)
	s.mu.Unlock()

	go s.runLoop(ctx)
	log.Println("scheduler started")
}

// Stop is idempotent and waits for the scheduler loop to exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stopChan)
	}
	s.running = false
	s.mu.Unlock()

	s.wg.Wait()
	log.Println("scheduler stopped")
}

func (s *Scheduler) runLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Initial tick
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	events, err := s.repo.GetDue(ctx, s.clock.Now(), s.batchSize)
	if err != nil {
		log.Printf("scheduler: get due events: %v", err)
		return
	}
	if len(events) == 0 {
		return
	}

	claimant := "scheduler-" + domain.NewID()
	claimed, err := s.claimDueEvents(ctx, events, claimant)
	if err != nil {
		log.Printf("scheduler: claim events: %v", err)
		return
	}
	log.Printf("scheduler: releasing %d scheduled events", len(claimed))
	for _, event := range claimed {
		s.processClaimedEvent(ctx, event, claimant)
	}
}

func (s *Scheduler) claimDueEvents(
	ctx context.Context,
	events []*domain.ScheduledEvent,
	claimant string,
) ([]*domain.ScheduledEvent, error) {
	eventIDs := scheduledEventIDs(events)
	return s.repo.Claim(ctx, eventIDs, claimant, 5*time.Minute)
}

func scheduledEventIDs(events []*domain.ScheduledEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	return ids
}

func (s *Scheduler) processClaimedEvent(ctx context.Context, event *domain.ScheduledEvent, claimant string) {
	idempotencyKey := fmt.Sprintf("sched-%s", event.EventID)
	if err := s.releaseEvent(ctx, event, idempotencyKey); err != nil {
		s.handleReleaseFailure(ctx, event, claimant, err)
		return
	}
	s.markEventReleased(ctx, event, claimant)
}

func (s *Scheduler) handleReleaseFailure(
	ctx context.Context,
	event *domain.ScheduledEvent,
	claimant string,
	releaseErr error,
) {
	log.Printf("scheduler: release event %s: %v", event.EventID, releaseErr)
	nextAttempt := event.RetryCount + 1
	if nextAttempt >= maxScheduledEventAttempts {
		if err := s.repo.MarkFailed(ctx, event.EventID, claimant, releaseErr.Error()); err != nil {
			log.Printf("scheduler: mark failed %s: %v", event.EventID, err)
		}
		return
	}
	retryAt := s.clock.Now().Add(scheduledRetryDelay(nextAttempt))
	if err := s.repo.ScheduleRetry(ctx, event.EventID, claimant, retryAt, releaseErr.Error()); err != nil {
		log.Printf("scheduler: schedule retry %s: %v", event.EventID, err)
	}
}

func (s *Scheduler) markEventReleased(ctx context.Context, event *domain.ScheduledEvent, claimant string) {
	// Use completion time so lease expiry is checked against the actual publish result.
	if err := s.repo.MarkReleased(ctx, event.EventID, claimant, s.clock.Now()); err != nil {
		log.Printf("scheduler: mark released %s: %v", event.EventID, err)
	}
}

func scheduledRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	delay := time.Duration(1<<uint(attempt-1)) * time.Second //nolint:gosec // attempt bounded to [1,7] above
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func (s *Scheduler) releaseEvent(ctx context.Context, evt *domain.ScheduledEvent, idempotencyKey string) error {
	env := &domain.EventEnvelope{
		ID:           evt.EventID,
		Type:         evt.EventType,
		TenantID:     evt.TenantID,
		PartitionKey: evt.PartitionKey,
		WorkflowID:   evt.WorkflowID,
		CorrelationID: evt.Headers["flowrulz_correlation_id"],
		CausationID: evt.Headers["flowrulz_parent_event_id"],
		OccurredAt:   s.clock.Now(),
		Data:         evt.Payload,
		Headers:      evt.Headers,
	}

	// Add idempotency key to headers
	if env.Headers == nil {
		env.Headers = make(map[string]string)
	}
	env.Headers["Idempotency-Key"] = idempotencyKey

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Publish to shard-specific subject using the shared base subject
	shard := env.VirtualShard(s.numShards)
	baseSubject := "events" // Must match consumer stream subject pattern
	return s.publisher.PublishToShard(ctx, baseSubject, shard, data)
}
