package effects

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service handles effect publishing, retries, and quarantine.
type Service struct {
	outbox       ports.OutboxDeliveryStore
	effectSender ports.EffectSender
	quarantine   ports.QuarantineRepository
	clock        ports.Clock
	claimant     string
}

// NewService creates a new effects Service for publishing, retrying, and quarantining effects.
func NewService(outbox ports.OutboxDeliveryStore, effectSender ports.EffectSender, quarantine ports.QuarantineRepository, clock ports.Clock, claimant string) *Service {
	claimant, clock = publisherDefaults(claimant, clock)
	return &Service{
		outbox: outbox, effectSender: effectSender, quarantine: quarantine,
		clock: clock, claimant: claimant,
	}
}

// publisherDefaults resolves the optional claimant identity and clock, falling
// back to the documented defaults for missing configuration.
func publisherDefaults(claimant string, clock ports.Clock) (string, ports.Clock) {
	if claimant == "" {
		claimant = "publisher"
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return claimant, clock
}

// PublishBatch claims pending effects and publishes them, then settles each
// claim as delivered, scheduled for retry, or quarantined.
func (s *Service) PublishBatch(ctx context.Context, batchSize int) error {
	if err := s.validateBatchConfig(); err != nil {
		return err
	}
	if batchSize <= 0 {
		return nil
	}
	claimToken, effects, err := s.claimPendingEffects(ctx, batchSize)
	if err != nil {
		return err
	}
	return s.deliverClaimedBatch(ctx, effects, claimToken)
}

// validateBatchConfig rejects a publisher that cannot send or settle effects.
func (s *Service) validateBatchConfig() error {
	if s.outbox == nil {
		return errors.New("publish effects: outbox repository is not configured")
	}
	if s.effectSender == nil {
		return errors.New("publish effects: effect sender is not configured")
	}
	return nil
}

// claimPendingEffects claims a batch under a unique claim token so a slow or
// stale publisher cannot settle a later claim made by the same configured
// worker identity. Persistence failures are returned to the caller instead of
// being logged and silently discarded.
func (s *Service) claimPendingEffects(ctx context.Context, batchSize int) (string, []domain.OutboxEffect, error) {
	claimToken := fmt.Sprintf("%s-%s", s.claimant, domain.NewID())
	effects, err := s.outbox.ClaimPending(ctx, batchSize, claimToken, 2*time.Minute)
	if err != nil {
		return "", nil, fmt.Errorf("claim pending: %w", err)
	}
	return claimToken, effects, nil
}

// deliverClaimedEffect sends one claimed effect and returns the settlement
// failures for that effect, preserving the claim, deliver and settle order.
func (s *Service) deliverClaimedEffect(ctx context.Context, ef domain.OutboxEffect, claimToken string) []error {
	idempotencyKey := fmt.Sprintf("effect-%s", ef.ID)
	sendErr := s.effectSender.Send(ctx, claimedDomainEffect(ef), idempotencyKey)
	if sendErr == nil {
		return s.settleDeliveredEffect(ctx, ef, claimToken)
	}
	attempts := ef.Attempts + 1
	if attempts >= ef.MaxAttempts {
		return s.quarantineExhaustedEffect(ctx, ef, claimToken, sendErr)
	}
	if retryErr := s.outbox.ScheduleRetry(ctx, ef.ID, claimToken, backoffDuration(attempts), attempts, sendErr.Error()); retryErr != nil {
		return []error{fmt.Errorf("schedule retry for effect %s: %w", ef.ID, retryErr)}
	}
	return nil
}

// claimedDomainEffect converts a stored outbox effect into the dispatch form.
func claimedDomainEffect(ef domain.OutboxEffect) *domain.Effect {
	return &domain.Effect{
		ID: ef.ID, ExecutionID: ef.ExecutionID, TenantID: ef.TenantID,
		Destination: ef.Destination, Name: ef.Name, Payload: ef.Payload,
		EffectType: ef.EffectType, CreatedAt: ef.CreatedAt,
	}
}

// settleDeliveredEffect records a successful delivery under the claim token.
func (s *Service) settleDeliveredEffect(ctx context.Context, ef domain.OutboxEffect, claimToken string) []error {
	if settleErr := s.outbox.MarkDelivered(ctx, []string{ef.ID}, claimToken); settleErr != nil {
		return []error{fmt.Errorf("mark effect %s delivered: %w", ef.ID, settleErr)}
	}
	return nil
}

// quarantineExhaustedEffect persists a claim-specific quarantine record for an
// effect that exhausted its attempts, then marks the outbox effect quarantined.
// The claim-specific ID prevents stale-claim cleanup deleting a newer record.
func (s *Service) quarantineExhaustedEffect(ctx context.Context, ef domain.OutboxEffect, claimToken string, sendErr error) []error {
	if s.quarantine == nil {
		return []error{fmt.Errorf("quarantine effect %s: quarantine repository is not configured (send error: %w)", ef.ID, sendErr)}
	}
	quarantineID := fmt.Sprintf("effect-%s-%s", ef.ID, domain.NewID())
	qEntry := newEffectQuarantineEntry(ef, quarantineID, sendErr, s.clock.Now())
	if qErr := s.quarantine.Save(ctx, qEntry); qErr != nil {
		return []error{fmt.Errorf("persist quarantine for effect %s: %w", ef.ID, qErr)}
	}
	if qErr := s.outbox.Quarantine(ctx, ef.ID, claimToken, sendErr.Error()); qErr != nil {
		return s.markExhaustedEffectQuarantined(ctx, ef, quarantineID, qErr)
	}
	return nil
}

// newEffectQuarantineEntry builds the claim-specific quarantine record for an effect.
func newEffectQuarantineEntry(ef domain.OutboxEffect, quarantineID string, sendErr error, now time.Time) *domain.QuarantineEntry {
	return &domain.QuarantineEntry{
		ID: quarantineID, TenantID: ef.TenantID, SourceType: "effect", SourceID: ef.ID,
		ErrorClass: string(domain.ClassifyError(sendErr)),
		Error:      sendErr.Error(),
		CreatedAt:  now,
	}
}

// markExhaustedEffectQuarantined reports the failed outbox quarantine
// transition and removes the just-saved record when the claim was lost, so
// cleanup never deletes a newer publisher's record.
func (s *Service) markExhaustedEffectQuarantined(ctx context.Context, ef domain.OutboxEffect, quarantineID string, qErr error) []error {
	failures := []error{fmt.Errorf("mark effect %s quarantined: %w", ef.ID, qErr)}
	if deleteErr := s.quarantine.Delete(ctx, ef.TenantID, quarantineID); deleteErr != nil {
		failures = append(failures, fmt.Errorf("remove orphan quarantine for effect %s: %w", ef.ID, deleteErr))
	}
	return failures
}

func backoffDuration(attempts int) time.Duration {
	// Cap attempts to prevent overflow.
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 30 {
		attempts = 30
	}
	delay := time.Duration(1<<uint(attempts-1)) * time.Second //nolint:gosec // attempts bounded to [1,30] above
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	return delay
}

// Dispatch sends a single effect immediately (bypasses outbox).
func (s *Service) Dispatch(ctx context.Context, effect *domain.Effect) error {
	if effect == nil {
		return errors.New("dispatch effect: effect is nil")
	}
	if s.effectSender == nil {
		return errors.New("dispatch effect: effect sender is not configured")
	}
	idempotencyKey := fmt.Sprintf("effect-%s", effect.ID)
	return s.effectSender.Send(ctx, effect, idempotencyKey)
}

// GetPending returns pending outbox effects.
func (s *Service) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	return s.outbox.GetPending(ctx, limit)
}

// GetByExecution returns outbox effects for a specific execution.
func (s *Service) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	return s.outbox.GetByExecution(ctx, executionID)
}

func (s *Service) deliverClaimedBatch(ctx context.Context, effects []domain.OutboxEffect, claimToken string) error {
	var failures []error
	for _, effect := range effects {
		failures = append(failures, s.deliverClaimedEffect(ctx, effect, claimToken)...)
	}
	return errors.Join(failures...)
}
