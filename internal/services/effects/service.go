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
func NewService(
	outbox ports.OutboxDeliveryStore,
	effectSender ports.EffectSender,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
	claimant string,
) *Service {
	if claimant == "" {
		claimant = "publisher"
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &Service{
		outbox:       outbox,
		effectSender: effectSender,
		quarantine:   quarantine,
		clock:        clock,
		claimant:     claimant,
	}
}

// PublishBatch claims pending effects and publishes them. Each invocation uses
// a unique claim token so a slow/stale publisher cannot settle a later claim
// made by the same configured worker identity. Persistence failures are
// returned to the caller instead of being logged and silently discarded.
func (s *Service) PublishBatch(ctx context.Context, batchSize int) error {
	if s.outbox == nil {
		return errors.New("publish effects: outbox repository is not configured")
	}
	if s.effectSender == nil {
		return errors.New("publish effects: effect sender is not configured")
	}
	if batchSize <= 0 {
		return nil
	}

	claimToken := fmt.Sprintf("%s-%s", s.claimant, domain.NewID())
	effects, err := s.outbox.ClaimPending(ctx, batchSize, claimToken, 2*time.Minute)
	if err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}

	var failures []error
	for _, ef := range effects {
		effect := &domain.Effect{
			ID:          ef.ID,
			ExecutionID: ef.ExecutionID,
			TenantID:    ef.TenantID,
			Destination: ef.Destination,
			Name:        ef.Name,
			Payload:     ef.Payload,
			EffectType:  ef.EffectType,
			CreatedAt:   ef.CreatedAt,
		}

		// The outbox effect ID remains the idempotency key across retries.
		idempotencyKey := fmt.Sprintf("effect-%s", ef.ID)
		sendErr := s.effectSender.Send(ctx, effect, idempotencyKey)
		if sendErr != nil {
			attempts := ef.Attempts + 1
			if attempts >= ef.MaxAttempts {
				if s.quarantine == nil {
					failures = append(failures, fmt.Errorf("quarantine effect %s: quarantine repository is not configured (send error: %w)", ef.ID, sendErr))
					continue
				}

				// Use a claim-specific ID. If this claim expired while sending, a
				// newer publisher's quarantine record must never be deleted as
				// cleanup for this stale claim.
				quarantineID := fmt.Sprintf("effect-%s-%s", ef.ID, domain.NewID())
				qEntry := &domain.QuarantineEntry{
					ID:         quarantineID,
					TenantID:   ef.TenantID,
					SourceType: "effect",
					SourceID:   ef.ID,
					ErrorClass: string(domain.ClassifyError(sendErr)),
					Error:      sendErr.Error(),
					CreatedAt:  s.clock.Now(),
				}
				if qErr := s.quarantine.Save(ctx, qEntry); qErr != nil {
					failures = append(failures, fmt.Errorf("persist quarantine for effect %s: %w", ef.ID, qErr))
					continue
				}
				if qErr := s.outbox.Quarantine(ctx, ef.ID, claimToken, sendErr.Error()); qErr != nil {
					failures = append(failures, fmt.Errorf("mark effect %s quarantined: %w", ef.ID, qErr))
					if deleteErr := s.quarantine.Delete(ctx, ef.TenantID, quarantineID); deleteErr != nil {
						failures = append(failures, fmt.Errorf("remove orphan quarantine for effect %s: %w", ef.ID, deleteErr))
					}
				}
				continue
			}

			if retryErr := s.outbox.ScheduleRetry(ctx, ef.ID, claimToken, backoffDuration(attempts), attempts, sendErr.Error()); retryErr != nil {
				failures = append(failures, fmt.Errorf("schedule retry for effect %s: %w", ef.ID, retryErr))
			}
			continue
		}

		if settleErr := s.outbox.MarkDelivered(ctx, []string{ef.ID}, claimToken); settleErr != nil {
			failures = append(failures, fmt.Errorf("mark effect %s delivered: %w", ef.ID, settleErr))
		}
	}

	return errors.Join(failures...)
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
