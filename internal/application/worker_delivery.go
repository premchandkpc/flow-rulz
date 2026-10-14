package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const maxDeliveryRetryDelay = 5 * time.Minute

// retryDelayForDelivery applies bounded exponential backoff with equal jitter.
// Delivery attempt metadata is optional, so adapters without it safely fall
// back to the initial delay. The broker remains responsible for durable
// redelivery; this delay is not the durable retry record.
func retryDelayForDelivery(base time.Duration, delivery ports.Delivery) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	attempt := uint64(1)
	if source, ok := delivery.(ports.DeliveryAttempt); ok {
		if n := source.Attempt(); n > 0 {
			attempt = n
		}
	}

	ceiling := base
	for n := uint64(1); n < attempt && ceiling < maxDeliveryRetryDelay; n++ {
		if ceiling > maxDeliveryRetryDelay/2 {
			ceiling = maxDeliveryRetryDelay
			break
		}
		ceiling *= 2
	}
	if ceiling > maxDeliveryRetryDelay {
		ceiling = maxDeliveryRetryDelay
	}

	lower := ceiling / 2
	jitterRange := int64(ceiling - lower)
	if jitterRange <= 0 {
		return ceiling
	}
	return lower + time.Duration(rand.Int64N(jitterRange+1)) //nolint:gosec // jitter is for retry spreading, not security
}

func (w *Worker) processDelivery(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	if env == nil {
		return errors.New("process delivery: event envelope is nil")
	}

	execution, err := w.events.Process(ctx, env, fencingToken, vshard, workerID)
	if err == nil && execution == nil {
		err = errors.New("event processor returned a nil execution without an error")
	}
	return w.settleEventResult(ctx, env, delivery, execution, err)
}

func (w *Worker) settleEventResult(
	ctx context.Context,
	env *domain.EventEnvelope,
	delivery ports.Delivery,
	execution *domain.Execution,
	processErr error,
) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if processErr != nil {
		return w.handleEventProcessingFailure(settleCtx, env, delivery, processErr)
	}
	if err := delivery.Ack(settleCtx); err != nil {
		return fmt.Errorf("ack event %s: %w", env.ID, err)
	}
	log.Printf("processed event %s -> execution %s", env.ID, execution.ID)
	return nil
}

func (w *Worker) handleEventProcessingFailure(
	settleCtx context.Context,
	env *domain.EventEnvelope,
	delivery ports.Delivery,
	processErr error,
) error {
	log.Printf("process event %s: %v", env.ID, processErr)
	if domain.IsPermanent(processErr) {
		return w.quarantinePermanentEvent(settleCtx, env, delivery, processErr)
	}
	if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
		return errors.Join(
			fmt.Errorf("process event %s: %w", env.ID, processErr),
			fmt.Errorf("retry event %s: %w", env.ID, retryErr),
		)
	}
	return processErr
}

func (w *Worker) quarantinePermanentEvent(
	settleCtx context.Context,
	env *domain.EventEnvelope,
	delivery ports.Delivery,
	processErr error,
) error {
	if err := w.events.QuarantineEvent(
		settleCtx,
		env.ID,
		env.ID,
		env.TenantID,
		domain.ClassifyError(processErr),
		processErr.Error(),
	); err != nil {
		retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery))
		return errors.Join(fmt.Errorf("quarantine permanent event %s: %w", env.ID, err), retryErr)
	}
	if err := delivery.Ack(settleCtx); err != nil {
		return fmt.Errorf("ack quarantined event %s: %w", env.ID, err)
	}
	return nil
}

// processSingleDelivery validates routing and submits a delivery to the per-key queue.
func (w *Worker) processSingleDelivery(ctx context.Context, delivery ports.Delivery) error {
	env, err := delivery.Event()
	if err != nil || env == nil {
		w.quarantineInvalidDelivery(ctx, delivery, err)
		return nil // The malformed delivery has been quarantined or scheduled for retry.
	}

	vshard := w.shardService.VirtualShardForEvent(env)
	if !w.shardService.IsOwner(vshard) {
		if err := settleNak(ctx, delivery); err != nil {
			log.Printf("nak non-owned shard %d: %v", vshard, err)
		}
		return nil
	}
	return w.submitOwnedDelivery(ctx, env, delivery, vshard)
}

func (w *Worker) submitOwnedDelivery(
	ctx context.Context,
	env *domain.EventEnvelope,
	delivery ports.Delivery,
	vshard uint32,
) error {
	fencingToken := int64(0)
	if token, ok := w.shardService.GetFencingToken(vshard); ok {
		fencingToken = token
	}
	if err := w.keyQueue.Submit(ctx, ports.QueueItem{
		Event:        env,
		Delivery:     delivery,
		FencingToken: fencingToken,
		Shard:        vshard,
		WorkerID:     w.workerID,
	}); err != nil {
		log.Printf("keyqueue submit failed: %v", err)
		if nakErr := settleNak(ctx, delivery); nakErr != nil {
			log.Printf("nak failed: %v", nakErr)
		}
		return err
	}
	return nil
}

// quarantineInvalidDelivery persists malformed broker messages before acknowledging
// them. If quarantine fails, the message is retried so the payload is not lost.
func (w *Worker) quarantineInvalidDelivery(ctx context.Context, delivery ports.Delivery, parseErr error) {
	sourceID := domain.ComputeSourceHash(delivery.Raw())
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	message := "invalid event envelope JSON"
	if parseErr != nil {
		log.Printf("invalid event envelope: subject=%q bytes=%d error=%v", delivery.Subject(), len(delivery.Raw()), parseErr)
	}
	if err := w.events.QuarantineEvent(settleCtx, sourceID, "", "", domain.ErrorClassValidation, message); err != nil {
		log.Printf("quarantine invalid event %s: %v", sourceID, err)
		if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
			log.Printf("retry invalid event %s: %v", sourceID, retryErr)
		}
		return
	}
	if err := delivery.Ack(settleCtx); err != nil {
		log.Printf("ack quarantined invalid event %s: %v", sourceID, err)
	}
}

func settleNak(ctx context.Context, delivery ports.Delivery) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return delivery.Nak(settleCtx)
}
