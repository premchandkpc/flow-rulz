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
	ceiling := deliveryBackoffCeiling(base, deliveryAttempt(delivery))
	lower := ceiling / 2
	jitterRange := int64(ceiling - lower)
	if jitterRange <= 0 {
		return ceiling
	}
	return lower + time.Duration(rand.Int64N(jitterRange+1)) //nolint:gosec // Retry spreading does not require cryptographic randomness.
}

func (w *Worker) processDelivery(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	if env == nil {
		return errors.New("process delivery: event envelope is nil")
	}

	started := time.Now()
	execution, err := w.events.Process(ctx, env, fencingToken, vshard, workerID)
	if err == nil && execution == nil {
		err = errors.New("event processor returned a nil execution without an error")
	}
	if observe := w.config.ObserveDeliveryStage; observe != nil {
		observe("process", err, time.Since(started))
	}
	return w.settleEventResult(ctx, env, delivery, execution, err)
}

func (w *Worker) settleEventResult(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, execution *domain.Execution, processErr error) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if processErr != nil {
		return w.handleEventProcessingFailure(settleCtx, env, delivery, processErr)
	}
	if err := w.measureDeliveryStage("ack", func() error { return delivery.Ack(settleCtx) }); err != nil {
		return fmt.Errorf("ack event %s: %w", env.ID, err)
	}
	log.Printf("processed event %s -> execution %s", env.ID, execution.ID)
	return nil
}

func (w *Worker) handleEventProcessingFailure(settleCtx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, processErr error) error {
	log.Printf("process event %s: %v", env.ID, processErr)
	if domain.IsPermanent(processErr) || w.deliveryAttemptsExhausted(delivery) {
		return w.quarantinePermanentEvent(settleCtx, env, delivery, processErr)
	}
	if retryErr := w.retryQueuedDelivery(settleCtx, delivery); retryErr != nil {
		return errors.Join(
			fmt.Errorf("process event %s: %w", env.ID, processErr),
			fmt.Errorf("retry event %s: %w", env.ID, retryErr),
		)
	}
	return processErr
}

func (w *Worker) quarantinePermanentEvent(settleCtx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, processErr error) error {
	if err := w.measureDeliveryStage("quarantine", func() error {
		return w.events.QuarantineEvent(settleCtx, env.ID, env.ID, env.TenantID, domain.ClassifyError(processErr), processErr.Error())
	}); err != nil {
		retryErr := w.retryQueuedDelivery(settleCtx, delivery)
		return errors.Join(fmt.Errorf("quarantine permanent event %s: %w", env.ID, err), retryErr)
	}
	if err := w.measureDeliveryStage("ack", func() error { return delivery.Ack(settleCtx) }); err != nil {
		return fmt.Errorf("ack quarantined event %s: %w", env.ID, err)
	}
	return nil
}

// deliveryAttemptsExhausted uses the broker's attempt counter. If metadata is
// unavailable, do not guess and accidentally discard a recoverable delivery.
func (w *Worker) deliveryAttemptsExhausted(delivery ports.Delivery) bool {
	attempt, ok := delivery.(ports.DeliveryAttempt)
	return ok && attempt.Attempt() > 0 && attempt.Attempt() >= w.config.MaxDeliveryAttempts && w.config.MaxDeliveryAttempts > 0
}

func (w *Worker) retryQueuedDelivery(ctx context.Context, delivery ports.Delivery) error {
	delay := retryDelayForDelivery(w.config.RetryDelay, delivery)
	return w.measureDeliveryStage("retry", func() error { return delivery.Retry(ctx, delay) })
}

func (w *Worker) measureDeliveryStage(stage string, run func() error) error {
	started := time.Now()
	err := run()
	if observe := w.config.ObserveDeliveryStage; observe != nil {
		observe(stage, err, time.Since(started))
	}
	return err
}

// processSingleDelivery validates routing and submits a delivery to the per-key queue.
func (w *Worker) processSingleDelivery(ctx context.Context, delivery ports.Delivery) error {
	env, err := delivery.Event()
	if err != nil || env == nil {
		w.quarantineInvalidDelivery(ctx, delivery, err)
		return nil // Malformed deliveries are quarantined or scheduled for retry.
	}
	if err := rejectDeploymentTarget(env); err != nil {
		return errors.Join(err, w.retryDeploymentTarget(ctx, delivery))
	}
	return w.routeDeliveryToShard(ctx, env, delivery)
}

func (w *Worker) submitOwnedDelivery(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, vshard uint32) error {
	fencingToken, _ := w.shardService.GetFencingToken(vshard)
	item := ports.QueueItem{
		Event: env, Delivery: delivery, FencingToken: fencingToken,
		Shard: vshard, WorkerID: w.workerID,
	}
	if err := w.submitToKeyQueue(ctx, item); err != nil {
		w.rejectQueueSubmission(ctx, delivery, err)
		return err
	}
	return nil
}

func (w *Worker) submitToKeyQueue(ctx context.Context, item ports.QueueItem) error {
	if queue, ok := w.keyQueue.(interface {
		TrySubmit(context.Context, ports.QueueItem) error
	}); ok {
		return queue.TrySubmit(ctx, item)
	}
	return w.keyQueue.Submit(ctx, item)
}

// quarantineInvalidDelivery persists malformed broker messages before acknowledging
// them. If quarantine fails, the message is retried so the payload is not lost.
func (w *Worker) quarantineInvalidDelivery(ctx context.Context, delivery ports.Delivery, parseErr error) {
	sourceID := domain.ComputeSourceHash(delivery.Raw())
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if parseErr != nil {
		log.Printf("invalid event envelope: subject=%q bytes=%d error=%v", delivery.Subject(), len(delivery.Raw()), parseErr)
	}
	if err := w.events.QuarantineEvent(settleCtx, sourceID, "", "", domain.ErrorClassValidation, "invalid event envelope JSON"); err != nil {
		w.retryInvalidDelivery(settleCtx, delivery, sourceID, err)
		return
	}
	if err := delivery.Ack(settleCtx); err != nil {
		log.Printf("ack quarantined invalid event %s: %v", sourceID, err)
	}
}

// settleRetry delays redelivery when another worker owns the shard, avoiding a hot NAK loop.
func settleRetry(ctx context.Context, delivery ports.Delivery, delay time.Duration) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return delivery.Retry(settleCtx, delay)
}

func rejectDeploymentTarget(env *domain.EventEnvelope) error {
	if env.HasDeploymentTarget() {
		return errors.New("service-targeted events require service-specific routing, which is not implemented")
	}
	return nil
}

func deliveryAttempt(delivery ports.Delivery) uint64 {
	if source, ok := delivery.(ports.DeliveryAttempt); ok && source.Attempt() > 0 {
		return source.Attempt()
	}
	return 1
}

func deliveryBackoffCeiling(base time.Duration, attempt uint64) time.Duration {
	ceiling := base
	for n := uint64(1); n < attempt && ceiling < maxDeliveryRetryDelay; n++ {
		if ceiling > maxDeliveryRetryDelay/2 {
			return maxDeliveryRetryDelay
		}
		ceiling *= 2
	}
	return min(ceiling, maxDeliveryRetryDelay)
}

func (w *Worker) retryDeploymentTarget(ctx context.Context, delivery ports.Delivery) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.retryQueuedDelivery(settleCtx, delivery)
}

func (w *Worker) routeDeliveryToShard(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery) error {
	vshard := w.shardService.VirtualShardForEvent(env)
	if !w.shardService.IsOwner(vshard) {
		if err := settleRetry(ctx, delivery, retryDelayForDelivery(w.config.RetryDelay, delivery)); err != nil {
			log.Printf("delay non-owned shard %d: %v", vshard, err)
		}
		return nil
	}
	return w.submitOwnedDelivery(ctx, env, delivery, vshard)
}

func (w *Worker) rejectQueueSubmission(ctx context.Context, delivery ports.Delivery, err error) {
	log.Printf("keyqueue submit failed: %v", err)
	if retryErr := settleRetry(ctx, delivery, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
		log.Printf("retry rejected keyqueue delivery: %v", retryErr)
	}
}

func (w *Worker) retryInvalidDelivery(ctx context.Context, delivery ports.Delivery, sourceID string, err error) {
	log.Printf("quarantine invalid event %s: %v", sourceID, err)
	if retryErr := delivery.Retry(ctx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
		log.Printf("retry invalid event %s: %v", sourceID, retryErr)
	}
}
