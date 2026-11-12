package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// DirectConsumer receives one delivery at a time without application shard leases.
type DirectConsumer interface {
	Next(context.Context) (ports.Delivery, error)
}

// DirectWorker evaluates immutable local rules and publishes outputs before ACK.
// It deliberately has no inbox, execution store, outbox or background scheduler.
// Redelivery can repeat any outputs already published by a previous attempt.
type DirectWorker struct {
	consumer         DirectConsumer
	evaluator        ports.RuleEvaluator
	sender           ports.EffectSender
	rules            map[string]*domain.RuleRevision
	retryDelay       time.Duration
	operationTimeout time.Duration
}

// NewDirectWorker constructs the sequential, broker-managed worker path.
func NewDirectWorker(consumer DirectConsumer, evaluator ports.RuleEvaluator, sender ports.EffectSender, revisions []*domain.RuleRevision, retryDelay, operationTimeout time.Duration) (*DirectWorker, error) {
	if consumer == nil || evaluator == nil || sender == nil || retryDelay <= 0 || operationTimeout <= 0 {
		return nil, errors.New("direct worker requires consumer, evaluator, sender and positive timeouts")
	}
	w := &DirectWorker{consumer: consumer, evaluator: evaluator, sender: sender, rules: make(map[string]*domain.RuleRevision), retryDelay: retryDelay, operationTimeout: operationTimeout}
	for _, revision := range revisions {
		if revision == nil || revision.RuleID == "" || revision.InputContract != nil {
			return nil, errors.New("direct rules require a ruleset name and cannot declare an input contract; use durable mode for registry validation")
		}
		if _, exists := w.rules[revision.RuleID]; exists {
			return nil, fmt.Errorf("duplicate direct ruleset %q", revision.RuleID)
		}
		for _, rule := range revision.Compiled {
			for _, action := range append(append([]domain.Action{}, rule.Then...), rule.Otherwise...) {
				if action.ScheduleEvent != nil {
					return nil, errors.New("direct mode does not support scheduled events")
				}
			}
		}
		w.rules[revision.RuleID] = revision
	}
	if len(w.rules) == 0 {
		return nil, errors.New("direct worker requires at least one local ruleset")
	}
	return w, nil
}

// Run processes one input at a time; replicas sharing a broker consumer share work.
// It makes no cross-replica partition ordering guarantee.
func (w *DirectWorker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		delivery, err := w.consumer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive direct event: %w", err)
		}
		if delivery == nil {
			continue
		}
		opCtx, cancel := context.WithTimeout(ctx, w.operationTimeout)
		err = w.ProcessDelivery(opCtx, delivery)
		cancel()
		if err != nil {
			log.Printf("direct delivery failed: %v", err)
		}
	}
	return nil
}

// ProcessDelivery acknowledges only after all output publications succeed.
// Failures, including malformed inputs, remain retryable in the broker.
func (w *DirectWorker) ProcessDelivery(ctx context.Context, delivery ports.Delivery) error {
	err := w.process(ctx, delivery)
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err != nil {
		return errors.Join(err, delivery.Retry(settleCtx, w.retryDelay))
	}
	if err := delivery.Ack(settleCtx); err != nil {
		return fmt.Errorf("ack direct event: %w", err)
	}
	return nil
}

func (w *DirectWorker) process(ctx context.Context, delivery ports.Delivery) error {
	env, err := delivery.Event()
	if err != nil {
		return fmt.Errorf("decode direct event: %w", err)
	}
	if env == nil {
		return errors.New("direct event is nil")
	}
	if err := env.Validate(); err != nil {
		return err
	}
	if env.Contract != nil {
		return errors.New("direct mode cannot validate registry contracts; use durable mode")
	}
	revision := w.rules[env.Type]
	if revision == nil {
		return fmt.Errorf("direct ruleset for event type %q: %w", env.Type, domain.ErrRuleNotReady)
	}
	decision, err := w.evaluator.Evaluate(revision, env, nil)
	if err != nil {
		return fmt.Errorf("evaluate direct rule: %w", err)
	}
	if decision == nil {
		return errors.New("direct evaluator returned nil decision")
	}
	for i := range decision.Effects {
		effect := &decision.Effects[i]
		effect.TenantID = env.TenantID
		effect.ExecutionID = domain.ComputeExecutionID(env.TenantID, env.ID, revision.RuleID, revision.Revision)
		if err := w.sender.Send(ctx, effect, effect.ID); err != nil {
			return fmt.Errorf("publish direct output: %w", err)
		}
	}
	return nil
}
