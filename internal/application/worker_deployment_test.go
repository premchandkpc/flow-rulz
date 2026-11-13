package application

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
)

type deploymentDelivery struct {
	retryTestDelivery
	event   *domain.EventEnvelope
	acks    int
	retries int
}

func (d *deploymentDelivery) Event() (*domain.EventEnvelope, error) { return d.event, nil }
func (d *deploymentDelivery) Ack(context.Context) error {
	d.acks++
	return nil
}
func (d *deploymentDelivery) Retry(context.Context, time.Duration) error {
	d.retries++
	return nil
}

func TestTargetedDurableEventIsRetriedWithoutAcknowledgement(t *testing.T) {
	for name, event := range targetedEvents() {
		t.Run(name, func(t *testing.T) {
			delivery := &deploymentDelivery{event: event}
			worker := &Worker{config: WorkerConfig{RetryDelay: time.Second}}
			err := worker.processSingleDelivery(context.Background(), delivery)
			if err == nil || delivery.retries != 1 || delivery.acks != 0 {
				t.Fatalf("error=%v retries=%d acknowledgements=%d", err, delivery.retries, delivery.acks)
			}
		})
	}
}

func targetedEvents() map[string]*domain.EventEnvelope {
	return map[string]*domain.EventEnvelope{
		"service header":    {Headers: map[string]string{"flow.service_id": "orders"}},
		"revision header":   {Headers: map[string]string{"flow.deployment_revision": "1"}},
		"service metadata":  {Meta: map[string]any{"target_service_id": "orders"}},
		"revision metadata": {Meta: map[string]any{"deployment_revision": 1}},
	}
}

func TestDirectWorkerRetriesTargetedEvent(t *testing.T) {
	var steps []string
	worker, err := NewDirectWorker(directTestConsumer{}, rules.NewEvaluator(domain.SystemClock{}),
		&directTestSender{steps: &steps}, []*domain.RuleRevision{{RuleID: "orders"}}, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	event := &domain.EventEnvelope{ID: "event", Type: "orders", TenantID: "a", PartitionKey: "1", OccurredAt: time.Now(), Headers: map[string]string{"flow.service_id": "orders"}}
	delivery := &deploymentDelivery{event: event}
	if err := worker.ProcessDelivery(context.Background(), delivery); err == nil || delivery.retries != 1 || delivery.acks != 0 || len(steps) != 0 {
		t.Fatalf("error=%v retries=%d acks=%d sends=%v", err, delivery.retries, delivery.acks, steps)
	}
}

func TestWorkerRejectsConfiguredDeploymentUntilRoutingIsImplemented(t *testing.T) {
	if _, err := normalizeWorkerConfig(WorkerConfig{ServiceID: "orders", DeploymentRevision: 1}); err == nil {
		t.Fatal("service-specific worker must not start before routing and isolation exist")
	}
	if err := rejectDeploymentTarget(&domain.EventEnvelope{}); err != nil {
		t.Fatalf("ordinary events must remain supported: %v", err)
	}
}
