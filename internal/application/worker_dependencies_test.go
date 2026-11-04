package application

import (
	"context"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type constructorEventProcessor struct{}

func (constructorEventProcessor) Process(context.Context, *domain.EventEnvelope, int64, uint32, string) (*domain.Execution, error) {
	return nil, nil
}
func (constructorEventProcessor) QuarantineEvent(context.Context, string, string, string, domain.ErrorClass, string) error {
	return nil
}

type constructorEffectPublisher struct{}

func (constructorEffectPublisher) PublishBatch(context.Context, int) error { return nil }

func TestNewWorkerAcceptsOnlyShardConsumerCapabilities(t *testing.T) {
	// This fake has no generic Fetch, Subscribe, Unsubscribe or Close methods.
	consumer := &idlePollConsumer{}
	worker, err := NewWorker(consumer, constructorEventProcessor{}, constructorEffectPublisher{}, nil, 0, domain.SystemClock{}, idlePollShardService{}, "worker-1", WorkerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if worker.shardedConsumer != consumer {
		t.Fatal("worker replaced supplied consumer")
	}
}

func TestNewWorkerRejectsMissingShardConsumer(t *testing.T) {
	_, err := NewWorker(nil, constructorEventProcessor{}, constructorEffectPublisher{}, nil, 0, domain.SystemClock{}, idlePollShardService{}, "worker-1", WorkerConfig{})
	if err == nil {
		t.Fatal("missing consumer accepted")
	}
}
