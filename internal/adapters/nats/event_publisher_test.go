package nats

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type envelopeTestPublisher struct {
	base    string
	shard   uint32
	payload []byte
	err     error
	ctx     context.Context
}

func (p *envelopeTestPublisher) PublishToShard(ctx context.Context, base string, shard uint32, payload []byte) error {
	p.ctx, p.base, p.shard, p.payload = ctx, base, shard, payload
	return p.err
}

func TestEventPublisherPreservesEnvelopeAndRouting(t *testing.T) {
	for _, tc := range []struct {
		name, base, wantBase string
		shards, wantShards   uint32
	}{
		{"configured", "custom.events", "custom.events", 64, 64},
		{"defaults", "", "events", 0, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := &envelopeTestPublisher{}
			publisher := NewEventPublisher(broker, tc.base, tc.shards)
			event := &domain.EventEnvelope{
				ID: "evt-child", Type: "payment.requested", TenantID: "tenant-a", PartitionKey: "order-42",
				OccurredAt: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC), Data: json.RawMessage(`{"amount":42}`),
				Headers: map[string]string{"Idempotency-Key": "sched-evt-child"},
				Meta:    map[string]any{"tenant_id": "tenant-a", "partition_key": "order-42", "trace": "trace-1"},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := publisher.PublishEvent(ctx, event); err != nil {
				t.Fatal(err)
			}
			if broker.base != tc.wantBase || broker.shard != event.VirtualShard(tc.wantShards) {
				t.Fatalf("routing = %s/%d, want %s/%d", broker.base, broker.shard, tc.wantBase, event.VirtualShard(tc.wantShards))
			}
			if broker.ctx != ctx {
				t.Fatal("caller context was replaced")
			}
			var got domain.EventEnvelope
			if err := json.Unmarshal(broker.payload, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(&got, event) {
				t.Fatalf("envelope changed: got %+v want %+v", got, event)
			}
			// Ruleset subject mapping remains in the underlying NATS publisher.
			gotSubject := subjectForMessage(broker.base, broker.shard, broker.payload, []string{event.Type})
			if want := ruleSetSubject(tc.wantBase, event.Type, broker.shard); gotSubject != want {
				t.Fatalf("ruleset subject = %s, want %s", gotSubject, want)
			}
		})
	}
}

func TestEventPublisherRejectsInvalidEnvelopesBeforePublishing(t *testing.T) {
	for _, event := range []*domain.EventEnvelope{nil, {}, {ID: "evt"}, {
		ID: "evt", Type: "test", TenantID: "a", PartitionKey: "key", OccurredAt: time.Now(), Data: json.RawMessage(`{`),
	}} {
		broker := &envelopeTestPublisher{}
		if err := NewEventPublisher(broker, "events", 64).PublishEvent(context.Background(), event); err == nil {
			t.Fatal("expected validation or encoding error")
		}
		if broker.payload != nil {
			t.Fatal("invalid event reached broker")
		}
	}
}

func TestEventPublisherPropagatesBrokerFailure(t *testing.T) {
	want := errors.New("broker unavailable")
	broker := &envelopeTestPublisher{err: want}
	event := &domain.EventEnvelope{ID: "evt", Type: "test", TenantID: "a", PartitionKey: "key", OccurredAt: time.Now()}
	if err := NewEventPublisher(broker, "events", 64).PublishEvent(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if err := NewEventPublisher(nil, "events", 64).PublishEvent(context.Background(), event); err == nil {
		t.Fatal("missing publisher accepted")
	}
}
