//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	effectadapter "github.com/flowrulz/flow-rulz/internal/adapters/effects"
	broker "github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
	effects "github.com/flowrulz/flow-rulz/internal/services/effects"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testcontainers "github.com/testcontainers/testcontainers-go"
	natstest "github.com/testcontainers/testcontainers-go/modules/nats"
)

// TestDirectWorkerChainWithoutDatabase proves A -> worker -> child -> worker -> B
// using only NATS, local compiled rules and the production effect router.
func TestDirectWorkerChainWithoutDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	container, err := natstest.RunContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Error(err)
		}
	})
	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := broker.Config{NatsURL: url, Stream: "direct-e2e", Consumer: "direct-e2e", Subjects: []string{"events"}, NumShards: 4, AckWait: 10 * time.Second, MaxDeliver: -1}
	consumer, err := broker.NewDirectConsumer(ctx, cfg, "events.shard.*", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	publisher, err := broker.NewPublisher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	conn, err := natsgo.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{Durable: "service-b", FilterSubject: "commands.b.handle", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	var revisions []*domain.RuleRevision
	for _, name := range []string{"a.json", "processed.json"} {
		source, err := os.ReadFile("../../examples/direct-worker/rules/" + name)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := rules.NewCompiler(rules.DefaultLimits()).Compile(source)
		if err != nil {
			t.Fatal(err)
		}
		revisions = append(revisions, revision)
	}
	sender := effects.NewRouterWithConfig(publisher, effectadapter.NewNATSDestination(publisher), "events", 4)
	w, err := application.NewDirectWorker(consumer, rules.NewEvaluator(domain.SystemClock{}), sender, revisions, time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(workerCtx) }()
	defer func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("direct worker shutdown timed out")
		}
	}()
	event := domain.EventEnvelope{ID: "direct-1", Type: "a.created", TenantID: "tenant-a", PartitionKey: "key-1", OccurredAt: time.Now(), Data: json.RawMessage(`{"id":"123"}`)}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishToShard(ctx, "events", event.VirtualShard(4), payload); err != nil {
		t.Fatal(err)
	}
	message, err := b.Next(jetstream.FetchMaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("B did not receive command: %v", err)
	}
	var effect domain.Effect
	if err := json.Unmarshal(message.Data(), &effect); err != nil {
		t.Fatal(err)
	}
	if effect.Destination != "b" || effect.Name != "handle" || effect.TenantID != event.TenantID || effect.IdempotencyKey == "" {
		t.Fatalf("unexpected command identity: %+v", effect)
	}
	var data map[string]any
	if err := json.Unmarshal(effect.Payload, &data); err != nil {
		t.Fatal(err)
	}
	if data["source"] != "a.processed" || data["action"] != "handle" {
		t.Fatalf("unexpected command payload: %s", effect.Payload)
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
	direct, err := js.Consumer(ctx, cfg.Stream, cfg.Consumer+"-direct")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := direct.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.AckFloor.Consumer >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("direct inputs not acknowledged: %+v", info)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
