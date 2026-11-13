//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestTargetedEventRemainsUnacknowledgedAndUnprocessed(t *testing.T) {
	env := StartStack(t)
	env.DeployRule(t, "tenant-a", "orders", json.RawMessage(`{
		"rule_set":"orders","revision":1,"mode":"first_match",
		"rules":[{"id":"r1","when":{"path":"$.id","op":"exists"},
		"then":[{"emit":{"topic":"processed","data":{}}}]}]}`))
	event := &domain.EventEnvelope{
		ID: "targeted-event", Type: "orders", TenantID: "tenant-a", PartitionKey: "order-1",
		OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"id":"order-1"}`),
		Headers: map[string]string{"flow.service_id": "orders-service", "flow.deployment_revision": "1"},
	}
	env.Publish(t, event)
	awaitTargetedRedelivery(t, env, event)
	if got := env.ExecutionCount(t, event.TenantID, event.ID); got != 0 {
		t.Fatalf("targeted event produced %d executions", got)
	}
	if got := env.QuarantineCount(t, event.TenantID, event.ID); got != 0 {
		t.Fatalf("targeted event was quarantined %d times", got)
	}
}

func awaitTargetedRedelivery(t *testing.T, env *TestEnv, event *domain.EventEnvelope) {
	t.Helper()
	js, err := env.NATSConn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	consumer := fmt.Sprintf("e2e-test-shard-%d", event.VirtualShard(env.ShardCount))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := js.ConsumerInfo("flowrulz", consumer)
		if err == nil && info.Delivered.Consumer >= 2 && info.NumAckPending > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("targeted event was not redelivered while pending: info=%+v err=%v", info, err)
		case <-ticker.C:
		}
	}
}
