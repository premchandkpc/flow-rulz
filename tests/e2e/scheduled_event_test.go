//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// TestScheduleEventActionPersistsAndPublishesChild verifies the full durable path:
// rule evaluation -> transactional outbox -> scheduled-event storage -> scheduler -> NATS -> worker.
func TestScheduleEventActionPersistsAndPublishesChild(t *testing.T) {
	env := StartStack(t)

	env.DeployRule(t, "test-tenant", "order.created", json.RawMessage(`{
		"rule_set":"order.created",
		"revision":1,
		"mode":"first_match",
		"rules":[{
			"id":"schedule-reminder",
			"priority":10,
			"when":{"path":"$.order_id","op":"exists"},
			"then":[{
				"schedule_event":{
					"type":"order.reminder",
					"delay_seconds":1,
					"partition_key_policy":"inherit",
					"data":{"order_id":"{{$.order_id}}"}
				}
			}]
		}]
	}`))
	env.DeployRule(t, "test-tenant", "order.reminder", json.RawMessage(`{
		"rule_set":"order.reminder",
		"revision":1,
		"mode":"first_match",
		"rules":[{
			"id":"observe-reminder",
			"priority":10,
			"when":{"path":"$.order_id","op":"exists"},
			"then":[{"emit":{"topic":"reminder.observed","data":{}}}]
		}]
	}`))

	parent := &domain.EventEnvelope{
		ID:            "schedule-parent-1",
		Type:          "order.created",
		TenantID:      "test-tenant",
		PartitionKey:  "order-1",
		CorrelationID: "correlation-1",
		OccurredAt:    time.Now().UTC(),
		Data:          json.RawMessage(`{"order_id":"order-1"}`),
	}
	env.Publish(t, parent)

	if !env.WaitForExecution(t, parent.TenantID, parent.ID, 20*time.Second) {
		t.Fatal("parent event was not executed")
	}

	childID := domain.ComputeChildEventID(parent.TenantID, parent.ID, "schedule-reminder", 0)
	if !env.WaitForExecution(t, parent.TenantID, childID, 20*time.Second) {
		t.Fatalf("scheduled child event %q was not executed", childID)
	}
	if got := env.ExecutionCount(t, parent.TenantID, childID); got != 1 {
		t.Fatalf("scheduled child execution count = %d, want exactly one", got)
	}

	var tenantID, eventType, partitionKey, status string
	var payload []byte
	var headers map[string]string
	err := env.PostgresPool.QueryRow(
		t.Context(),
		`SELECT tenant_id, event_type, partition_key, payload, headers, status
		 FROM scheduled_events WHERE event_id = $1`,
		childID,
	).Scan(&tenantID, &eventType, &partitionKey, &payload, &headers, &status)
	if err != nil {
		t.Fatalf("load persisted scheduled event: %v", err)
	}
	if tenantID != parent.TenantID || eventType != "order.reminder" || partitionKey != parent.PartitionKey {
		t.Fatalf("scheduled event identity = tenant %q, type %q, partition %q", tenantID, eventType, partitionKey)
	}
	var childData map[string]any
	if err := json.Unmarshal(payload, &childData); err != nil {
		t.Fatalf("decode scheduled payload: %v", err)
	}
	if childData["order_id"] != "order-1" {
		t.Fatalf("scheduled payload = %#v, want order_id order-1", childData)
	}
	if headers["flowrulz_parent_event_id"] != parent.ID || headers["flowrulz_correlation_id"] != parent.CorrelationID {
		t.Fatalf("scheduled headers lost parent/correlation metadata: %#v", headers)
	}
	if status != string(domain.ScheduledEventStatusReleased) {
		t.Fatalf("scheduled event status = %q, want %q", status, domain.ScheduledEventStatusReleased)
	}
}
