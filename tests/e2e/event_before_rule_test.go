//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
)

// TestEventBeforeRuleActivation_IsNotLost verifies that an event published
// before its rule is deployed still produces exactly one execution after
// the rule activation. Missing activation is retryable until the rule is ready.
func TestEventBeforeRuleActivation_IsNotLost(t *testing.T) {
	env := StartStack(t)

	// 1. Publish event BEFORE rule exists
	testEvent := &domain.EventEnvelope{
		ID:           "e1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-1",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"order_id":"order-1","amount":1500}`),
	}

	env.Publish(t, testEvent)

	// Worker sees event, no rule yet → should retry (not quarantine)
	time.Sleep(2 * time.Second)

	// 2. Deploy the rule AFTER event arrived
	repositories := env.Storage.Repositories()
	compiler := rules.NewCompiler(rules.DefaultLimits())

	ruleSource := json.RawMessage(`{
		"rule_set": "order.created",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "high-value",
			"name": "High Value Order",
			"priority": 10,
			"when": {"path": "$.amount", "op": "gt", "value": 1000},
			"then": [{
				"emit_event": {
					"type": "order.flagged",
					"partition_key_policy": "inherit",
					"data": {"order_id": "{{$.order_id}}", "flag": "high-value", "reviewed_at": "{{now}}"}
				}
			}]
		}]
	}`)
	revision, err := compiler.Compile(ruleSource)
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}
	revision.TenantScope = "test-tenant"
	revision.RuleID = "order.created"
	if err := repositories.Rules.Save(context.Background(), revision); err != nil {
		t.Fatalf("save revision: %v", err)
	}

	activation := &domain.RuleActivation{
		TenantScope: "test-tenant",
		RuleSet:     "order.created",
		Revision:    1,
		Version:     1,
		Actor:       "e2e-test",
		ActivatedAt: domain.SystemClock{}.Now(),
	}
	if err := repositories.Activations.Set(context.Background(), activation); err != nil {
		t.Fatalf("set activation: %v", err)
	}

	// 3. Wait for redelivery and processing
	if !env.WaitForExecution(t, "test-tenant", "e1", 30*time.Second) {
		t.Fatal("timeout: execution never created - event was lost")
	}
	if got := env.ExecutionCount(t, "test-tenant", "e1"); got != 1 {
		t.Fatalf("execution count = %d, want exactly one", got)
	}
	if got := env.QuarantineCount(t, "test-tenant", "e1"); got != 0 {
		t.Fatalf("quarantine count = %d, want zero", got)
	}
}

// TestEventBeforeRuleActivation_WithMaxDeliver verifies the retry budget
// is respected. If MaxDeliver is exhausted before rule activation, the
// event should be quarantined (not silently dropped).
func TestEventBeforeRuleActivation_WithMaxDeliver(t *testing.T) {
	// TODO: requires configuring NATS MaxDeliver on the test consumer
	// and verifying quarantine after N redeliveries
	t.Skip("requires NATS consumer MaxDeliver config in harness")
}

// TestCrashAfterCommitBeforeAck verifies that killing the worker after
// the DB commit but before the NATS ACK results in exactly one execution
// on redelivery (not zero, not two).
func TestCrashAfterCommitBeforeAck(t *testing.T) {
	// TODO: requires testcontainers exec or signal injection
	t.Skip("requires process crash injection")
}

// TestConcurrentWorkersPerKeyOrder verifies 3 workers processing 10k events
// across 100 keys produces zero order violations.
func TestConcurrentWorkersPerKeyOrder(t *testing.T) {
	t.Skip("requires multi-worker harness + SQL invariant check")
}

// TestEffectDeliveryNoDuplicates verifies no effect_id is delivered twice
// to an idempotent receiver under publisher stall/reclaim.
func TestEffectDeliveryNoDuplicates(t *testing.T) {
	t.Skip("requires fake HTTP destination with dedup tracking")
}

// TestStaleLeaseHolderFenced verifies a lease holder that pauses for GC
// cannot commit after another worker takes over the lease.
func TestStaleLeaseHolderFenced(t *testing.T) {
	t.Skip("requires lease takeover injection")
}
