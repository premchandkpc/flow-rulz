package application

import (
	"encoding/json"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestValidateServiceDeploymentRequiresExactTargetAndRevision(t *testing.T) {
	worker := &Worker{config: WorkerConfig{ServiceID: "tenant-a/orders-domain/orders-evaluator", DeploymentRevision: 4}}
	event := &domain.EventEnvelope{Meta: map[string]json.RawMessage{
		"flow.service_id": json.RawMessage(`"tenant-a/orders-domain/orders-evaluator"`),
		"flow.deployment_revision": json.RawMessage(`4`),
	}}
	if err := worker.validateServiceDeployment(event); err != nil {
		t.Fatalf("matching deployment rejected: %v", err)
	}
	event.Meta["flow.service_id"] = json.RawMessage(`"billing-evaluator"`)
	if err := worker.validateServiceDeployment(event); err == nil {
		t.Fatal("expected cross-service event to be rejected")
	}
	event.Meta["flow.service_id"] = json.RawMessage(`"tenant-a/orders-domain/orders-evaluator"`)
	event.Meta["flow.deployment_revision"] = json.RawMessage(`"3"`)
	if err := worker.validateServiceDeployment(event); err == nil {
		t.Fatal("expected stale deployment revision to be rejected")
	}
}

func TestValidateServiceDeploymentAllowsLegacyModeOnlyWhenUnset(t *testing.T) {
	worker := &Worker{config: WorkerConfig{}}
	if err := worker.validateServiceDeployment(&domain.EventEnvelope{}); err != nil {
		t.Fatalf("legacy worker should remain compatible during rollout: %v", err)
	}
}
