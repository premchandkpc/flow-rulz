package domainservices

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestDomainDeploymentPinsImmutableRulesetRevisions(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(40, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name:       "orders",
		EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID:     "order-evaluator",
			Name:          "order-evaluator",
			Revision:      1,
			Active:        true,
			Rulesets:      []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 7}},
			WorkerProfile: domain.WorkerProfile{MinReplicas: 1, MaxReplicas: 4, Concurrency: 8},
		}},
	})
	if err != nil {
		t.Fatalf("SaveDomain() with valid deployment error = %v", err)
	}
	if len(item.Deployments) != 1 || item.Deployments[0].Rulesets[0].Revision != 7 {
		t.Fatalf("saved deployment lost its pinned ruleset revision: %#v", item.Deployments)
	}

	invalid := &domain.TenantDomain{
		Name:       "billing",
		EventTypes: []string{"billing.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "billing-worker", Name: "billing-worker", Revision: 1, Active: true,
			Rulesets: []domain.RulesetAssignment{
				{RulesetID: "invoice", Revision: 2},
				{RulesetID: "invoice", Revision: 3},
			},
		}},
	}
	if _, err := service.SaveDomain(context.Background(), "tenant-a", invalid); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate ruleset assignment error = %v, want ErrInvalidInput", err)
	}
}

func TestDomainDeploymentChangesRequireRevisionBumpAndExplicitDeactivation(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(60, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "order-worker", Name: "order-worker", Revision: 1, Active: true,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 7}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	changed := *item
	changed.Deployments = append([]domain.ServiceDeployment(nil), item.Deployments...)
	changed.Deployments[0].Rulesets = []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 8}}
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &changed); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("changed assignment without revision bump = %v, want ErrInvalidInput", err)
	}

	changed.Deployments[0].Revision = 2
	updated, err := service.SaveDomain(context.Background(), "tenant-a", &changed)
	if err != nil {
		t.Fatalf("assignment update with revision bump: %v", err)
	}

	removed := *updated
	removed.Deployments = nil
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &removed); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("removing active deployment = %v, want ErrInvalidInput", err)
	}

	deactivated := *updated
	deactivated.Deployments = append([]domain.ServiceDeployment(nil), updated.Deployments...)
	deactivated.Deployments[0].Active = false
	deactivated.Deployments[0].Revision = 3
	updated, err = service.SaveDomain(context.Background(), "tenant-a", &deactivated)
	if err != nil {
		t.Fatalf("deactivate deployment with revision bump: %v", err)
	}
	removed = *updated
	removed.Deployments = nil
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &removed); err != nil {
		t.Fatalf("remove previously deactivated deployment: %v", err)
	}
}
