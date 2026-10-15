package domainservices

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

func TestDomainDeploymentPinsImmutableRulesetRevisions(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(40, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders",
		EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "order-evaluator",
			Name: "order-evaluator",
			Revision: 1,
			Active: true,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 7}},
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
		Name: "billing",
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

type ruleRevisionStub struct {
	ports.RuleService
	revision *domain.RuleRevision
	err error
}

func (r ruleRevisionStub) GetRevision(context.Context, string, string, int64) (*domain.RuleRevision, error) {
	return r.revision, r.err
}

func TestDomainDeploymentRejectsUnavailableTenantScopedRevision(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(50, 0).UTC()}).WithRuleService(ruleRevisionStub{
		revision: &domain.RuleRevision{TenantScope: "tenant-b", RuleID: "order-created", Revision: 7},
	})
	_, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "order-worker", Name: "order-worker", Revision: 1, Active: true,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 7}},
		}},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant revision error = %v, want ErrInvalidInput", err)
	}
}
