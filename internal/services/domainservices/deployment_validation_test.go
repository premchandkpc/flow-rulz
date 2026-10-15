package domainservices

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestValidateDeploymentShapeRejectsInvalidAssignments(t *testing.T) {
	valid := domain.ServiceDeployment{
		ServiceID: "orders-worker", Name: "orders-worker", Revision: 1, Active: true,
		Rulesets: []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 1}},
	}
	tests := []struct {
		name string
		edit func(*domain.ServiceDeployment)
	}{
		{name: "empty service id", edit: func(d *domain.ServiceDeployment) { d.ServiceID = "" }},
		{name: "invalid service name", edit: func(d *domain.ServiceDeployment) { d.Name = "Orders Worker" }},
		{name: "zero deployment revision", edit: func(d *domain.ServiceDeployment) { d.Revision = 0 }},
		{name: "negative deployment revision", edit: func(d *domain.ServiceDeployment) { d.Revision = -1 }},
		{name: "no rulesets", edit: func(d *domain.ServiceDeployment) { d.Rulesets = nil }},
		{name: "empty ruleset id", edit: func(d *domain.ServiceDeployment) { d.Rulesets[0].RulesetID = "" }},
		{name: "zero ruleset revision", edit: func(d *domain.ServiceDeployment) { d.Rulesets[0].Revision = 0 }},
		{name: "negative ruleset revision", edit: func(d *domain.ServiceDeployment) { d.Rulesets[0].Revision = -1 }},
		{name: "negative min replicas", edit: func(d *domain.ServiceDeployment) { d.WorkerProfile.MinReplicas = -1 }},
		{name: "negative max replicas", edit: func(d *domain.ServiceDeployment) { d.WorkerProfile.MaxReplicas = -1 }},
		{name: "negative concurrency", edit: func(d *domain.ServiceDeployment) { d.WorkerProfile.Concurrency = -1 }},
		{name: "min exceeds max", edit: func(d *domain.ServiceDeployment) { d.WorkerProfile = domain.WorkerProfile{MinReplicas: 4, MaxReplicas: 2} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := valid
			deployment.Rulesets = append([]domain.RulesetAssignment(nil), valid.Rulesets...)
			tt.edit(&deployment)
			service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(70, 0).UTC()})
			_, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
				Name: "orders", EventTypes: []string{"orders.created"},
				Deployments: []domain.ServiceDeployment{deployment},
			})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("SaveDomain() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestValidateDeploymentRejectsDuplicateServiceIDsAndNames(t *testing.T) {
	first := domain.ServiceDeployment{
		ServiceID: "orders-worker", Name: "orders-worker", Revision: 1,
		Rulesets: []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 1}},
	}
	tests := []struct {
		name   string
		second domain.ServiceDeployment
	}{
		{name: "duplicate service id", second: domain.ServiceDeployment{
			ServiceID: "orders-worker", Name: "billing-worker", Revision: 1,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "billing-created", Revision: 1}},
		}},
		{name: "duplicate service name", second: domain.ServiceDeployment{
			ServiceID: "billing-worker", Name: "orders-worker", Revision: 1,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "billing-created", Revision: 1}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(71, 0).UTC()})
			_, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
				Name: "orders", EventTypes: []string{"orders.created"},
				Deployments: []domain.ServiceDeployment{first, tt.second},
			})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("SaveDomain() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestDomainConfigurationStoresRulesetAssignmentsWithoutTenantLookup(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(72, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), domain.DomainConfigurationScope, &domain.TenantDomain{
		Name: "orders",
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "orders-worker", Name: "orders-worker", Revision: 1,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 7}},
		}},
	})
	if err != nil {
		t.Fatalf("SaveDomain() error = %v", err)
	}
	if len(item.Deployments) != 1 || len(item.Deployments[0].Rulesets) != 1 ||
		item.Deployments[0].Rulesets[0] != (domain.RulesetAssignment{RulesetID: "orders-created", Revision: 7}) {
		t.Fatalf("service ruleset assignment was not preserved: %#v", item.Deployments)
	}
}

func TestDeploymentRevisionCannotMoveBackwards(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(73, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "orders-worker", Name: "orders-worker", Revision: 4, Active: true,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 7}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	update := *item
	update.Deployments = append([]domain.ServiceDeployment(nil), item.Deployments...)
	update.Deployments[0].Revision = 3
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &update); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("backwards deployment revision error = %v, want ErrInvalidInput", err)
	}
}
