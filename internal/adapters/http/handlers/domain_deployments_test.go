package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type deploymentCaptureUseCase struct {
	ports.DomainServiceUseCase
	called   bool
	scope string
	item     *domain.TenantDomain
}

func (s *deploymentCaptureUseCase) SaveDomain(_ context.Context, tenantID string, item *domain.TenantDomain) (*domain.TenantDomain, error) {
	s.called = true
	s.scope = tenantID
	cloned := *item
	s.item = &cloned
	return item, nil
}

func TestCreateDomainDecodesAndReturnsServiceDeploymentAssignments(t *testing.T) {
	service := &deploymentCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	body := `{
		"name":"orders",
		"services":[{
			"service_id":"order-worker",
			"name":"order-worker",
			"revision":3,
			"active":true,
			"rulesets":[{"ruleset_id":"order-created","revision":7}],
			"worker_profile":{"min_replicas":1,"max_replicas":4,"concurrency":8}
		}]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)

	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s; want %d", res.Code, res.Body.String(), http.StatusCreated)
	}
	if !service.called || service.scope != domainConfigurationScope {
		t.Fatalf("SaveDomain called=%t scope=%q", service.called, service.scope)
	}
	if len(service.item.Deployments) != 1 {
		t.Fatalf("deployment assignments = %#v", service.item.Deployments)
	}
	deployment := service.item.Deployments[0]
	if deployment.ServiceID != "order-worker" || deployment.Revision != 3 ||
		len(deployment.Rulesets) != 1 || deployment.Rulesets[0].Revision != 7 ||
		deployment.WorkerProfile.Concurrency != 8 {
		t.Fatalf("decoded deployment differs from request: %#v", deployment)
	}

	var response map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if _, exists := response["tenant_id"]; exists {
		t.Fatal("domain API must not expose tenant_id")
	}
	if _, exists := response["event_types"]; exists {
		t.Fatal("domain API must not expose event_types configuration")
	}
	if _, exists := response["services"]; !exists {
		t.Fatal("domain API response must expose nested services")
	}
}

func TestCreateDomainDoesNotRequireTenantConfiguration(t *testing.T) {
	service := &deploymentCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(`{"name":"orders","services":[]}`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s; want created without tenant configuration", res.Code, res.Body.String())
	}
	if !service.called {
		t.Fatal("SaveDomain must be called without requiring tenant configuration")
	}
}

func TestCreateDomainRejectsMalformedDeploymentJSON(t *testing.T) {
	service := &deploymentCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(`{"name":"orders","services":[`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s; want bad request", res.Code, res.Body.String())
	}
	if service.called {
		t.Fatal("SaveDomain must not be called for malformed JSON")
	}
}
