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
	called bool
	scope  string
	item   *domain.TenantDomain
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
	if !service.called || service.scope != domain.DomainConfigurationScope {
		t.Fatalf("SaveDomain called=%t scope=%q", service.called, service.scope)
	}
	if len(service.item.Deployments) != 1 {
		t.Fatalf("service assignments = %#v", service.item.Deployments)
	}
	if len(service.item.EventTypes) != 0 {
		t.Fatalf("caller-controlled event_types were accepted as configuration: %#v", service.item.EventTypes)
	}
	deployment := service.item.Deployments[0]
	if deployment.ServiceID != "order-worker" || deployment.Revision != 3 ||
		len(deployment.Rulesets) != 1 || deployment.Rulesets[0].Revision != 7 ||
		deployment.WorkerProfile.Concurrency != 8 {
		t.Fatalf("decoded deployment differs from request: %#v", deployment)
	}

	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	response := envelope.Data
	if _, exists := response["tenant_id"]; exists {
		t.Fatal("domain API must not expose tenant_id")
	}
	if _, exists := response["event_types"]; exists {
		t.Fatal("domain API must not expose event_types configuration")
	}
	if _, exists := response["partition_key"]; exists {
		t.Fatal("domain API must not expose partition_key configuration")
	}
	if _, exists := response["services"]; !exists {
		t.Fatal("domain API response must expose nested services")
	}
}

func TestCreateDomainRejectsTenantAndRuntimeIdentityFields(t *testing.T) {
	service := &deploymentCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	body := `{"name":"orders","tenant_id":"caller","event_types":["orders.created"],"partition_key":"order-1","services":[{"service_id":"orders-worker","name":"orders-worker","revision":1,"active":true,"rulesets":[{"ruleset_id":"orders-created","revision":1}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s; want bad request for runtime fields in config", res.Code, res.Body.String())
	}
	if service.called {
		t.Fatal("SaveDomain must not be called when tenant/event identity fields are supplied")
	}
}

func TestCreateDomainDoesNotRequireTenantConfiguration(t *testing.T) {
	service := &deploymentCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(`{"name":"orders","services":[{"service_id":"orders-worker","name":"orders-worker","revision":1,"active":true,"rulesets":[{"ruleset_id":"orders-created","revision":1}]}]}`))
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

type subscriptionCaptureUseCase struct {
	ports.DomainServiceUseCase
	called bool
	scope  string
	item   *domain.DomainService
}

func (s *subscriptionCaptureUseCase) SaveService(_ context.Context, scope string, item *domain.DomainService) (*domain.DomainService, error) {
	s.called = true
	s.scope = scope
	cloned := *item
	cloned.EventTypes = append([]string(nil), item.EventTypes...)
	s.item = &cloned
	return item, nil
}

func TestCreateSubscriptionUsesEventTypesWithoutTenantConfiguration(t *testing.T) {
	service := &subscriptionCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/subscriptions", strings.NewReader(`{"name":"audit-consumer","event_types":["orders.created","payments.completed"]}`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s; want created", res.Code, res.Body.String())
	}
	if !service.called || service.scope != domain.DomainConfigurationScope {
		t.Fatalf("SaveService called=%t scope=%q", service.called, service.scope)
	}
	if len(service.item.EventTypes) != 2 || service.item.EventTypes[0] != "orders.created" {
		t.Fatalf("subscription filters = %#v", service.item.EventTypes)
	}
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	response := envelope.Data
	if _, exists := response["tenant_id"]; exists {
		t.Fatal("subscription API must not expose tenant_id")
	}
}

func TestLegacyStandaloneServiceRouteIsNotRegistered(t *testing.T) {
	handler := NewDomainServiceHandler(&deploymentCaptureUseCase{})
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/v1/services", nil))
	if pattern != "" {
		t.Fatalf("legacy /v1/services route pattern = %q, want no route", pattern)
	}
	_, pattern = mux.Handler(httptest.NewRequest(http.MethodGet, "/v1/subscriptions", nil))
	if pattern == "" {
		t.Fatal("/v1/subscriptions route must remain available for generic subscriber routing")
	}
}

func TestCreateSubscriptionRejectsTenantConfiguration(t *testing.T) {
	service := &subscriptionCaptureUseCase{}
	handler := NewDomainServiceHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/subscriptions", strings.NewReader(`{"name":"audit-consumer","tenant_id":"caller-controlled","event_types":["orders.created"]}`))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s; want bad request", res.Code, res.Body.String())
	}
	if service.called {
		t.Fatal("SaveService must not be called when tenant configuration is supplied")
	}
}

func TestCreateDomainRejectsRuntimeIdentityFields(t *testing.T) {
	for _, field := range []string{"tenant_id", "partition_key", "event_types"} {
		t.Run(field, func(t *testing.T) {
			service := &deploymentCaptureUseCase{}
			handler := NewDomainServiceHandler(service)
			mux := http.NewServeMux()
			handler.RegisterRoutes(mux)
			body := `{"name":"orders","services":[],"FIELD":"unexpected"}`
			body = strings.Replace(body, "FIELD", field, 1)
			req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(body))
			res := httptest.NewRecorder()
			mux.ServeHTTP(res, req)
			if res.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s; want bad request for config field %q", res.Code, res.Body.String(), field)
			}
			if service.called {
				t.Fatal("SaveDomain must not be called when runtime-only fields are sent as configuration")
			}
		})
	}
}

func TestCreateDomainRejectsServerManagedFields(t *testing.T) {
	for _, body := range []string{
		`{"id":"caller-id","name":"orders","services":[]}`,
		`{"name":"orders","created_at":"2026-10-15T00:00:00Z","services":[]}`,
		`{"name":"orders","updated_at":"2026-10-15T00:00:00Z","services":[]}`,
	} {
		service := &deploymentCaptureUseCase{}
		handler := NewDomainServiceHandler(service)
		mux := http.NewServeMux()
		handler.RegisterRoutes(mux)
		req := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(body))
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, want bad request", body, res.Code)
		}
		if service.called {
			t.Fatalf("SaveDomain called for server-managed input: %s", body)
		}
	}
}
