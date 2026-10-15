package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type ruleServiceStub struct {
	ports.RuleService
	tenantScope string
	ruleSet     string
	source      []byte
	actor       string
	calls       int
}

func (s *ruleServiceStub) CompileAndActivate(_ context.Context, tenantScope, ruleSet string, source []byte, actor string) (*domain.RuleActivation, error) {
	s.tenantScope = tenantScope
	s.ruleSet = ruleSet
	s.source = append([]byte(nil), source...)
	s.actor = actor
	s.calls++
	return &domain.RuleActivation{TenantScope: tenantScope, RuleSet: ruleSet, Revision: 1}, nil
}

func TestCreateRevisionAcceptsYAMLAndNormalizesToJSON(t *testing.T) {
	service := &ruleServiceStub{}
	handler := NewRuleHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `rule_set: order.created
revision: 1
mode: first_match
rules:
  - id: high-value
    priority: 10
    when:
      path: $.total
      op: gte
      value: 100
    then:
      - emit_event:
          type: payment.requested
          partition_key_policy: inherit
          data:
            amount: 100
`
	request := httptest.NewRequest(http.MethodPost, "/v1/rules/order.created/revisions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/yaml")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.calls != 1 {
		t.Fatalf("CompileAndActivate calls = %d, want 1", service.calls)
	}
	if service.tenantScope != domain.DomainConfigurationScope || service.ruleSet != "order.created" || service.actor != "api" {
		t.Fatalf("unexpected call metadata: scope=%q rule=%q actor=%q", service.tenantScope, service.ruleSet, service.actor)
	}
	var document map[string]any
	if err := json.Unmarshal(service.source, &document); err != nil {
		t.Fatalf("normalized source is not JSON: %v", err)
	}
	if document["rule_set"] != "order.created" {
		t.Fatalf("normalized rule_set = %v", document["rule_set"])
	}
}

func TestCreateRevisionRejectsMismatchedRuleSet(t *testing.T) {
	service := &ruleServiceStub{}
	handler := NewRuleHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"rule_set":"another.rule","revision":1,"mode":"first_match","rules":[]}`
	request := httptest.NewRequest(http.MethodPost, "/v1/rules/order.created/revisions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body = %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
	if service.calls != 0 {
		t.Fatalf("CompileAndActivate calls = %d, want 0", service.calls)
	}
}

func TestCreateRevisionRejectsMultipleYAMLDocuments(t *testing.T) {
	service := &ruleServiceStub{}
	handler := NewRuleHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := "rule_set: order.created\nrevision: 1\n---\nrule_set: another.rule\nrevision: 2\n"
	request := httptest.NewRequest(http.MethodPost, "/v1/rules/order.created/revisions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/yaml")
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body = %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
	if service.calls != 0 {
		t.Fatalf("CompileAndActivate calls = %d, want 0", service.calls)
	}
}
