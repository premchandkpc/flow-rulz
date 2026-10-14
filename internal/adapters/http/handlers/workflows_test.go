package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type workflowServiceStub struct {
	ports.WorkflowService
	createCalls      int
	getTenant        string
	getID            string
	getCalls         int
	transitionTenant string
	transitionID     string
	transitionFrom   string
	transitionTo     string
	updateTenant     string
	updateID         string
	updateState      string
}

func (s *workflowServiceStub) Get(_ context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error) {
	s.getCalls++
	s.getTenant, s.getID = tenantID, workflowID
	return &domain.WorkflowInstance{WorkflowID: workflowID, TenantID: tenantID, State: domain.WorkflowStatusPending, Version: 1}, nil
}

func (s *workflowServiceStub) Transition(_ context.Context, tenantID, workflowID, fromState, toState string) error {
	s.transitionTenant, s.transitionID = tenantID, workflowID
	s.transitionFrom, s.transitionTo = fromState, toState
	return nil
}

func (s *workflowServiceStub) UpdateState(_ context.Context, tenantID, workflowID, newState string, _ int64) error {
	s.updateTenant, s.updateID, s.updateState = tenantID, workflowID, newState
	return nil
}

func (s *workflowServiceStub) Create(context.Context, *domain.WorkflowInstance) error {
	s.createCalls++
	return nil
}

func TestCreateWorkflowRejectsTenantMismatch(t *testing.T) {
	service := &workflowServiceStub{}
	handler := NewWorkflowHandler(service, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(
		`{"tenant_id":"tenant-b","workflow_type":"order","state":"pending"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.CreateWorkflow)).ServeHTTP(response, req)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
	}
	if service.createCalls != 0 {
		t.Fatalf("Create calls = %d, want 0", service.createCalls)
	}
}

func TestWorkflowDefinitionMutationRoutesAreNotExposed(t *testing.T) {
	mux := http.NewServeMux()
	NewWorkflowHandler(&workflowServiceStub{}, nil, nil).RegisterAllRoutes(mux)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{name: "create is not exposed", method: http.MethodPost, path: "/v1/workflows/definitions", want: http.StatusMethodNotAllowed},
		{name: "delete is not exposed", method: http.MethodDelete, path: "/v1/workflows/definitions/order/1", want: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			mux.ServeHTTP(res, httptest.NewRequest(tt.method, tt.path, nil))
			if res.Code != tt.want {
				t.Fatalf("status = %d, want %d", res.Code, tt.want)
			}
		})
	}
}

func TestGetWorkflowPassesTenantToService(t *testing.T) {
	service := &workflowServiceStub{}
	handler := NewWorkflowHandler(service, nil, nil)
	mux := http.NewServeMux()
	handler.RegisterAllRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if service.getTenant != "tenant-a" || service.getID != "wf-1" {
		t.Fatalf("Get args = tenant %q workflow %q", service.getTenant, service.getID)
	}
}

func TestTransitionWorkflowPassesTenantToService(t *testing.T) {
	service := &workflowServiceStub{}
	handler := NewWorkflowHandler(service, nil, nil)
	mux := http.NewServeMux()
	handler.RegisterAllRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/wf-1/transition", strings.NewReader(
		`{"from_state":"pending","to_state":"completed"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if service.getTenant != "tenant-a" || service.transitionTenant != "tenant-a" ||
		service.transitionID != "wf-1" || service.transitionFrom != "pending" || service.transitionTo != "completed" {
		t.Fatalf("Get tenant %q, transition tenant %q workflow %q from %q to %q",
			service.getTenant, service.transitionTenant, service.transitionID, service.transitionFrom, service.transitionTo)
	}
}

type workflowDefinitionRepositoryStub struct {
	ports.WorkflowDefinitionRepository
	gotType    string
	gotVersion int64
}

func (s *workflowDefinitionRepositoryStub) Get(_ context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	s.gotType, s.gotVersion = workflowType, version
	return &domain.WorkflowDefinition{WorkflowType: workflowType, Version: version}, nil
}

func (s *workflowDefinitionRepositoryStub) List(context.Context) ([]*domain.WorkflowDefinition, error) {
	return nil, nil
}

func TestGetWorkflowDefinitionSupportsVersionedRoute(t *testing.T) {
	definitions := &workflowDefinitionRepositoryStub{}
	handler := NewWorkflowHandler(&workflowServiceStub{}, nil, definitions)
	mux := http.NewServeMux()
	handler.RegisterAllRoutes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/workflows/definitions/orders/3", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if definitions.gotType != "orders" || definitions.gotVersion != 3 {
		t.Fatalf("Get called with (%q, %d), want (%q, %d)", definitions.gotType, definitions.gotVersion, "orders", 3)
	}
}
