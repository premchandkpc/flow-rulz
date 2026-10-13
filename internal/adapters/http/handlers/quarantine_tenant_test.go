package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type quarantineServiceStub struct {
	ports.QuarantineService
	entry        *domain.QuarantineEntry
	getTenant    string
	getID        string
	replayTenant string
	replayID     string
	deleteTenant string
	deleteID     string
	getCalls     int
	replayCalls  int
	deleteCalls  int
}

func (s *quarantineServiceStub) Get(_ context.Context, tenantID, id string) (*domain.QuarantineEntry, error) {
	s.getCalls++
	s.getTenant, s.getID = tenantID, id
	if s.entry == nil || s.entry.TenantID != tenantID || s.entry.ID != id {
		return nil, nil
	}
	return s.entry, nil
}

func (s *quarantineServiceStub) Replay(_ context.Context, tenantID, id string) error {
	s.replayCalls++
	s.replayTenant, s.replayID = tenantID, id
	return nil
}

func (s *quarantineServiceStub) Delete(_ context.Context, tenantID, id string) error {
	s.deleteCalls++
	s.deleteTenant, s.deleteID = tenantID, id
	return nil
}

func quarantineRequest(t *testing.T, method, path string, service *quarantineServiceStub) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	NewQuarantineHandler(service).RegisterRoutes(mux)
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	res := httptest.NewRecorder()
	middleware.TenantExtractor(mux).ServeHTTP(res, req)
	return res
}

func TestGetQuarantineScopesLookupToRequestTenant(t *testing.T) {
	service := &quarantineServiceStub{
		entry: &domain.QuarantineEntry{ID: "q-1", TenantID: "tenant-a"},
	}
	res := quarantineRequest(t, http.MethodGet, "/v1/quarantine/q-1", service)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", res.Code, http.StatusOK, res.Body.String())
	}
	if service.getTenant != "tenant-a" || service.getID != "q-1" {
		t.Fatalf("Get args = tenant %q id %q", service.getTenant, service.getID)
	}
}

func TestReplayQuarantineUsesTenantScopedMutation(t *testing.T) {
	service := &quarantineServiceStub{}
	res := quarantineRequest(t, http.MethodPost, "/v1/quarantine/q-1/replay", service)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", res.Code, http.StatusOK, res.Body.String())
	}
	if service.replayTenant != "tenant-a" || service.replayID != "q-1" || service.replayCalls != 1 {
		t.Fatalf("Replay args = tenant %q id %q calls %d", service.replayTenant, service.replayID, service.replayCalls)
	}
	if service.getCalls != 0 {
		t.Fatalf("Replay performed a separate unscoped pre-read %d times", service.getCalls)
	}
}

func TestDeleteQuarantineUsesTenantScopedMutation(t *testing.T) {
	service := &quarantineServiceStub{}
	res := quarantineRequest(t, http.MethodDelete, "/v1/quarantine/q-1", service)
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body=%s", res.Code, http.StatusNoContent, res.Body.String())
	}
	if service.deleteTenant != "tenant-a" || service.deleteID != "q-1" || service.deleteCalls != 1 {
		t.Fatalf("Delete args = tenant %q id %q calls %d", service.deleteTenant, service.deleteID, service.deleteCalls)
	}
	if service.getCalls != 0 {
		t.Fatalf("Delete performed a separate unscoped pre-read %d times", service.getCalls)
	}
}
