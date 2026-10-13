package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/domain"
)

type scheduledRepositoryStub struct {
	saveCalls    int
	saved        *domain.ScheduledEvent
	getTenant    string
	getID        string
	listTenant   string
	listLimit    int
	listOffset   int
	cancelTenant string
	cancelID     string
	listResult   []*domain.ScheduledEvent
	cancelErr    error
}

func (s *scheduledRepositoryStub) Save(_ context.Context, evt *domain.ScheduledEvent) error {
	s.saveCalls++
	eventCopy := *evt
	s.saved = &eventCopy
	return nil
}
func (s *scheduledRepositoryStub) GetDue(context.Context, time.Time, int) ([]*domain.ScheduledEvent, error) {
	return nil, nil
}
func (s *scheduledRepositoryStub) Claim(context.Context, []string, string, time.Duration) ([]*domain.ScheduledEvent, error) {
	return nil, nil
}
func (s *scheduledRepositoryStub) MarkReleased(context.Context, string, string, time.Time) error {
	return nil
}
func (s *scheduledRepositoryStub) ScheduleRetry(context.Context, string, string, time.Time, string) error {
	return nil
}
func (s *scheduledRepositoryStub) MarkFailed(context.Context, string, string, string) error {
	return nil
}
func (s *scheduledRepositoryStub) Get(_ context.Context, tenantID, eventID string) (*domain.ScheduledEvent, error) {
	s.getTenant, s.getID = tenantID, eventID
	if tenantID == "tenant-a" && eventID == "event-a" {
		return &domain.ScheduledEvent{EventID: eventID, TenantID: tenantID}, nil
	}
	return nil, nil
}
func (s *scheduledRepositoryStub) List(_ context.Context, tenantID string, limit, offset int) ([]*domain.ScheduledEvent, error) {
	s.listTenant, s.listLimit, s.listOffset = tenantID, limit, offset
	return s.listResult, nil
}
func (s *scheduledRepositoryStub) Cancel(_ context.Context, tenantID, eventID string) error {
	s.cancelTenant, s.cancelID = tenantID, eventID
	return s.cancelErr
}

func TestScheduleEventRejectsTenantMismatch(t *testing.T) {
	repo := &scheduledRepositoryStub{}
	handler := NewScheduledHandler(repo)
	req := httptest.NewRequest(http.MethodPost, "/v1/scheduled-events", strings.NewReader(`{"tenant_id":"tenant-b","event_type":"invoice.created","partition_key":"invoice-1","scheduled_at":"2030-01-01T00:00:00Z"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.ScheduleEvent)).ServeHTTP(response, req)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
	}
	if repo.saveCalls != 0 {
		t.Fatalf("Save calls = %d, want 0", repo.saveCalls)
	}
}

func TestListScheduledEventsUsesTenantAndPagination(t *testing.T) {
	repo := &scheduledRepositoryStub{listResult: []*domain.ScheduledEvent{{EventID: "event-a", TenantID: "tenant-a"}}}
	handler := NewScheduledHandler(repo)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/scheduled-events?limit=25&offset=50", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if repo.listTenant != "tenant-a" || repo.listLimit != 25 || repo.listOffset != 50 {
		t.Fatalf("List args = tenant %q limit %d offset %d", repo.listTenant, repo.listLimit, repo.listOffset)
	}
}

func TestGetScheduledEventIsTenantScoped(t *testing.T) {
	repo := &scheduledRepositoryStub{}
	handler := NewScheduledHandler(repo)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/scheduled-events/event-a", nil)
	req.Header.Set("X-Tenant-ID", "tenant-b")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if repo.getTenant != "tenant-b" || repo.getID != "event-a" {
		t.Fatalf("Get args = tenant %q id %q", repo.getTenant, repo.getID)
	}
}

func TestCancelScheduledEventIsTenantScoped(t *testing.T) {
	repo := &scheduledRepositoryStub{}
	handler := NewScheduledHandler(repo)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodDelete, "/v1/scheduled-events/event-a", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusNoContent, response.Body.String())
	}
	if repo.cancelTenant != "tenant-a" || repo.cancelID != "event-a" {
		t.Fatalf("Cancel args = tenant %q id %q", repo.cancelTenant, repo.cancelID)
	}
}

func TestCancelScheduledEventReturnsConflictWhenNotPending(t *testing.T) {
	repo := &scheduledRepositoryStub{cancelErr: errors.New("not pending")}
	handler := NewScheduledHandler(repo)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodDelete, "/v1/scheduled-events/event-a", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
}
