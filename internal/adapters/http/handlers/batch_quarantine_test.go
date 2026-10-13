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

type batchServiceStub struct {
	ports.BatchService
	createCalls int
	getTenant   string
	getBatchID  string
	createErr   error
	batch       *domain.BatchRun
}

func (s *batchServiceStub) CreateBatch(context.Context, string, string, string, []string) (*domain.BatchRun, error) {
	s.createCalls++
	return s.batch, s.createErr
}

func (s *batchServiceStub) GetBatch(_ context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	s.getTenant, s.getBatchID = tenantID, batchID
	if s.batch != nil && s.batch.TenantID == tenantID && s.batch.BatchID == batchID {
		return s.batch, nil
	}
	return nil, nil
}

func (s *batchServiceStub) ListBatches(context.Context, string, string, int, int) ([]*domain.BatchRun, error) {
	return nil, nil
}

func TestCreateBatchRejectsTenantMismatch(t *testing.T) {
	service := &batchServiceStub{}
	handler := NewBatchHandler(service)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", strings.NewReader(
		`{"tenant_id":"tenant-b","partition_key":"order-1","rule_set":"order.created","event_ids":["event-1"]}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.CreateBatch)).ServeHTTP(response, req)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
	}
	if service.createCalls != 0 {
		t.Fatalf("CreateBatch calls = %d, want 0", service.createCalls)
	}
}

func TestCreateBatchReturnsUnavailableWhenProcessingIsNotConfigured(t *testing.T) {
	service := &batchServiceStub{createErr: domain.ErrBatchProcessingUnavailable}
	handler := NewBatchHandler(service)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", strings.NewReader(
		`{"partition_key":"order-1","rule_set":"order.created","event_ids":["event-1"]}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.CreateBatch)).ServeHTTP(response, req)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusServiceUnavailable, response.Body.String())
	}
}

func TestGetBatchPassesTenantToService(t *testing.T) {
	service := &batchServiceStub{batch: &domain.BatchRun{BatchID: "batch-1", TenantID: "tenant-a"}}
	handler := NewBatchHandler(service)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/batches/batch-1", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(mux).ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if service.getTenant != "tenant-a" || service.getBatchID != "batch-1" {
		t.Fatalf("GetBatch args = tenant %q id %q", service.getTenant, service.getBatchID)
	}
}
