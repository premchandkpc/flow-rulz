package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type traceInboxStub struct {
	ports.InboxRepository
	tenantID string
	eventID  string
	lookups  []string
	entry    *domain.InboxEntry
}

func (s *traceInboxStub) Get(_ context.Context, tenantID, eventID string) (*domain.InboxEntry, error) {
	s.tenantID, s.eventID = tenantID, eventID
	s.lookups = append(s.lookups, tenantID+"/"+eventID)
	if tenantID != "tenant-a" || eventID != "root-event" {
		return nil, nil
	}
	return s.entry, nil
}

type traceExecutionStub struct {
	ports.ExecutionRepository
	execution *domain.Execution
}

func (s *traceExecutionStub) Get(_ context.Context, executionID string) (*domain.Execution, error) {
	if s.execution != nil && executionID == s.execution.ID {
		return s.execution, nil
	}
	return nil, nil
}

type traceOutboxStub struct {
	ports.OutboxRepository
	effects []domain.OutboxEffect
}

func (s *traceOutboxStub) GetByExecution(_ context.Context, _ string) ([]domain.OutboxEffect, error) {
	return s.effects, nil
}

func TestEventTraceReturnsTenantScopedChildFlow(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	child := domain.EventEnvelope{
		ID: "child-event", Type: "payment.requested", TenantID: "tenant-a",
		PartitionKey: "customer-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-1"}`),
	}
	childPayload, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	inbox := &traceInboxStub{entry: &domain.InboxEntry{
		TenantID: "tenant-a", EventID: "root-event", ExecutionID: "execution-1",
		Status: domain.InboxStatusCommitted, RuleSet: "order.created", PartitionKey: "customer-1",
	}}
	executions := &traceExecutionStub{execution: &domain.Execution{
		ID: "execution-1", EventID: "root-event", TenantID: "tenant-a",
		RuleSet: "order.created", Revision: 2, Status: domain.ExecutionStatusCompleted,
		CreatedAt: time.Now().UTC(),
	}}
	outbox := &traceOutboxStub{effects: []domain.OutboxEffect{{
		ID: "effect-1", ExecutionID: "execution-1", TenantID: "tenant-a",
		EffectType: domain.EffectTypeEmitEvent, Name: child.Type,
		Destination: "events." + child.Type, Status: domain.OutboxStatusDelivered,
		Payload: childPayload,
	}}}

	mux := http.NewServeMux()
	NewEventTraceHandler(inbox, executions, outbox, 32).RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/events/root-event/trace", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Data EventTrace `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	trace := response.Data
	if len(inbox.lookups) != 2 || inbox.lookups[0] != "tenant-a/root-event" || inbox.lookups[1] != "tenant-a/child-event" {
		t.Fatalf("inbox lookups = %v", inbox.lookups)
	}
	if trace.Status != string(domain.ExecutionStatusCompleted) || trace.Type != "order.created" ||
		trace.PartitionKey != "customer-1" || trace.Shard != child.VirtualShard(32) || len(trace.Effects) != 1 {
		t.Fatalf("trace = %+v", trace)
	}
	effect := trace.Effects[0]
	if effect.Destination != "events.payment.requested" || effect.Status != domain.OutboxStatusDelivered {
		t.Fatalf("effect = %+v", effect)
	}
	if effect.Child == nil || effect.Child.EventID != child.ID || effect.Child.Type != child.Type || effect.Child.Status != "queued" {
		t.Fatalf("child trace = %+v", effect.Child)
	}
	if effect.Child.Shard != child.VirtualShard(32) {
		t.Fatalf("child shard = %d, want %d", effect.Child.Shard, child.VirtualShard(32))
	}
}

func TestEventTraceDoesNotReadAnotherTenantInbox(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	inbox := &traceInboxStub{entry: &domain.InboxEntry{
		TenantID: "tenant-a", EventID: "root-event", ExecutionID: "execution-1",
		Status: domain.InboxStatusCommitted,
	}}
	mux := http.NewServeMux()
	NewEventTraceHandler(inbox, &traceExecutionStub{}, &traceOutboxStub{}, 32).RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)
	req := httptest.NewRequest(http.MethodGet, "/v1/events/root-event/trace", nil)
	req.Header.Set("X-Tenant-ID", "tenant-b")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if inbox.tenantID != "tenant-b" || inbox.eventID != "root-event" {
		t.Fatalf("inbox lookup used tenant=%q event=%q", inbox.tenantID, inbox.eventID)
	}
	var response struct {
		Data EventTrace `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.Execution != nil || len(response.Data.Effects) != 0 {
		t.Fatalf("trace exposed another tenant's execution: %+v", response.Data)
	}
}
