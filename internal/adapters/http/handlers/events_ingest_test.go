package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type capturePublisher struct {
	subject string
	shard   uint32
	payload []byte
	calls   int
}

func (p *capturePublisher) Publish(context.Context, string, []byte) error              { return nil }
func (p *capturePublisher) PublishBatch(context.Context, []ports.PublishMessage) error { return nil }
func (p *capturePublisher) PublishToShard(_ context.Context, subject string, shard uint32, payload []byte) error {
	p.subject = subject
	p.shard = shard
	p.payload = append([]byte(nil), payload...)
	p.calls++
	return nil
}
func (p *capturePublisher) Close() error { return nil }

func TestEventIngestionPublishesValidatedEnvelopeToShard(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)

	body, err := json.Marshal(struct {
		ID           string            `json:"id"`
		Type         string            `json:"type"`
		OccurredAt time.Time       `json:"occurred_at"`
		Data       json.RawMessage `json:"data"`
		Meta       map[string]any  `json:"meta"`
	}{
		ID: "evt-1", Type: "order.created", OccurredAt: time.Now().UTC(),
		Data: json.RawMessage(`{"order_id":"order-1"}`),
		Meta: map[string]any{"tenant_id":"tenant-a", "partition_key":"order-1", "traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", "producer":"test"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.calls)
	}
	if publisher.subject != "events" {
		t.Fatalf("subject = %q, want events", publisher.subject)
	}
	var published struct {
		ID string `json:"id"`
		Type string `json:"type"`
		Meta map[string]json.RawMessage `json:"meta"`
	}
	if err := json.Unmarshal(publisher.payload, &published); err != nil {
		t.Fatal(err)
	}
	var tenantID, partitionKey, producer string
	_ = json.Unmarshal(published.Meta["tenant_id"], &tenantID)
	_ = json.Unmarshal(published.Meta["partition_key"], &partitionKey)
	_ = json.Unmarshal(published.Meta["producer"], &producer)
	if published.ID != "evt-1" || tenantID != "tenant-a" || partitionKey != "order-1" || producer != "test" || published.Type != "order.created" {
		t.Fatalf("published envelope = %+v", published)
	}
}

func TestEventIngestionRejectsTenantMismatch(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)

	body, err := json.Marshal(struct {
		ID           string    `json:"id"`
		Type         string    `json:"type"`
		OccurredAt time.Time       `json:"occurred_at"`
		Data       json.RawMessage `json:"data"`
		Meta       map[string]any  `json:"meta"`
	}{
		ID: "evt-1", Type: "order.created", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{}`),
		Meta: map[string]any{"tenant_id":"tenant-victim", "partition_key":"order-1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-attacker")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.calls)
	}
}

func TestEventIngestionRejectsInvalidKnownMetaTypes(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)
	body := `{
		"id":"evt-invalid-meta","type":"order.created",
		"occurred_at":"2026-10-15T07:30:00Z","data":{},
		"meta":{"tenant_id":"tenant-a","partition_key":"order-1","headers":["not","an","object"],"future_field":{"allowed":true}}
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.calls)
	}
}
