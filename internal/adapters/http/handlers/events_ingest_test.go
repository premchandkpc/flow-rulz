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
		Meta         map[string]any     `json:"meta"`
		OccurredAt   time.Time         `json:"occurred_at"`
		Data         []byte            `json:"data"`
		Headers      map[string]string `json:"headers,omitempty"`
	}{
		ID: "evt-1", Type: "order.created", Meta: map[string]any{"tenant_id": "tenant-a", "partition_key": "order-1", "region": "ap-south-1"}, OccurredAt: time.Now().UTC(),
		Data: []byte(`{"order_id":"order-1"}`),
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
		Meta map[string]any `json:"meta"`
	}
	if err := json.Unmarshal(publisher.payload, &published); err != nil {
		t.Fatal(err)
	}
	if published.ID != "evt-1" || published.Meta["tenant_id"] != "tenant-a" || published.Meta["partition_key"] != "order-1" || published.Meta["region"] != "ap-south-1" || published.Type != "order.created" {
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
		Meta         map[string]any `json:"meta"`
		OccurredAt   time.Time `json:"occurred_at"`
		Data         []byte    `json:"data"`
	}{
		ID: "evt-1", Type: "order.created", Meta: map[string]any{"tenant_id": "tenant-victim", "partition_key": "order-1"}, OccurredAt: time.Now().UTC(), Data: []byte(`{}`),
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

func TestEventIngestionRejectsMissingOrInvalidMetaIdentity(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	tests := []struct {
		name string
		meta map[string]any
	}{
		{name: "missing tenant", meta: map[string]any{"partition_key": "order-1"}},
		{name: "missing partition", meta: map[string]any{"tenant_id": "tenant-a"}},
		{name: "tenant has wrong type", meta: map[string]any{"tenant_id": 42, "partition_key": "order-1"}},
		{name: "partition has wrong type", meta: map[string]any{"tenant_id": "tenant-a", "partition_key": false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &capturePublisher{}
			handler := NewEventIngestionHandler(publisher, 32)
			mux := http.NewServeMux()
			handler.RegisterRoutes(mux)
			server := middleware.TenantExtractor(mux)
			body, err := json.Marshal(map[string]any{
				"id": "evt-1", "type": "order.created", "occurred_at": time.Now().UTC(),
				"data": map[string]any{"order_id": "order-1"}, "meta": tt.meta,
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
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
		})
	}
}
