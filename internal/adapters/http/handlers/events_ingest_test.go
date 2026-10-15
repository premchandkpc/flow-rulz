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

func (p *capturePublisher) Publish(context.Context, string, []byte) error { return nil }
func (p *capturePublisher) PublishBatch(context.Context, []ports.PublishMessage) error { return nil }
func (p *capturePublisher) PublishToShard(_ context.Context, subject string, shard uint32, payload []byte) error {
	p.subject = subject
	p.shard = shard
	p.payload = append([]byte(nil), payload...)
	p.calls++
	return nil
}
func (p *capturePublisher) Close() error { return nil }

func TestEventIngestionPublishesExtensibleMetaEnvelopeToShard(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)

	body := []byte(`{
		"id":"evt-1",
		"type":"order.created",
		"occurred_at":"2026-10-15T07:30:00Z",
		"data":{"order_id":"order-1"},
		"contract":{"name":"order.created","version":"v1","schema_hash":"sha256:abc"},
		"meta":{
			"tenant_id":"tenant-a",
			"partition_key":"order-1",
			"correlation_id":"corr-1",
			"headers":{"traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
			"source":"checkout-api",
			"producer_sequence":1042
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if publisher.calls != 1 { t.Fatalf("publish calls = %d, want 1", publisher.calls) }
	if publisher.subject != "events" { t.Fatalf("subject = %q, want events", publisher.subject) }

	var published map[string]json.RawMessage
	if err := json.Unmarshal(publisher.payload, &published); err != nil { t.Fatal(err) }
	if _, ok := published["tenant_id"]; ok { t.Fatal("tenant_id must not be a top-level event field") }
	if _, ok := published["partition_key"]; ok { t.Fatal("partition_key must not be a top-level event field") }
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(published["meta"], &meta); err != nil { t.Fatalf("meta is invalid: %v", err) }
	for key, want := range map[string]string{"tenant_id":"tenant-a","partition_key":"order-1","correlation_id":"corr-1","source":"checkout-api"} {
		var got string
		if err := json.Unmarshal(meta[key], &got); err != nil || got != want { t.Fatalf("meta.%s = %q, want %q (err=%v)", key, got, want, err) }
	}
	var sequence int
	if err := json.Unmarshal(meta["producer_sequence"], &sequence); err != nil || sequence != 1042 {
		t.Fatalf("meta.producer_sequence = %d, want 1042 (err=%v)", sequence, err)
	}
	var data map[string]any
	if err := json.Unmarshal(published["data"], &data); err != nil || data["order_id"] != "order-1" {
		t.Fatalf("data must remain a JSON object, got %s (err=%v)", published["data"], err)
	}
}

func TestEventIngestionRejectsTenantMismatchInsideMeta(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)

	body := []byte(`{
		"id":"evt-1","type":"order.created","occurred_at":"2026-10-15T07:30:00Z",
		"data":{},
		"meta":{"tenant_id":"tenant-victim","partition_key":"order-1"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-attacker")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden { t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String()) }
	if publisher.calls != 0 { t.Fatalf("publish calls = %d, want 0", publisher.calls) }
}

func TestEventIngestionRequiresPartitionKeyInMeta(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)
	body := []byte(`{"id":"evt-1","type":"order.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest { t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String()) }
	if publisher.calls != 0 { t.Fatalf("publish calls = %d, want 0", publisher.calls) }
}

func TestEventIngestionLegacyTopLevelMetadataIsNormalizedIntoMeta(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	publisher := &capturePublisher{}
	handler := NewEventIngestionHandler(publisher, 32)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := middleware.TenantExtractor(mux)
	body, err := json.Marshal(struct {
		ID string `json:"id"`
		Type string `json:"type"`
		TenantID string `json:"tenant_id"`
		PartitionKey string `json:"partition_key"`
		OccurredAt time.Time `json:"occurred_at"`
		Data json.RawMessage `json:"data"`
	}{ID:"evt-legacy", Type:"order.created", TenantID:"tenant-a", PartitionKey:"order-legacy", OccurredAt:time.Now().UTC(), Data:json.RawMessage(`{}`)})
	if err != nil { t.Fatal(err) }
	req := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted { t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String()) }
	var published map[string]json.RawMessage
	if err := json.Unmarshal(publisher.payload, &published); err != nil { t.Fatal(err) }
	if _, ok := published["tenant_id"]; ok { t.Fatal("legacy top-level tenant_id must be normalized") }
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(published["meta"], &meta); err != nil { t.Fatal(err) }
	var tenant, partition string
	_ = json.Unmarshal(meta["tenant_id"], &tenant)
	_ = json.Unmarshal(meta["partition_key"], &partition)
	if tenant != "tenant-a" || partition != "order-legacy" { t.Fatalf("normalized meta = %#v", meta) }
}
