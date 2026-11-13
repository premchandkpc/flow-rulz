package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/brokerstore"
	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/embedded"
	brokerhttp "github.com/flowrulz/flow-rulz/internal/adapters/http/broker"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
)

func TestEmbeddedBrokerHTTPRuleChain(t *testing.T) {
	for _, storage := range []string{"memory", "file"} {
		t.Run(storage, func(t *testing.T) {
			var store broker.Store
			if storage == "file" {
				disk, err := brokerstore.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := disk.Close(); err != nil {
						t.Error(err)
					}
				})
				store = disk
			}
			b, err := broker.New(broker.Config{Partitions: 4, MaxMessages: 100, MaxBytes: 1 << 20, AckWait: time.Second}, store)
			if err != nil {
				t.Fatal(err)
			}
			var revisions []*domain.RuleRevision
			for _, name := range []string{"a.json", "processed.json"} {
				source, err := os.ReadFile("../../examples/direct-worker/rules/" + name)
				if err != nil {
					t.Fatal(err)
				}
				revision, err := rules.NewCompiler(rules.DefaultLimits()).Compile(source)
				if err != nil {
					t.Fatal(err)
				}
				revisions = append(revisions, revision)
			}
			publisher := &embedded.Publisher{Broker: b, Partitions: 4}
			consumer, err := embedded.NewConsumer(b)
			if err != nil {
				t.Fatal(err)
			}
			sender := svceffects.NewRouterWithConfig(publisher, effects.NewNATSDestination(publisher), "events", 4)
			worker, err := application.NewDirectWorker(consumer, rules.NewEvaluator(domain.SystemClock{}), sender, revisions, 10*time.Millisecond, 500*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("embedded worker did not stop")
				}
			}()
			server := httptest.NewServer(brokerhttp.NewHandler(b, "integration-token"))
			defer server.Close()
			client := &http.Client{Timeout: 3 * time.Second}
			event := &domain.EventEnvelope{ID: "embedded-1", Type: "a.created", TenantID: "tenant-a", PartitionKey: "key-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"id":"123"}`)}
			status, _ := brokerRequest(t, client, server.URL, "/v1/broker/events", "tenant-a", "integration-token", event)
			if status != http.StatusAccepted {
				t.Fatalf("publish status=%d", status)
			}
			request := map[string]any{"topic": "commands.b.handle", "group": "service-b", "wait_ms": 1000}
			status, data := brokerRequest(t, client, server.URL, "/v1/broker/consume", "tenant-a", "integration-token", request)
			if status != http.StatusOK {
				t.Fatalf("consume status=%d body=%s", status, data)
			}
			var claim broker.Claim
			if err := json.Unmarshal(data, &claim); err != nil {
				t.Fatal(err)
			}
			var effect domain.Effect
			if err := json.Unmarshal(claim.Record.Payload, &effect); err != nil {
				t.Fatal(err)
			}
			if effect.Destination != "b" || effect.Name != "handle" || effect.TenantID != "tenant-a" || effect.IdempotencyKey == "" {
				t.Fatalf("invalid delivered command: %+v", effect)
			}
			settle := map[string]any{"topic": claim.Record.Topic, "group": claim.Group, "partition": claim.Record.Partition, "receipt": claim.Receipt}
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/ack", "tenant-b", "integration-token", settle)
			if status != http.StatusConflict {
				t.Fatal("cross-tenant ACK was accepted")
			}
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/ack", "tenant-a", "integration-token", settle)
			if status != http.StatusNoContent {
				t.Fatalf("ACK status=%d", status)
			}
			request["wait_ms"] = 0
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/consume", "tenant-a", "integration-token", request)
			if status != http.StatusNoContent {
				t.Fatal("ACKed output redelivered")
			}
			request["group"] = "replay-b"
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/consume", "tenant-a", "integration-token", request)
			if status != http.StatusOK {
				t.Fatal("retained output replay failed")
			}
			request["group"] = "service-b"
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/consume", "tenant-b", "integration-token", request)
			if status != http.StatusNoContent {
				t.Fatal("cross-tenant record exposed")
			}
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/consume", "tenant-a", "wrong-token", request)
			if status != http.StatusUnauthorized {
				t.Fatal("invalid token accepted")
			}
			status, _ = brokerRequest(t, client, server.URL, "/v1/broker/events", "tenant-b", "integration-token", event)
			if status != http.StatusForbidden {
				t.Fatal("cross-tenant publish accepted")
			}
		})
	}
}

func brokerRequest(t *testing.T, client *http.Client, base, path, tenant, token string, value any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Tenant-ID", tenant)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	return resp.StatusCode, body
}
