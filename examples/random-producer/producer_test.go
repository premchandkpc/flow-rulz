package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

var supportedEventTypes = map[string]struct{}{
	"order.created":         {},
	"transaction.initiated": {},
	"order.placed":          {},
}

type fakePublisher struct {
	mu       sync.Mutex
	subjects []string
	events   []domain.EventEnvelope
	fail     bool
}

func (p *fakePublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.fail {
		return errors.New("broker unavailable")
	}
	var event domain.EventEnvelope
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	p.mu.Lock()
	p.subjects = append(p.subjects, subject)
	p.events = append(p.events, event)
	p.mu.Unlock()
	return nil
}

func validConfig() Config {
	return Config{Clients: 3, MaxMessagesPerClient: 4, NumShards: 64, Concurrency: 3, BaseSubject: DefaultBaseSubject, TenantID: DefaultTenantID, Scenario: ScenarioAll}
}

func TestRunPublishesValidEventsToDomainComputedShard(t *testing.T) {
	publisher := &fakePublisher{}
	summary, err := Run(context.Background(), validConfig(), publisher)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Attempted < 3 || summary.Attempted > 12 {
		t.Fatalf("attempted = %d, want 3..12", summary.Attempted)
	}
	if summary.BrokerAcked != summary.Attempted || summary.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if len(publisher.events) != summary.Attempted {
		t.Fatalf("published events = %d, attempted = %d", len(publisher.events), summary.Attempted)
	}
	for i, event := range publisher.events {
		if err := event.Validate(); err != nil {
			t.Fatalf("event %d invalid: %v", i, err)
		}
		expected := fmt.Sprintf("events.shard.%d", event.VirtualShard(validConfig().NumShards))
		if publisher.subjects[i] != expected {
			t.Errorf("subject = %q, want %q", publisher.subjects[i], expected)
		}
		if event.TenantID != DefaultTenantID {
			t.Errorf("tenant ID = %q, want shared example tenant %q", event.TenantID, DefaultTenantID)
		}
		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Errorf("event data is not valid JSON: %v", err)
		}
		if _, ok := supportedEventTypes[event.Type]; !ok {
			t.Errorf("event type %q is not a supported domain contract", event.Type)
		}
	}
}

func TestRunCountsPublishFailures(t *testing.T) {
	publisher := &fakePublisher{fail: true}
	summary, err := Run(context.Background(), validConfig(), publisher)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Attempted == 0 || summary.Failed != summary.Attempted || summary.BrokerAcked != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"zero clients", func(c *Config) { c.Clients = 0 }, "clients"},
		{"too many clients", func(c *Config) { c.Clients = maxClients + 1 }, "clients"},
		{"zero max messages", func(c *Config) { c.MaxMessagesPerClient = 0 }, "max messages"},
		{"zero shards", func(c *Config) { c.NumShards = 0 }, "shards"},
		{"zero concurrency", func(c *Config) { c.Concurrency = 0 }, "concurrency"},
		{"missing subject", func(c *Config) { c.BaseSubject = "" }, "base subject"},
		{"missing tenant", func(c *Config) { c.TenantID = "" }, "tenant ID"},
		{"unknown scenario", func(c *Config) { c.Scenario = Scenario("unknown") }, "unsupported scenario"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.edit(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestRunRejectsNilPublisher(t *testing.T) {
	_, err := Run(context.Background(), validConfig(), nil)
	if err == nil || !strings.Contains(err.Error(), "publisher") {
		t.Fatalf("Run() error = %v, want publisher error", err)
	}
}

func TestRunHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	summary, err := Run(ctx, validConfig(), &fakePublisher{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if summary.Attempted != summary.BrokerAcked+summary.Failed {
		t.Fatalf("summary counts do not reconcile: %+v", summary)
	}
}
