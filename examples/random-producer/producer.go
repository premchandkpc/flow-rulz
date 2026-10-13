// Package main contains the opt-in domain-aware producer example.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

const (
	minClients     = 1
	maxClients     = 1000
	minMessages    = 1
	maxMessages    = 100
	minConcurrency = 1
	maxConcurrency = 256
	// DefaultBaseSubject is the base subject used by FlowRulz event producers.
	DefaultBaseSubject = "events"
	DefaultTenantID    = "acme-corp"
)

// Publisher is the smallest transport contract required by the example.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
}

// Config bounds one simulation run. NumShards must match the running workers.
// Clients are simulated business customers/accounts, not separate FlowRulz tenants.
type Config struct {
	Clients              int
	MaxMessagesPerClient int
	NumShards            uint32
	Concurrency          int
	BaseSubject          string
	TenantID             string
	Scenario             Scenario
}

// Summary distinguishes generated attempts from durable broker acknowledgements.
type Summary struct {
	Clients     int
	Attempted   int
	BrokerAcked int
	Failed      int
	NumShards   uint32
}

type generationResult struct {
	count int
	err   error
}

// Validate rejects unsafe or nonsensical load settings before starting goroutines.
func (c Config) Validate() error {
	switch {
	case c.Clients < minClients || c.Clients > maxClients:
		return fmt.Errorf("clients must be between %d and %d", minClients, maxClients)
	case c.MaxMessagesPerClient < minMessages || c.MaxMessagesPerClient > maxMessages:
		return fmt.Errorf("max messages per client must be between %d and %d", minMessages, maxMessages)
	case c.NumShards == 0:
		return errors.New("shards must be greater than zero")
	case c.Concurrency < minConcurrency || c.Concurrency > maxConcurrency:
		return fmt.Errorf("concurrency must be between %d and %d", minConcurrency, maxConcurrency)
	case c.BaseSubject == "":
		return errors.New("base subject is required")
	case c.TenantID == "":
		return errors.New("tenant ID is required")
	case !c.Scenario.Valid():
		return fmt.Errorf("unsupported scenario %q (want all, ecommerce, banking, or food-delivery)", c.Scenario)
	}
	return nil
}

// Run generates realistic domain events and publishes them concurrently.
// It returns a partial summary if ctx is cancelled; publish errors are counted,
// not hidden. A broker ACK confirms JetStream acceptance, not worker processing.
func Run(ctx context.Context, cfg Config, publisher Publisher) (Summary, error) {
	if err := cfg.Validate(); err != nil {
		return Summary{}, err
	}
	if publisher == nil {
		return Summary{}, errors.New("publisher is required")
	}
	if ctx == nil {
		return Summary{}, errors.New("context is required")
	}

	summary := Summary{Clients: cfg.Clients, NumShards: cfg.NumShards}
	jobs := make(chan domain.EventEnvelope, cfg.Concurrency)
	results := make(chan error, cfg.Concurrency)
	generated := make(chan generationResult, 1)

	var workers sync.WaitGroup
	workers.Add(cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		go func() {
			defer workers.Done()
			for event := range jobs {
				results <- publishEvent(ctx, cfg, publisher, event)
			}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()
	go func() {
		count, err := generate(ctx, cfg, jobs)
		close(jobs)
		generated <- generationResult{count: count, err: err}
		close(generated)
	}()

	for err := range results {
		if err != nil {
			summary.Failed++
		} else {
			summary.BrokerAcked++
		}
	}
	generation := <-generated
	summary.Attempted = generation.count
	if generation.err != nil {
		return summary, generation.err
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	return summary, nil
}

func generate(ctx context.Context, cfg Config, jobs chan<- domain.EventEnvelope) (int, error) {
	//nolint:gosec // math/rand is acceptable for test data generation
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	attempted := 0
	globalSequence := 0

	for client := 1; client <= cfg.Clients; client++ {
		clientID := fmt.Sprintf("client-%04d", client)
		messageCount := rng.Intn(cfg.MaxMessagesPerClient) + 1
		for sequence := 1; sequence <= messageCount; sequence++ {
			globalSequence++
			scenario := cfg.Scenario
			if scenario == ScenarioAll {
				scenario = scenarioForSequence(globalSequence)
			}
			event, err := newScenarioEvent(scenario, cfg.TenantID, clientID, sequence, rng)
			if err != nil {
				return attempted, fmt.Errorf("generate %s event for %s: %w", scenario, clientID, err)
			}
			select {
			case jobs <- event:
				attempted++
			case <-ctx.Done():
				return attempted, nil
			}
		}
	}
	return attempted, nil
}

func publishEvent(ctx context.Context, cfg Config, publisher Publisher, event domain.EventEnvelope) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate generated event: %w", err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal generated event %q: %w", event.ID, err)
	}
	shard := event.VirtualShard(cfg.NumShards)
	subject := fmt.Sprintf("%s.shard.%d", cfg.BaseSubject, shard)
	if err := publisher.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish event %q (%s) to %s: %w", event.ID, event.Type, subject, err)
	}
	return nil
}
