package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	natsadapter "github.com/flowrulz/flow-rulz/internal/adapters/nats"
)

const (
	defaultNATSURL    = "nats://nats:4222"
	defaultNATSStream = "flowrulz"
	connectTimeout    = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	clientsDefault, err := envInt("RANDOM_PRODUCER_CLIENTS", 10)
	if err != nil {
		return err
	}
	messagesDefault, err := envInt("RANDOM_PRODUCER_MESSAGES_MAX", 100)
	if err != nil {
		return err
	}
	shardsDefault, err := envInt("NUM_SHARDS", 4096)
	if err != nil {
		return err
	}
	concurrencyDefault, err := envInt("RANDOM_PRODUCER_CONCURRENCY", 16)
	if err != nil {
		return err
	}

	clients := flag.Int("clients", clientsDefault, "number of simulated business clients/accounts")
	maxMessages := flag.Int("max-messages", messagesDefault, "maximum random messages per client (1-100)")
	if shardsDefault < 1 {
		return fmt.Errorf("NUM_SHARDS must be greater than zero")
	}
	shards := flag.Uint64("shards", uint64(shardsDefault), "virtual shard count; must match the workers")
	natsURL := flag.String("nats", env("NATS_URL", defaultNATSURL), "NATS URL; use the Compose service name inside the network")
	tenantID := flag.String("tenant", env("RANDOM_PRODUCER_TENANT_ID", DefaultTenantID), "FlowRulz tenant used by the deployed example rules")
	scenario := flag.String("scenario", env("RANDOM_PRODUCER_SCENARIO", string(ScenarioAll)), "business scenario: all, ecommerce, banking, or food-delivery")
	timeout := flag.Duration("timeout", 30*time.Second, "overall simulation timeout")
	concurrency := flag.Int("concurrency", concurrencyDefault, "maximum concurrent publishers")
	flag.Parse()

	if *shards == 0 || *shards > uint64(^uint32(0)) {
		return fmt.Errorf("shards must be between 1 and %d", uint64(^uint32(0)))
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be greater than zero")
	}
	cfg := Config{
		Clients:              *clients,
		MaxMessagesPerClient: *maxMessages,
		NumShards:            uint32(*shards),
		Concurrency:          *concurrency,
		BaseSubject:          DefaultBaseSubject,
		TenantID:             *tenantID,
		Scenario:             Scenario(*scenario),
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	publisher, err := natsadapter.NewPublisher(natsadapter.Config{
		NatsURL:        *natsURL,
		Stream:         env("NATS_STREAM", defaultNATSStream),
		NumShards:      cfg.NumShards,
		ConnectTimeout: connectTimeout,
	})
	if err != nil {
		return fmt.Errorf("create NATS publisher: %w", err)
	}
	defer func() { _ = publisher.Close() }()

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, *timeout)
	defer cancel()

	summary, runErr := Run(ctx, cfg, publisher)
	log.Printf("random-producer example complete: tenant=%s scenario=%s clients=%d attempted=%d broker_acked=%d failed=%d shards=%d",
		cfg.TenantID, cfg.Scenario, summary.Clients, summary.Attempted, summary.BrokerAcked, summary.Failed, summary.NumShards)
	if runErr != nil {
		return fmt.Errorf("simulation stopped: %w", runErr)
	}
	if summary.Failed > 0 {
		return fmt.Errorf("%d of %d publish attempts failed", summary.Failed, summary.Attempted)
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}
