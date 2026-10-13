package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"
)

const (
	defaultClients  = 5
	defaultMessages = 100
	maxClients      = 20
	maxMessages     = 100
	eventType       = "loadtest.message"
)

type simulatedClient struct {
	id     string
	client *flowrulz.Client
}

type payload struct {
	ClientID  string    `json:"client_id"`
	MessageNo int       `json:"message_no"`
	Text      string    `json:"text"`
	Value     int       `json:"value"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	clientsFlag := flag.Int("clients", defaultClients, "number of simulated tenant clients (1-20)")
	messagesFlag := flag.Int("messages", defaultMessages, "total messages to publish (1-100)")
	delayFlag := flag.Duration("startup-delay", 500*time.Millisecond, "wait for best-effort observer subscriptions before publishing")
	flag.Parse()

	if *clientsFlag < 1 || *clientsFlag > maxClients {
		log.Fatalf("--clients must be between 1 and %d", maxClients)
	}
	if *messagesFlag < 1 || *messagesFlag > maxMessages {
		log.Fatalf("--messages must be between 1 and %d", maxMessages)
	}
	if *delayFlag < 0 || *delayFlag > 10*time.Second {
		log.Fatal("--startup-delay must be between 0 and 10s")
	}

	natsURL := envOr("NATS_URL", "nats://localhost:4222")
	stream := envOr("NATS_STREAM", "flowrulz")
	numShards := uint32(4096)
	if raw := os.Getenv("NUM_SHARDS"); raw != "" {
		var parsed uint32
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err != nil || parsed == 0 {
			log.Fatal("NUM_SHARDS must be a positive integer")
		}
		numShards = parsed
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	clients := make([]simulatedClient, 0, *clientsFlag)
	for i := 0; i < *clientsFlag; i++ {
		tenantID := fmt.Sprintf("load-client-%02d", i+1)
		cfg := &flowrulz.Config{
			Transport:    flowrulz.TransportNATS,
			TenantID:     tenantID,
			NATSURL:      natsURL,
			NATSStream:   stream,
			NATSSubjects: []string{"events"},
			NumShards:    numShards,
			SendTimeout:  5 * time.Second,
			MaxRetries:   2,
			RetryBackoff: 100 * time.Millisecond,
		}
		client, err := flowrulz.NewWithConfig(cfg)
		if err != nil {
			closeClients(clients)
			log.Fatalf("create simulated client %s: %v", tenantID, err)
		}
		clients = append(clients, simulatedClient{id: tenantID, client: client})
	}

	var received atomic.Int64
	var consumerWG sync.WaitGroup
	for _, simulated := range clients {
		consumerWG.Add(1)
		go func(sc simulatedClient) {
			defer consumerWG.Done()
			err := sc.client.Subscribe(ctx, eventType, func(_ context.Context, envelope *flowrulz.Envelope) error {
				received.Add(1)
				log.Printf("CONSUMED tenant=%s event_id=%s partition_key=%s payload_bytes=%d",
					envelope.TenantID, envelope.ID, envelope.PartitionKey, len(envelope.Data))
				return nil
			})
			if err != nil && ctx.Err() == nil {
				log.Printf("consumer for %s stopped: %v", sc.id, err)
			}
		}(simulated)
	}

	if *delayFlag > 0 {
		select {
		case <-ctx.Done():
			closeClients(clients)
			return
		case <-time.After(*delayFlag):
		}
	}

	started := time.Now()
	var sent atomic.Int64
	var failed atomic.Int64
	for i := 1; i <= *messagesFlag; i++ {
		select {
		case <-ctx.Done():
			log.Printf("interrupted after %d send attempts", i-1)
			closeClients(clients)
			return
		default:
		}

		sc := clients[rng.Intn(len(clients))]
		key := fmt.Sprintf("%s-account-%02d", sc.id, rng.Intn(10)+1)
		message := payload{
			ClientID: sc.id, MessageNo: i,
			Text:      fmt.Sprintf("random message %s", randomID()),
			Value:     rng.Intn(10000),
			CreatedAt: time.Now().UTC(),
		}
		if err := sc.client.Send(ctx, eventType, message, flowrulz.WithPartitionKey(key)); err != nil {
			failed.Add(1)
			log.Printf("PUBLISH_FAILED tenant=%s message_no=%d error=%v", sc.id, i, err)
			continue
		}
		sent.Add(1)
		log.Printf("PUBLISHED tenant=%s message_no=%d partition_key=%s", sc.id, i, key)
	}

	log.Printf("publish complete: clients=%d attempted=%d accepted=%d failed=%d elapsed=%s",
		len(clients), *messagesFlag, sent.Load(), failed.Load(), time.Since(started).Round(time.Millisecond))
	log.Printf("observing consumers for 2s; consumed_count is best-effort because SDK subscriptions are live, non-durable observers")
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
	}
	log.Printf("summary: accepted=%d observed=%d publish_failed=%d", sent.Load(), received.Load(), failed.Load())

	closeClients(clients)
	stop()
	consumerWG.Wait()
}

func closeClients(clients []simulatedClient) {
	for _, sc := range clients {
		if err := sc.client.Close(); err != nil {
			log.Printf("close client %s: %v", sc.id, err)
		}
	}
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
