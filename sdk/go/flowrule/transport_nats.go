package flowrulz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsTransport struct {
	cfg    *TransportConfig
	conn   *nats.Conn
	js     jetstream.JetStream
	mu     sync.RWMutex
	closed bool
}

func newNATSTransport(cfg *TransportConfig) (Transport, error) {
	conn, err := nats.Connect(cfg.Config.NATSURL,
		nats.Timeout(10*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create JetStream: %w", err)
	}

	// Ensure stream exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subjects := make([]string, 0, len(cfg.Config.NATSSubjects)*2)
	for _, s := range cfg.Config.NATSSubjects {
		subjects = append(subjects, s+".shard.*", s+".ruleset.*.shard.*")
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Config.NATSStream,
		Subjects:  subjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxMsgs:   -1,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return &natsTransport{
		cfg:  cfg,
		conn: conn,
		js:   js,
	}, nil
}

func (t *natsTransport) Send(ctx context.Context, env *Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// Compute shard
	shard := computeShard(env.TenantID, env.PartitionKey, t.cfg.Config.NumShards)

	// Use first subject as base
	baseSubject := t.cfg.Config.NATSSubjects[0]
	subject := natsSubjectForEnvelope(baseSubject, shard, env.Type, t.cfg.Config.NATSRuleSets)

	// Create context with timeout
	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	_, err = t.js.Publish(sendCtx, subject, data)
	if err != nil {
		return fmt.Errorf("publish to NATS: %w", err)
	}

	return nil
}

func (t *natsTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	if len(envs) == 0 {
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	for _, env := range envs {
		data, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}

		shard := computeShard(env.TenantID, env.PartitionKey, t.cfg.Config.NumShards)
		baseSubject := t.cfg.Config.NATSSubjects[0]
		subject := natsSubjectForEnvelope(baseSubject, shard, env.Type, t.cfg.Config.NATSRuleSets)

		_, err = t.js.Publish(sendCtx, subject, data)
		if err != nil {
			return fmt.Errorf("publish to NATS: %w", err)
		}
	}

	return nil
}

func (t *natsTransport) Subscribe(ctx context.Context, ruleSet string, handler EventHandler) (retErr error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return ErrTransportClosed
	}
	conn := t.conn
	cfg := t.cfg.Config
	t.mu.RUnlock()

	messages := make(chan *nats.Msg, 128)
	subscriptions := make([]*nats.Subscription, 0, len(cfg.NATSSubjects))
	defer func() {
		for _, subscription := range subscriptions {
			if err := subscription.Unsubscribe(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("unsubscribe from NATS: %w", err))
			}
		}
	}()

	for _, subject := range cfg.NATSSubjects {
		for _, filter := range natsSubscriptionFilters(subject, ruleSet, cfg.NATSRuleSets) {
			subscription, err := conn.ChanSubscribe(filter, messages)
			if err != nil {
				return fmt.Errorf("subscribe to NATS subject %q: %w", filter, err)
			}
			subscriptions = append(subscriptions, subscription)
		}
	}
	flushCtx, cancel := context.WithTimeout(ctx, cfg.SendTimeout)
	defer cancel()
	if err := conn.FlushWithContext(flushCtx); err != nil {
		return fmt.Errorf("activate NATS subscriptions: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			if message == nil {
				continue
			}

			var envelope Envelope
			if err := json.Unmarshal(message.Data, &envelope); err != nil {
				return fmt.Errorf("decode NATS event envelope: %w", err)
			}
			if envelope.Type == "" || envelope.TenantID == "" {
				return ErrInvalidArgument("NATS event envelope must include type and tenant ID")
			}
			if envelope.TenantID != cfg.TenantID || envelope.Type != ruleSet {
				continue
			}
			if err := handler(ctx, &envelope); err != nil {
				return fmt.Errorf("handle NATS event for rule set %q: %w", ruleSet, err)
			}
		}
	}
}

func natsSubjectForEnvelope(baseSubject string, shard uint32, eventType string, configuredRuleSets []string) string {
	for _, ruleSet := range configuredRuleSets {
		if ruleSet == eventType {
			sum := sha256.Sum256([]byte(eventType))
			return fmt.Sprintf("%s.ruleset.%s.shard.%d", baseSubject, hex.EncodeToString(sum[:]), shard)
		}
	}
	return fmt.Sprintf("%s.shard.%d", baseSubject, shard)
}

// natsSubscriptionFilters limits a ruleset subscriber to its hashed route and the
// legacy route used by producers that have not configured ruleset-specific routing.
func natsSubscriptionFilters(baseSubject, ruleSet string, configuredRuleSets []string) []string {
	filters := []string{fmt.Sprintf("%s.shard.*", baseSubject)}
	if ruleSet == "" {
		return filters
	}
	for _, configured := range configuredRuleSets {
		if configured != ruleSet {
			continue
		}
		sum := sha256.Sum256([]byte(ruleSet))
		filters = append(filters, fmt.Sprintf("%s.ruleset.%s.shard.*", baseSubject, hex.EncodeToString(sum[:])))
		break
	}
	return filters
}

func (t *natsTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	if t.conn != nil {
		t.conn.Close()
	}
	return nil
}

func (t *natsTransport) Name() string {
	return "nats"
}

// ErrTransportClosed is returned when sending on a closed transport
var ErrTransportClosed = fmt.Errorf("transport closed")
