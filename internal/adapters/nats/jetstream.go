// Package nats provides NATS JetStream adapters for event publishing and consumption.
package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds NATS JetStream connection and stream configuration.
type Config struct {
	NatsURL        string
	ConnectTimeout time.Duration
	Stream         string
	Consumer       string
	Subjects       []string
	RuleSets       []string
	AckWait        time.Duration
	MaxDeliver     int
	NumShards      uint32
}

type consumerKey struct {
	baseSubject string
	ruleSet     string
	shard       uint32
}

// Consumer is the pull-consumer adapter. Publisher is composed separately so
// consuming and publishing lifecycles can be managed independently.
type Consumer struct {
	conn        *natslib.Conn
	js          jetstream.JetStream
	stream      string
	consumers   map[consumerKey]string
	subjects    []string
	ruleSets    []string
	baseDurable string
	cfg         Config
	numShards   uint32
	fetchCursor uint64
	mu          sync.RWMutex
}

func connect(cfg Config) (*natslib.Conn, error) {
	if cfg.ConnectTimeout > 0 {
		return natslib.Connect(cfg.NatsURL, natslib.Timeout(cfg.ConnectTimeout))
	}
	return natslib.Connect(cfg.NatsURL)
}

func normalizeConfig(cfg Config) (Config, error) {
	cfg = applyConfigDefaults(cfg)
	if err := validateConsumerConfig(cfg); err != nil {
		return Config{}, err
	}
	subjects, err := normalizeSubjects(cfg.Subjects)
	if err != nil {
		return Config{}, err
	}
	ruleSets, err := normalizeRuleSets(cfg.RuleSets)
	if err != nil {
		return Config{}, err
	}
	cfg.Subjects, cfg.RuleSets = subjects, ruleSets
	return cfg, nil
}

func applyConfigDefaults(cfg Config) Config {
	if cfg.NumShards == 0 {
		cfg.NumShards = 4096
	}
	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"events"}
	}
	if cfg.Stream == "" {
		cfg.Stream = "flowrulz"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = "flowrulz-worker"
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	return cfg
}

func validateConsumerConfig(cfg Config) error {
	if cfg.MaxDeliver != -1 {
		return fmt.Errorf("NATS MaxDeliver must be -1 (unlimited) until durable exhausted-delivery recovery is implemented")
	}
	return nil
}

func normalizeSubjects(values []string) ([]string, error) {
	subjects := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, subject := range values {
		subject = strings.TrimSpace(subject)
		if subject == "" {
			return nil, fmt.Errorf("NATS subject must not be empty")
		}
		if _, exists := seen[subject]; exists {
			continue
		}
		seen[subject] = struct{}{}
		subjects = append(subjects, subject)
	}
	return subjects, nil
}

func normalizeRuleSets(values []string) ([]string, error) {
	ruleSets := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, ruleSet := range values {
		ruleSet = strings.TrimSpace(ruleSet)
		if ruleSet == "" {
			return nil, fmt.Errorf("NATS ruleset must not be empty")
		}
		if len(ruleSet) > 128 {
			return nil, fmt.Errorf("NATS ruleset exceeds 128 bytes")
		}
		if _, exists := seen[ruleSet]; exists {
			return nil, fmt.Errorf("duplicate NATS ruleset %q", ruleSet)
		}
		seen[ruleSet] = struct{}{}
		ruleSets = append(ruleSets, ruleSet)
	}
	sort.Strings(ruleSets)
	return ruleSets, nil
}

func streamSubjects(cfg Config) []string {
	subjects := make([]string, 0, len(cfg.Subjects)*2)
	for _, base := range cfg.Subjects {
		subjects = append(subjects, base+".shard.*", base+".ruleset.*.shard.*")
	}
	return subjects
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: cfg.Stream, Subjects: streamSubjects(cfg), Retention: jetstream.WorkQueuePolicy,
		Storage: jetstream.FileStorage, MaxMsgs: -1, Discard: jetstream.DiscardOld,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	return nil
}

// NewConsumer creates a new NATS JetStream consumer.
func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream: %w", err)
	}
	if err := ensureStream(ctx, js, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	return &Consumer{
		conn: conn, js: js, stream: cfg.Stream, consumers: make(map[consumerKey]string),
		subjects: cfg.Subjects, ruleSets: cfg.RuleSets, baseDurable: cfg.Consumer,
		cfg: cfg, numShards: cfg.NumShards,
	}, nil
}

func ruleSetToken(ruleSet string) string {
	sum := sha256.Sum256([]byte(ruleSet))
	return hex.EncodeToString(sum[:])
}

func ruleSetSubject(baseSubject, ruleSet string, shard uint32) string {
	return fmt.Sprintf("%s.ruleset.%s.shard.%d", baseSubject, ruleSetToken(ruleSet), shard)
}

func containsRuleSet(ruleSets []string, candidate string) bool {
	for _, ruleSet := range ruleSets {
		if ruleSet == candidate {
			return true
		}
	}
	return false
}

func subjectForMessage(baseSubject string, shard uint32, data []byte, ruleSets []string) string {
	var envelope domain.EventEnvelope
	if json.Unmarshal(data, &envelope) == nil && envelope.Type != "" && containsRuleSet(ruleSets, envelope.Type) {
		return ruleSetSubject(baseSubject, envelope.Type, shard)
	}
	return fmt.Sprintf("%s.shard.%d", baseSubject, shard)
}

func safeDurableToken(value string) string {
	value = strings.NewReplacer(".", "-", "*", "x", ">", "x", " ", "-").Replace(value)
	if len(value) > 32 {
		value = value[:32]
	}
	return value
}

func (c *Consumer) ensureConsumerLocked(ctx context.Context, key consumerKey) error {
	if _, exists := c.consumers[key]; exists {
		return nil
	}
	filterSubject := fmt.Sprintf("%s.shard.%d", key.baseSubject, key.shard)
	durable := fmt.Sprintf("%s-shard-%d", c.baseDurable, key.shard)
	if key.baseSubject != c.subjects[0] {
		durable = fmt.Sprintf("%s-%s-shard-%d", c.baseDurable, safeDurableToken(key.baseSubject), key.shard)
	}
	if key.ruleSet != "" {
		token := ruleSetToken(key.ruleSet)
		filterSubject = ruleSetSubject(key.baseSubject, key.ruleSet, key.shard)
		durable = fmt.Sprintf("%s-%s-rs-%s-shard-%d", c.baseDurable, safeDurableToken(key.baseSubject), token[:32], key.shard)
	}
	_, err := c.js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, jetstream.ConsumerConfig{
		Durable: durable, AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: filterSubject,
		MaxDeliver: c.cfg.MaxDeliver, AckWait: c.cfg.AckWait,
	})
	if err != nil {
		return fmt.Errorf("create consumer for subject %q shard %d: %w", filterSubject, key.shard, err)
	}
	c.consumers[key] = durable
	log.Printf("created durable consumer subject=%s ruleset=%q shard=%d durable=%s", key.baseSubject, key.ruleSet, key.shard, durable)
	return nil
}

// EnsureShardConsumer ensures durable consumers exist for the given shard.
func (c *Consumer) EnsureShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, base := range c.subjects {
		if err := c.ensureConsumerLocked(ctx, consumerKey{baseSubject: base, shard: shard}); err != nil {
			return err
		}
		for _, ruleSet := range c.ruleSets {
			if err := c.ensureConsumerLocked(ctx, consumerKey{baseSubject: base, ruleSet: ruleSet, shard: shard}); err != nil {
				return err
			}
		}
	}
	return nil
}

// RemoveShardConsumer removes durable consumers for the given shard.
func (c *Consumer) RemoveShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.RLock()
	keys := make([]consumerKey, 0)
	for key := range c.consumers {
		if key.shard == shard {
			keys = append(keys, key)
		}
	}
	c.mu.RUnlock()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].baseSubject != keys[j].baseSubject {
			return keys[i].baseSubject < keys[j].baseSubject
		}
		return keys[i].ruleSet < keys[j].ruleSet
	})
	var removeErrs []error
	for _, key := range keys {
		c.mu.RLock()
		durable := c.consumers[key]
		c.mu.RUnlock()
		if durable == "" {
			continue
		}
		if err := c.js.DeleteConsumer(ctx, c.cfg.Stream, durable); err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("delete consumer %q: %w", durable, err))
			continue
		}
		c.mu.Lock()
		if c.consumers[key] == durable {
			delete(c.consumers, key)
		}
		c.mu.Unlock()
	}
	return errors.Join(removeErrs...)
}

// FetchShards fetches messages from the specified shards.
func (c *Consumer) FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	if err := validateFetchRequest(ctx, maxMessages); err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, nil
	}

	selected, durables := c.selectFetchConsumers(maxMessages, shards)
	if len(selected) == 0 {
		return nil, nil
	}
	results := c.fetchSelectedConsumers(ctx, maxMessages, selected, durables)
	return collectFetchResults(results, maxMessages)
}

func validateFetchRequest(ctx context.Context, maxMessages int) error {
	if maxMessages <= 0 {
		return fmt.Errorf("fetch shards: maxMessages must be > 0")
	}
	if ctx == nil {
		return fmt.Errorf("fetch shards: context is nil")
	}
	return ctx.Err()
}

func (c *Consumer) selectFetchConsumers(
	maxMessages int,
	shards []uint32,
) ([]consumerKey, map[consumerKey]string) {
	wanted := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		wanted[shard] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	keys := matchingConsumerKeys(c.consumers, wanted)
	if len(keys) == 0 {
		return nil, nil
	}

	selectedCount := min(len(keys), maxMessages)
	start := int(c.fetchCursor % uint64(len(keys))) //nolint:gosec // bounded by len(keys)
	selected := make([]consumerKey, selectedCount)
	durables := make(map[consumerKey]string, selectedCount)
	for i := 0; i < selectedCount; i++ {
		key := keys[(start+i)%len(keys)]
		selected[i] = key
		durables[key] = c.consumers[key]
	}
	c.fetchCursor = uint64((start + selectedCount) % len(keys)) //nolint:gosec // bounded by len(keys)
	return selected, durables
}

func matchingConsumerKeys(consumers map[consumerKey]string, wanted map[uint32]struct{}) []consumerKey {
	keys := make([]consumerKey, 0, len(consumers))
	for key := range consumers {
		if _, ok := wanted[key.shard]; ok {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].shard != keys[j].shard {
			return keys[i].shard < keys[j].shard
		}
		if keys[i].baseSubject != keys[j].baseSubject {
			return keys[i].baseSubject < keys[j].baseSubject
		}
		return keys[i].ruleSet < keys[j].ruleSet
	})
	return keys
}

type shardFetchResult struct {
	deliveries []ports.Delivery
	err        error
}

func (c *Consumer) fetchSelectedConsumers(
	ctx context.Context,
	maxMessages int,
	selected []consumerKey,
	durables map[consumerKey]string,
) []shardFetchResult {
	results := make(chan shardFetchResult, len(selected))
	var wg sync.WaitGroup
	baseBudget, remainder := maxMessages/len(selected), maxMessages%len(selected)
	for i, key := range selected {
		budget := baseBudget
		if i < remainder {
			budget++
		}
		wg.Add(1)
		go func(_ consumerKey, durable string, budget int) {
			defer wg.Done()
			results <- c.fetchFromConsumer(ctx, durable, budget)
		}(key, durables[key], budget)
	}
	wg.Wait()
	close(results)
	collected := make([]shardFetchResult, 0, len(selected))
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

func (c *Consumer) fetchFromConsumer(ctx context.Context, durable string, budget int) shardFetchResult {
	consumer, err := c.js.Consumer(ctx, c.stream, durable)
	if err != nil {
		return shardFetchResult{err: fmt.Errorf("get consumer %q: %w", durable, err)}
	}
	batch, err := consumer.Fetch(budget, jetstream.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		if errors.Is(err, natslib.ErrTimeout) {
			return shardFetchResult{}
		}
		return shardFetchResult{err: fmt.Errorf("fetch consumer %q: %w", durable, err)}
	}
	deliveries := make([]ports.Delivery, 0, budget)
	for msg := range batch.Messages() {
		deliveries = append(deliveries, &jetstreamDelivery{msg: msg})
	}
	if batchErr := batch.Error(); isUnexpectedFetchError(batchErr) {
		return shardFetchResult{deliveries: deliveries, err: fmt.Errorf("receive from consumer %q: %w", durable, batchErr)}
	}
	return shardFetchResult{deliveries: deliveries}
}

func isUnexpectedFetchError(err error) bool {
	return err != nil && !errors.Is(err, natslib.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages)
}

func collectFetchResults(results []shardFetchResult, maxMessages int) ([]ports.Delivery, error) {
	deliveries := make([]ports.Delivery, 0, maxMessages)
	var fetchErrs []error
	for _, result := range results {
		deliveries = append(deliveries, result.deliveries...)
		if result.err != nil {
			fetchErrs = append(fetchErrs, result.err)
		}
	}
	if len(deliveries) > maxMessages {
		fetchErrs = append(fetchErrs, fmt.Errorf("fetch invariant violated: got %d deliveries for limit %d", len(deliveries), maxMessages))
	}
	return deliveries, errors.Join(fetchErrs...)
}

// OwnedShards returns the shards for which this consumer has active consumers.
func (c *Consumer) OwnedShards() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	expected := len(c.subjects) * (1 + len(c.ruleSets))
	counts := make(map[uint32]int)
	for key := range c.consumers {
		counts[key.shard]++
	}
	shards := make([]uint32, 0, len(counts))
	for shard, count := range counts {
		if count == expected {
			shards = append(shards, shard)
		}
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}

// ShardCount returns the configured number of shards.
func (c *Consumer) ShardCount() uint32 {
	return c.numShards
}

// Ping reports whether the broker connection is up and the event stream exists.
func (c *Consumer) Ping(ctx context.Context) error {
	if c.conn == nil || c.conn.Status() != natslib.CONNECTED {
		return errors.New("nats connection is not established")
	}
	if _, err := c.js.Stream(ctx, c.stream); err != nil {
		return fmt.Errorf("look up stream %q: %w", c.stream, err)
	}
	return nil
}

// Close closes the NATS connection.
func (c *Consumer) Close() error {
	c.conn.Close()
	return nil
}

// Fetch fetches messages from all owned shards.
func (c *Consumer) Fetch(ctx context.Context, maxMessages int) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, c.OwnedShards())
}

// Subscribe is a no-op for pull-based consumers.
func (c *Consumer) Subscribe(_ context.Context, _ []string) error {
	return nil
}

// Unsubscribe is a no-op for pull-based consumers.
func (c *Consumer) Unsubscribe(_ context.Context) error {
	return nil
}

// SendOrderCreate publishes the order-create contract payload to its assigned shard.
// The NATS subject and JetStream acknowledgement details remain inside this adapter.
func (c *Consumer) SendOrderCreate(ctx context.Context, shard uint32, payload []byte) error {
	return c.PublishToShard(ctx, "orders.create", shard, payload)
}

// ConsumeOrderCreate fetches order-create deliveries for the requested owned shards.
// It returns ports.Delivery values so callers never need JetStream message types.
func (c *Consumer) ConsumeOrderCreate(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, shards)
}

// Compile-time assertions keep the adapter aligned with consumer-owned contracts.
var _ ports.EventConsumer = (*Consumer)(nil)
var _ ports.ShardedConsumer = (*Consumer)(nil)
var _ ports.EventPublisher = (*Consumer)(nil)
var _ ports.OrderEventBroker = (*Consumer)(nil)
var _ ports.EventPublisher = (*Publisher)(nil)

// Publish is the generic adapter-level primitive that implements ports.BrokerPublisher.
func (c *Consumer) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish subject %q: %w", subject, err)
	}
	return nil
}

// PublishBatch implements ports.BrokerPublisher.
func (c *Consumer) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if _, err := c.js.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish batch message %d to %q: %w", i, msg.Subject, err)
		}
	}
	return nil
}

// PublishToShard implements ports.BrokerPublisher.
func (c *Consumer) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	return c.Publish(ctx, subjectForMessage(baseSubject, shard, data, c.ruleSets), data)
}

// Publisher provides a dedicated publisher for sending messages without consuming
type Publisher struct {
	conn      *natslib.Conn
	js        jetstream.JetStream
	stream    string
	numShards uint32
	ruleSets  []string
}

// NewPublisher creates a new NATS JetStream publisher.
func NewPublisher(cfg Config) (*Publisher, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect nats publisher: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream publisher: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ensureStream(ctx, js, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	return &Publisher{
		conn: conn, js: js, stream: cfg.Stream, numShards: cfg.NumShards, ruleSets: cfg.RuleSets,
	}, nil
}

// Publish publishes a message to a subject.
func (p *Publisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish subject %q: %w", subject, err)
	}
	return nil
}

// SendOrderCreate publishes the order-create contract payload to its assigned shard.
func (p *Publisher) SendOrderCreate(ctx context.Context, shard uint32, payload []byte) error {
	return p.PublishToShard(ctx, "orders.create", shard, payload)
}

// PublishToShard publishes a message to a shard-specific subject.
func (p *Publisher) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	return p.Publish(ctx, subjectForMessage(baseSubject, shard, data, p.ruleSets), data)
}

// PublishBatch publishes multiple messages.
func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if err := p.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish batch message %d: %w", i, err)
		}
	}
	return nil
}

// Ping reports whether the publisher connection is up and the stream exists.
func (p *Publisher) Ping(ctx context.Context) error {
	if p.conn == nil || p.conn.Status() != natslib.CONNECTED {
		return errors.New("nats publisher connection is not established")
	}
	if _, err := p.js.Stream(ctx, p.stream); err != nil {
		return fmt.Errorf("look up stream %q: %w", p.stream, err)
	}
	return nil
}

// Close closes the publisher connection.
func (p *Publisher) Close() error {
	if p.conn != nil {
		p.conn.Close()
	}
	return nil
}

type jetstreamDelivery struct {
	msg jetstream.Msg
}

func (d *jetstreamDelivery) Event() (*domain.EventEnvelope, error) {
	env := &domain.EventEnvelope{}
	if err := json.Unmarshal(d.msg.Data(), env); err != nil {
		return nil, err
	}
	return env, nil
}

func (d *jetstreamDelivery) Raw() []byte {
	return d.msg.Data()
}

func (d *jetstreamDelivery) Attempt() uint64 {
	metadata, err := d.msg.Metadata()
	if err != nil || metadata == nil || metadata.NumDelivered == 0 {
		return 1
	}
	return metadata.NumDelivered
}

func (d *jetstreamDelivery) Ack(_ context.Context) error {
	return d.msg.Ack()
}

func (d *jetstreamDelivery) Nak(_ context.Context) error {
	return d.msg.Nak()
}

func (d *jetstreamDelivery) Retry(_ context.Context, delay time.Duration) error {
	return d.msg.NakWithDelay(delay)
}

func (d *jetstreamDelivery) Headers() map[string]string {
	h := make(map[string]string)
	for k, v := range d.msg.Headers() {
		if len(v) > 0 {
			h[k] = v[0]
		}
	}
	return h
}

func (d *jetstreamDelivery) Subject() string {
	return d.msg.Subject()
}
