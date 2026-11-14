// Package broker implements a bounded, partitioned broker inside a worker process.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/google/uuid"
)

var (
	// ErrFull indicates retained data or metadata reached a configured bound.
	ErrFull = errors.New("broker retention capacity reached")
	// ErrReceipt rejects an expired or superseded consumer's settlement attempt.
	ErrReceipt = errors.New("delivery receipt expired or superseded")
	// ErrInvalid identifies invalid requests and incompatible persisted state.
	ErrInvalid = errors.New("invalid broker request")
)

// Store atomically replaces durable broker state. A nil store selects memory.
type Store interface {
	Load() ([]byte, error)
	Save([]byte) error
}

// Config bounds retained data and delivery ownership.
type Config struct {
	Partitions  uint32
	MaxMessages int
	MaxBytes    int64
	AckWait     time.Duration
}

// Record is an immutable message in a topic partition. Offsets start at zero.
type Record struct {
	Topic     string          `json:"topic"`
	Partition uint32          `json:"partition"`
	Offset    uint64          `json:"offset"`
	TenantID  string          `json:"tenant_id"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
}

// Claim carries a temporary ownership token required to settle a record.
type Claim struct {
	Record    Record    `json:"record"`
	Group     string    `json:"group"`
	Receipt   string    `json:"receipt"`
	ExpiresAt time.Time `json:"expires_at"`
}

type group struct {
	TenantID string            `json:"tenant_id"`
	Topic    string            `json:"topic"`
	Name     string            `json:"name"`
	Offsets  map[uint32]uint64 `json:"offsets"`
	Leases   map[uint32]Claim  `json:"-"`
	Cursor   uint32            `json:"-"`
}

type partitionLog struct {
	Base    uint64   `json:"base"`
	Records []Record `json:"records"`
}

type state struct {
	Version    int                                 `json:"version"`
	Partitions uint32                              `json:"partitions"`
	Topics     map[string]map[uint32]*partitionLog `json:"topics"`
	Groups     map[string]*group                   `json:"groups"`
}

// Broker owns topic logs, consumer offsets, and fenced delivery claims.
type Broker struct {
	mu      sync.Mutex
	cfg     Config
	store   Store
	state   state
	count   int
	bytes   int64
	changed chan struct{}
	failure error
}

// New restores retained messages and committed offsets. Claims expire on restart.
func New(cfg Config, store Store) (*Broker, error) {
	if cfg.Partitions == 0 || cfg.Partitions > 4096 || cfg.MaxMessages <= 0 || cfg.MaxMessages > 10000 || cfg.MaxBytes <= 0 || cfg.MaxBytes > 64<<20 || cfg.AckWait <= 0 || cfg.AckWait > 5*time.Minute {
		return nil, ErrInvalid
	}
	b := &Broker{cfg: cfg, store: store, changed: make(chan struct{}), state: state{Version: 1, Partitions: cfg.Partitions, Topics: make(map[string]map[uint32]*partitionLog), Groups: make(map[string]*group)}}
	if err := b.restore(); err != nil {
		return nil, err
	}
	if err := b.validateState(); err != nil {
		return nil, err
	}
	return b, nil
}

func validName(s string) bool { return len(s) > 0 && len(s) <= 128 }

func (b *Broker) validateState() error {
	if b.state.Version != 1 || b.state.Partitions != b.cfg.Partitions || b.state.Topics == nil || b.state.Groups == nil || len(b.state.Topics) > 128 || len(b.state.Groups) > 1024 {
		return fmt.Errorf("invalid or incompatible broker snapshot: %w", ErrInvalid)
	}
	if err := b.validateTopics(); err != nil {
		return err
	}
	if err := b.validateGroups(); err != nil {
		return err
	}
	if b.count > b.cfg.MaxMessages || b.bytes > b.cfg.MaxBytes {
		return ErrFull
	}
	return nil
}

func (b *Broker) persistLocked() error {
	if b.store == nil {
		return nil
	}
	data, err := json.Marshal(b.state)
	if err == nil {
		err = b.store.Save(data)
	}
	if err != nil {
		b.failure = fmt.Errorf("persist broker state; restart required: %w", err)
		b.signalLocked()
		return b.failure
	}
	return nil
}

func (b *Broker) signalLocked() { close(b.changed); b.changed = make(chan struct{}) }

// Publish appends using the shared tenant/partition-key routing algorithm.
// Acceptance confirms broker storage, independently of rule processing.
func (b *Broker) Publish(topic, tenant, key string, payload json.RawMessage) (Record, error) {
	return b.publish(topic, tenant, key, payload, false)
}

// PublishInput reserves a quarter of capacity for rule outputs and retries.
func (b *Broker) PublishInput(tenant, key string, payload json.RawMessage) (Record, error) {
	return b.publish("events", tenant, key, payload, true)
}

func (b *Broker) publish(topic, tenant, key string, payload json.RawMessage, input bool) (Record, error) {
	if !validName(topic) || !validName(tenant) || len(key) == 0 || len(key) > 1024 || len(payload) > 1<<20 || !json.Valid(payload) {
		return Record{}, ErrInvalid
	}
	partition := (&domain.EventEnvelope{TenantID: tenant, PartitionKey: key}).VirtualShard(b.cfg.Partitions)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.reserveCapacityLocked(len(payload), input); err != nil {
		return Record{}, err
	}
	return b.publishPartitionLocked(topic, tenant, key, payload, partition)
}

// compactLocked reclaims only prefixes consumed by every registered matching group.
// Unsubscribed output topics retain messages until a consumer acknowledges them.
func (b *Broker) compactLocked() {
	for _, partitions := range b.state.Topics {
		for _, log := range partitions {
			b.compactPartitionLocked(log)
		}
	}
}

func (b *Broker) consumedLocked(record Record) bool {
	matched := false
	for _, g := range b.state.Groups {
		if g.Topic != record.Topic || (g.TenantID != "" && g.TenantID != record.TenantID) {
			continue
		}
		matched = true
		if g.Offsets[record.Partition] <= record.Offset {
			return false
		}
	}
	return matched
}

func groupKey(tenant, topic, name string) string {
	data, _ := json.Marshal([]string{tenant, topic, name})
	return string(data)
}

// Register establishes a group before publishers can reclaim its unprocessed data.
func (b *Broker) Register(tenant, topic, name string) error {
	if !validName(topic) || !validName(name) || (tenant != "" && !validName(tenant)) {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.groupLocked(tenant, topic, name)
	return err
}

func (b *Broker) groupLocked(tenant, topic, name string) (*group, error) {
	if b.failure != nil {
		return nil, b.failure
	}
	key := groupKey(tenant, topic, name)
	if g := b.state.Groups[key]; g != nil {
		return g, nil
	}
	return b.createGroupLocked(key, tenant, topic, name)
}

// Next claims the first unacknowledged matching record in an available partition.
// Empty tenant is reserved for the in-process rule consumer across all tenants.
func (b *Broker) Next(ctx context.Context, tenant, topic, name string, wait time.Duration) (*Claim, error) {
	if !validName(topic) || !validName(name) || (tenant != "" && !validName(tenant)) || wait < 0 || wait > 20*time.Second {
		return nil, ErrInvalid
	}
	return b.nextUntil(ctx, tenant, topic, name, wait)
}

func (b *Broker) claimLocked(tenant, topic, name string, deadline time.Time) (*Claim, time.Time, <-chan struct{}, error) {
	g, err := b.groupLocked(tenant, topic, name)
	if err != nil {
		return nil, deadline, b.changed, err
	}
	claim, wake := b.claimAvailableLocked(g, time.Now(), deadline)
	return claim, wake, b.changed, nil
}

// Settle advances an offset on ACK or delays redelivery on retry. Stale tokens fail.
func (b *Broker) Settle(tenant, topic, name string, partition uint32, receipt string, retry bool, delay time.Duration) error {
	if !validName(topic) || !validName(name) || partition >= b.cfg.Partitions || receipt == "" || delay < 0 || delay > 5*time.Minute {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failure != nil {
		return b.failure
	}
	return b.settleGroupLocked(b.state.Groups[groupKey(tenant, topic, name)], partition, receipt, retry, delay)
}

func (b *Broker) restore() error {
	if b.store == nil {
		return nil
	}
	data, err := b.store.Load()
	if err != nil {
		return fmt.Errorf("load broker state: %w", err)
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &b.state); err != nil {
			return fmt.Errorf("decode broker state: %w", err)
		}
	}
	return nil
}

func (b *Broker) validateTopics() error {
	for topic, partitions := range b.state.Topics {
		if !validName(topic) || partitions == nil {
			return ErrInvalid
		}
		for partition, log := range partitions {
			if err := b.validatePartition(topic, partition, log); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Broker) validatePartition(topic string, partition uint32, log *partitionLog) error {
	if partition >= b.cfg.Partitions || log == nil {
		return ErrInvalid
	}
	for i, r := range log.Records {
		if r.Topic != topic || r.Partition != partition || r.Offset != log.Base+uint64(i) || !validName(r.TenantID) || len(r.Key) == 0 || len(r.Key) > 1024 || len(r.Payload) > 1<<20 || !json.Valid(r.Payload) {
			return ErrInvalid
		}
		b.count++
		b.bytes += int64(len(r.Payload))
	}
	return nil
}

func (b *Broker) validateGroups() error {
	for key, g := range b.state.Groups {
		if err := b.validateGroup(key, g); err != nil {
			return err
		}
		g.Leases = make(map[uint32]Claim)
	}
	return nil
}

func (b *Broker) validateGroup(key string, g *group) error {
	if g == nil || g.Offsets == nil || !validName(g.Topic) || !validName(g.Name) || key != groupKey(g.TenantID, g.Topic, g.Name) {
		return ErrInvalid
	}
	for partition, offset := range g.Offsets {
		log := b.state.Topics[g.Topic][partition]
		if partition >= b.cfg.Partitions || log == nil || offset > log.Base+uint64(len(log.Records)) {
			return ErrInvalid
		}
	}
	return nil
}

func (b *Broker) capacityLimits(input bool) (int, int64) {
	messages, bytes := b.cfg.MaxMessages, b.cfg.MaxBytes
	if input {
		messages -= max(1, messages/4)
		bytes -= bytes / 4
	}
	return messages, bytes
}

func (b *Broker) reserveCapacityLocked(size int, input bool) error {
	if b.failure != nil {
		return b.failure
	}
	messages, bytes := b.capacityLimits(input)
	if b.count >= messages || b.bytes+int64(size) > bytes {
		b.compactLocked()
	}
	if b.count >= messages || b.bytes+int64(size) > bytes {
		return ErrFull
	}
	return nil
}

func (b *Broker) topicLocked(topic string) (map[uint32]*partitionLog, error) {
	if partitions := b.state.Topics[topic]; partitions != nil {
		return partitions, nil
	}
	if len(b.state.Topics) >= 128 {
		return nil, ErrFull
	}
	partitions := make(map[uint32]*partitionLog)
	b.state.Topics[topic] = partitions
	return partitions, nil
}

func (b *Broker) partitionLocked(topic string, partition uint32) (*partitionLog, error) {
	partitions, err := b.topicLocked(topic)
	if err != nil {
		return nil, err
	}
	if partitions[partition] == nil {
		partitions[partition] = &partitionLog{}
	}
	return partitions[partition], nil
}

func (b *Broker) appendLocked(log *partitionLog, record Record) (Record, error) {
	log.Records = append(log.Records, record)
	b.count++
	b.bytes += int64(len(record.Payload))
	if err := b.persistLocked(); err != nil {
		return Record{}, err
	}
	b.signalLocked()
	record.Payload = append(json.RawMessage{}, record.Payload...)
	return record, nil
}

func (b *Broker) compactPartitionLocked(log *partitionLog) {
	removed := 0
	for _, record := range log.Records {
		if !b.consumedLocked(record) {
			break
		}
		removed++
		b.count--
		b.bytes -= int64(len(record.Payload))
	}
	if removed > 0 {
		log.Base += uint64(removed)
		log.Records = append([]Record{}, log.Records[removed:]...)
	}
}

func (b *Broker) createGroupLocked(key, tenant, topic, name string) (*group, error) {
	if len(b.state.Groups) >= 1024 {
		return nil, ErrFull
	}
	g := &group{TenantID: tenant, Topic: topic, Name: name, Offsets: make(map[uint32]uint64), Leases: make(map[uint32]Claim)}
	b.state.Groups[key] = g
	if err := b.persistLocked(); err != nil {
		return nil, err
	}
	return g, nil
}

func (b *Broker) tryClaim(ctx context.Context, tenant, topic, name string, deadline time.Time) (*Claim, time.Time, <-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, deadline, nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.claimLocked(tenant, topic, name, deadline)
}

func waitForChange(ctx context.Context, wake time.Time, changed <-chan struct{}) error {
	timer := time.NewTimer(time.Until(wake))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
	case <-timer.C:
	}
	return nil
}

func (b *Broker) claimAvailableLocked(g *group, now, wake time.Time) (*Claim, time.Time) {
	for i := uint32(0); i < b.cfg.Partitions; i++ {
		p := (g.Cursor + i) % b.cfg.Partitions
		if lease, ok := g.Leases[p]; ok && now.Before(lease.ExpiresAt) {
			if lease.ExpiresAt.Before(wake) {
				wake = lease.ExpiresAt
			}
			continue
		}
		if claim := b.claimPartitionLocked(g, p, now); claim != nil {
			return claim, wake
		}
	}
	return nil, wake
}

func (b *Broker) claimPartitionLocked(g *group, partition uint32, now time.Time) *Claim {
	log := b.state.Topics[g.Topic][partition]
	if log == nil {
		return nil
	}
	for offset := max(g.Offsets[partition], log.Base); offset < log.Base+uint64(len(log.Records)); offset++ {
		record := log.Records[offset-log.Base]
		if g.TenantID != "" && record.TenantID != g.TenantID {
			continue
		}
		return b.leaseRecordLocked(g, record, now)
	}
	return nil
}

func (b *Broker) leaseRecordLocked(g *group, record Record, now time.Time) *Claim {
	record.Payload = append(json.RawMessage{}, record.Payload...)
	claim := Claim{Record: record, Group: g.Name, Receipt: uuid.NewString(), ExpiresAt: now.Add(b.cfg.AckWait)}
	lease := claim
	lease.Record.Payload = nil
	g.Leases[record.Partition] = lease
	g.Cursor = (record.Partition + 1) % b.cfg.Partitions
	return &claim
}

func (b *Broker) settleGroupLocked(g *group, partition uint32, receipt string, retry bool, delay time.Duration) error {
	if g == nil {
		return ErrReceipt
	}
	claim, ok := g.Leases[partition]
	if !ok || claim.Receipt != receipt || !time.Now().Before(claim.ExpiresAt) {
		return ErrReceipt
	}
	if err := b.settleClaimLocked(g, claim, retry, delay); err != nil {
		return err
	}
	b.signalLocked()
	return nil
}

func (b *Broker) settleClaimLocked(g *group, claim Claim, retry bool, delay time.Duration) error {
	partition := claim.Record.Partition
	if retry {
		claim.Receipt = ""
		claim.ExpiresAt = time.Now().Add(delay)
		g.Leases[partition] = claim
		return nil
	}
	g.Offsets[partition] = claim.Record.Offset + 1
	delete(g.Leases, partition)
	return b.persistLocked()
}

func (b *Broker) publishPartitionLocked(topic, tenant, key string, payload json.RawMessage, partition uint32) (Record, error) {
	log, err := b.partitionLocked(topic, partition)
	if err != nil {
		return Record{}, err
	}
	record := Record{Topic: topic, Partition: partition, Offset: log.Base + uint64(len(log.Records)), TenantID: tenant, Key: key, Payload: append(json.RawMessage{}, payload...)}
	return b.appendLocked(log, record)
}

func (b *Broker) nextUntil(ctx context.Context, tenant, topic, name string, wait time.Duration) (*Claim, error) {
	deadline := time.Now().Add(wait)
	for {
		claim, wake, changed, err := b.tryClaim(ctx, tenant, topic, name, deadline)
		if err != nil || claim != nil {
			return claim, err
		}
		if wait == 0 || !time.Now().Before(deadline) {
			return nil, nil
		}
		if err := waitForChange(ctx, wake, changed); err != nil {
			return nil, err
		}
	}
}
