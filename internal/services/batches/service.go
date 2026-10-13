// Package batches provides batch processing for events.
// It groups events by tenant, partition key, and rule set, and processes them when
// batch criteria (size or time window) are met.
package batches

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service processes batched events based on configured batch mode.
// It groups events by tenant, partition key, and rule set, and processes them
// when batch criteria (accumulation size or time window) are satisfied.
type Service struct {
	batches       ports.BatchRepository
	events        ports.EventProcessor
	clock         ports.Clock
	cfg           domain.BatchConfig
	leaseProvider ports.ShardLeaseView
	workerID      string
	numShards     uint32
}

// NewService creates a new batch Service with the given dependencies.
func NewService(
	batches ports.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
) *Service {
	return &Service{
		batches:   batches,
		events:    events,
		clock:     clock,
		cfg:       cfg,
		numShards: 4096,
	}
}

// NewServiceWithLeaseProvider creates a batch Service with lease provider
// for fencing token validation. Batch events must obey the same lease invariant
// as ordinary events; a synthetic token or fencing bypass is never permitted.
func NewServiceWithLeaseProvider(
	batches ports.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
	leaseProvider ports.ShardLeaseView,
	workerID string,
	numShards uint32,
) *Service {
	service := NewService(batches, events, clock, cfg)
	service.leaseProvider = leaseProvider
	service.workerID = workerID
	if numShards > 0 {
		service.numShards = numShards
	}
	return service
}

func (s *Service) ownsPartition(tenantID, partitionKey string) bool {
	if s.leaseProvider == nil || tenantID == "" || partitionKey == "" {
		return false
	}
	envelope := &domain.EventEnvelope{TenantID: tenantID, PartitionKey: partitionKey}
	return s.leaseProvider.IsOwner(envelope.VirtualShard(s.numShards))
}

type batchGroupKey struct {
	tenantID     string
	partitionKey string
	ruleSet      string
}

// Tick processes due batches based on the configured batch mode.
// Returns the number of batches formed and an error if any.
func (s *Service) Tick(ctx context.Context) (int, error) {
	if s.cfg.Mode == domain.BatchModeNone {
		return 0, nil
	}
	entries, err := s.loadUnbatchedEntries(ctx)
	if err != nil {
		return 0, err
	}
	groups, order := groupInboxEntries(entries)
	return s.processReadyGroups(ctx, groups, order)
}

func (s *Service) loadUnbatchedEntries(ctx context.Context) ([]*domain.InboxEntry, error) {
	limit := s.cfg.MaxBatch * 10
	if limit <= 0 {
		limit = 400
	}
	entries, err := s.batches.ListUnbatched(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("list unbatched: %w", err)
	}
	return entries, nil
}

func groupInboxEntries(entries []*domain.InboxEntry) (map[batchGroupKey][]*domain.InboxEntry, []batchGroupKey) {
	groups := make(map[batchGroupKey][]*domain.InboxEntry)
	order := make([]batchGroupKey, 0)
	for _, entry := range entries {
		if entry.TenantID == "" || entry.PartitionKey == "" || entry.RuleSet == "" {
			continue
		}
		key := batchGroupKey{tenantID: entry.TenantID, partitionKey: entry.PartitionKey, ruleSet: entry.RuleSet}
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], entry)
	}
	return groups, order
}

func (s *Service) processReadyGroups(
	ctx context.Context,
	groups map[batchGroupKey][]*domain.InboxEntry,
	order []batchGroupKey,
) (int, error) {
	formed := 0
	for _, key := range order {
		members := groups[key]
		sort.SliceStable(members, func(i, j int) bool {
			return members[i].FirstSeenAt.Before(members[j].FirstSeenAt)
		})
		if !s.ready(members) {
			continue
		}
		if !s.ownsPartition(members[0].TenantID, members[0].PartitionKey) {
			continue
		}
		count, err := s.processGroup(ctx, members)
		if err != nil {
			return formed, err
		}
		formed += count
	}
	return formed, nil
}

// CreateBatch creates a new batch from unbatched entries (manual batch formation)
func (s *Service) CreateBatch(
	ctx context.Context,
	tenantID, partitionKey, ruleSet string,
	eventIDs []string,
) (*domain.BatchRun, error) {
	if err := s.validateManualBatchRequest(tenantID, partitionKey, ruleSet, eventIDs); err != nil {
		return nil, err
	}
	members, err := s.loadManualBatchMembers(ctx, tenantID, partitionKey, ruleSet, eventIDs)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(members, func(i, j int) bool {
		return members[i].FirstSeenAt.Before(members[j].FirstSeenAt)
	})
	// Batch IDs are order-independent, so use the same sorted membership order
	// when selecting and processing the batch. This also keeps tie ordering stable.
	if _, err := s.processGroup(ctx, members); err != nil {
		return nil, err
	}

	batchID := domain.ComputeBatchID(tenantID, partitionKey, ruleSet, eventIDs)
	return s.batches.GetRun(ctx, batchID)
}

func (s *Service) validateManualBatchRequest(
	tenantID, partitionKey, ruleSet string,
	eventIDs []string,
) error {
	if s.events == nil {
		return domain.ErrBatchProcessingUnavailable
	}
	if len(eventIDs) == 0 {
		return fmt.Errorf("no event IDs provided")
	}
	if tenantID == "" || partitionKey == "" || ruleSet == "" {
		return fmt.Errorf("tenant, partition key, and rule set are required")
	}
	if s.cfg.MaxBatch > 0 && len(eventIDs) > s.cfg.MaxBatch {
		return fmt.Errorf("requested batch size %d exceeds maximum %d", len(eventIDs), s.cfg.MaxBatch)
	}
	return validateUniqueEventIDs(eventIDs)
}

func validateUniqueEventIDs(eventIDs []string) error {
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if id == "" {
			return fmt.Errorf("event IDs must not be empty")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate event ID %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (s *Service) loadManualBatchMembers(
	ctx context.Context,
	tenantID, partitionKey, ruleSet string,
	eventIDs []string,
) ([]*domain.InboxEntry, error) {
	members, err := s.batches.ListUnbatchedByIDs(ctx, tenantID, partitionKey, ruleSet, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("list requested unbatched events: %w", err)
	}
	if err := validateRequestedBatchMembers(eventIDs, members); err != nil {
		return nil, err
	}
	return members, nil
}

func validateRequestedBatchMembers(eventIDs []string, members []*domain.InboxEntry) error {
	if len(members) == len(eventIDs) {
		return nil
	}
	found := make(map[string]struct{}, len(members))
	for _, member := range members {
		found[member.EventID] = struct{}{}
	}
	for _, id := range eventIDs {
		if _, ok := found[id]; !ok {
			return fmt.Errorf("requested event %q is missing, already batched, or outside the requested scope", id)
		}
	}
	return fmt.Errorf("requested event set is incomplete")
}

// GetBatch retrieves a batch run by ID
func (s *Service) GetBatch(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	return s.batches.GetRunForTenant(ctx, tenantID, batchID)
}

// ListBatches lists batch runs with optional filters
func (s *Service) ListBatches(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	return s.batches.ListRuns(ctx, tenantID, ruleSet, limit, offset)
}

func (s *Service) ready(members []*domain.InboxEntry) bool {
	switch s.cfg.Mode {
	case domain.BatchModeAccumulate:
		return s.cfg.MaxBatch > 0 && len(members) >= s.cfg.MaxBatch
	case domain.BatchModeWindow:
		oldest := members[0].FirstSeenAt
		return !s.clock.Now().Before(oldest.Add(s.cfg.Window))
	}
	return false
}

func (s *Service) processGroup(ctx context.Context, members []*domain.InboxEntry) (int, error) {
	if s.events == nil {
		return 0, domain.ErrBatchProcessingUnavailable
	}
	if len(members) == 0 {
		return 0, nil
	}

	members = s.limitBatchMembers(members)
	input := buildBatchInput(members)
	existing, err := s.batches.GetRun(ctx, input.batchID)
	if err != nil {
		return 0, fmt.Errorf("check existing batch: %w", err)
	}
	if existing != nil && existing.Status == domain.BatchRunStatusCompleted {
		return s.recoverCompletedBatch(ctx, existing, input.eventIDs)
	}

	now := s.clock.Now()
	envelope, windowFrom, err := s.buildBatchEnvelope(input, now)
	if err != nil {
		return 0, err
	}
	execution, err := s.processBatchEnvelope(ctx, envelope)
	if err != nil {
		return 0, err
	}
	if execution == nil {
		return 0, nil
	}

	run := buildCompletedBatchRun(input, execution, windowFrom, now)
	if err := s.batches.CompleteBatch(ctx, run, input.eventIDs); err != nil {
		return 0, fmt.Errorf("complete batch: %w", err)
	}
	return 1, nil
}

func (s *Service) limitBatchMembers(members []*domain.InboxEntry) []*domain.InboxEntry {
	if s.cfg.MaxBatch > 0 && len(members) > s.cfg.MaxBatch {
		return members[:s.cfg.MaxBatch]
	}
	return members
}

type batchInput struct {
	batchID      string
	tenantID     string
	partitionKey string
	ruleSet      string
	eventIDs     []string
	batchEvents  []domain.BatchMember
	members      []*domain.InboxEntry
}

func buildBatchInput(members []*domain.InboxEntry) batchInput {
	first := members[0]
	input := batchInput{
		tenantID: first.TenantID, partitionKey: first.PartitionKey, ruleSet: first.RuleSet,
		eventIDs:    make([]string, 0, len(members)),
		batchEvents: make([]domain.BatchMember, 0, len(members)),
		members:     members,
	}
	for _, member := range members {
		input.eventIDs = append(input.eventIDs, member.EventID)
		input.batchEvents = append(input.batchEvents, domain.BatchMember{
			ID: member.EventID, Type: member.RuleSet, Data: member.Payload,
		})
	}
	input.batchID = domain.ComputeBatchID(input.tenantID, input.partitionKey, input.ruleSet, input.eventIDs)
	return input
}

func (s *Service) recoverCompletedBatch(
	ctx context.Context,
	existing *domain.BatchRun,
	eventIDs []string,
) (int, error) {
	// Processing may commit before membership finalization. Completing the same
	// deterministic batch is idempotent and recovers that interrupted operation.
	if err := s.batches.CompleteBatch(ctx, existing, eventIDs); err != nil {
		return 0, fmt.Errorf("recover completed batch: %w", err)
	}
	return 0, nil
}

func (s *Service) buildBatchEnvelope(input batchInput, now time.Time) (*domain.EventEnvelope, time.Time, error) {
	windowFrom := input.members[0].FirstSeenAt
	payload := domain.BatchData{
		BatchID: input.batchID, TenantID: input.tenantID, PartitionKey: input.partitionKey,
		RuleSet: input.ruleSet, WindowFrom: windowFrom, WindowTo: now, Events: input.batchEvents,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("marshal batch data: %w", err)
	}
	envelope := &domain.EventEnvelope{
		ID: input.batchID, Type: domain.BatchRuleSet(input.ruleSet), TenantID: input.tenantID,
		PartitionKey: input.partitionKey, OccurredAt: now, Data: data,
	}
	return envelope, windowFrom, nil
}

func (s *Service) processBatchEnvelope(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error) {
	if s.leaseProvider == nil || s.workerID == "" {
		return nil, fmt.Errorf("batch processing requires a configured shard lease provider and worker ID")
	}
	shard := envelope.VirtualShard(s.numShards)
	if !s.leaseProvider.IsOwner(shard) {
		return nil, domain.ErrFencingTokenMismatch
	}
	fencingToken, ok := s.leaseProvider.GetFencingToken(shard)
	if !ok || fencingToken <= 0 {
		return nil, domain.ErrFencingTokenMismatch
	}
	execution, err := s.events.Process(ctx, envelope, fencingToken, shard, s.workerID)
	if err != nil {
		if errors.Is(err, domain.ErrRuleNotActive) || errors.Is(err, domain.ErrRuleNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("process batch: %w", err)
	}
	return execution, nil
}

func buildCompletedBatchRun(
	input batchInput,
	execution *domain.Execution,
	windowFrom, now time.Time,
) *domain.BatchRun {
	return &domain.BatchRun{
		BatchID: input.batchID, TenantID: input.tenantID, PartitionKey: input.partitionKey,
		RuleSet: input.ruleSet, Status: domain.BatchRunStatusCompleted, MemberCount: len(input.members),
		ExecutionID: execution.ID, DecisionHash: execution.DecisionHash,
		WindowFrom: &windowFrom, WindowTo: &now, CreatedAt: now,
	}
}
