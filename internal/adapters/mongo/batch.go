// Package mongo provides MongoDB adapters for FlowRulz repositories.
package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BatchRepository handles batch-related persistence operations in MongoDB.
type BatchRepository struct {
	querier Querier
}

// NewBatchRepository creates a new BatchRepository.
func NewBatchRepository(q Querier) *BatchRepository {
	return &BatchRepository{querier: q}
}

// SaveRun upserts a batch run.
func (r *BatchRepository) SaveRun(ctx context.Context, run *domain.BatchRun) error {
	coll := r.collection(collectionBatchRuns)
	doc := BatchRunDocFromDomain(run)
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"batch_id":      run.BatchID,
			"tenant_id":     run.TenantID,
			"partition_key": run.PartitionKey,
			"rule_set":      run.RuleSet,
		},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save batch run: %w", err)
	}
	return nil
}

// GetRun retrieves a batch run by ID.
func (r *BatchRepository) GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	coll := r.collection(collectionBatchRuns)
	var doc BatchRunDoc
	err := coll.FindOne(ctx, bson.M{"batch_id": batchID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run: %w", err)
	}
	return doc.ToDomain(), nil
}

// GetRunForTenant retrieves a batch run by ID and tenant.
func (r *BatchRepository) GetRunForTenant(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	coll := r.collection(collectionBatchRuns)
	var doc BatchRunDoc
	err := coll.FindOne(ctx, bson.M{"tenant_id": tenantID, "batch_id": batchID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run for tenant: %w", err)
	}
	return doc.ToDomain(), nil
}

// batchRunListFilter always scopes batch listing to one tenant. An absent tenant
// must fail closed rather than turn a tenant-scoped query into a global query.
func batchRunListFilter(tenantID, ruleSet string) (bson.M, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant ID is required to list batch runs")
	}
	filter := bson.M{"tenant_id": tenantID}
	if ruleSet != "" {
		filter["rule_set"] = ruleSet
	}
	return filter, nil
}

// ListRuns returns batch runs for a tenant, optionally filtered by rule set.
func (r *BatchRepository) ListRuns(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	filter, err := batchRunListFilter(tenantID, ruleSet)
	if err != nil {
		return nil, err
	}
	cursor, err := r.collection(collectionBatchRuns).Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetSkip(int64(offset)).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("list batch runs: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var runs []*domain.BatchRun
	for cursor.Next(ctx) {
		var doc BatchRunDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode batch run: %w", err)
		}
		runs = append(runs, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}
	return runs, nil
}

// ListUnbatched returns up to limit unbatched committed inbox entries.
func (r *BatchRepository) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	coll := r.collection(collectionInbox)
	filter := bson.M{
		"status":   "committed",
		"batch_id": bson.M{"$exists": false},
	}
	opts := options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "first_seen_at", Value: 1}})
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("list unbatched: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*domain.InboxEntry
	for cursor.Next(ctx) {
		var doc InboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode inbox entry: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}
	return entries, nil
}

// ListUnbatchedByIDs returns requested eligible events in one tenant partition.
func (r *BatchRepository) ListUnbatchedByIDs(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error) {
	coll := r.collection(collectionInbox)
	filter := bson.M{
		"tenant_id":     tenantID,
		"partition_key": partitionKey,
		"rule_set":      ruleSet,
		"event_id":      bson.M{"$in": eventIDs},
		"status":        "committed",
		"batch_id":      bson.M{"$exists": false},
	}
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list unbatched by ids: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*domain.InboxEntry
	for cursor.Next(ctx) {
		var doc InboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode inbox entry: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}
	return entries, nil
}

// MarkBatched marks an inbox entry as belonging to a batch.
func (r *BatchRepository) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	coll := r.collection(collectionInbox)
	_, err := coll.UpdateOne(ctx,
		bson.M{"tenant_id": tenantID, "event_id": eventID},
		bson.M{"$set": bson.M{"batch_id": batchID}})
	if err != nil {
		return fmt.Errorf("mark batched: %w", err)
	}
	return nil
}

// CompleteBatch finalizes a batch run and marks its member events as batched.
// It runs in a single MongoDB transaction to ensure atomicity.
func (r *BatchRepository) CompleteBatch(ctx context.Context, run *domain.BatchRun, eventIDs []string) error {
	client := r.collection(collectionInbox).Database().Client()
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start batch completion session: %w", err)
	}
	defer session.EndSession(ctx)

	// nolint:contextcheck // txnCtx is passed to all database operations within the transaction
	_, err = session.WithTransaction(ctx, func(txnCtx mongo.SessionContext) (interface{}, error) {
		runs := r.collection(collectionBatchRuns)
		doc := BatchRunDocFromDomain(run)
		_, err := runs.UpdateOne(txnCtx,
			bson.M{
				"batch_id":      run.BatchID,
				"tenant_id":     run.TenantID,
				"partition_key": run.PartitionKey,
				"rule_set":      run.RuleSet,
			},
			bson.M{"$set": doc},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return nil, fmt.Errorf("upsert batch run: %w", err)
		}

		filter := bson.M{
			"tenant_id":     run.TenantID,
			"partition_key": run.PartitionKey,
			"rule_set":      run.RuleSet,
			"event_id":      bson.M{"$in": eventIDs},
			"status":        "committed",
			"$or": []bson.M{
				{"batch_id": bson.M{"$exists": false}},
				{"batch_id": run.BatchID},
			},
		}
		result, err := r.collection(collectionInbox).UpdateMany(txnCtx, filter,
			bson.M{"$set": bson.M{"batch_id": run.BatchID}},
		)
		if err != nil {
			return nil, fmt.Errorf("associate batch members: %w", err)
		}
		if result.MatchedCount != int64(len(eventIDs)) {
			return nil, fmt.Errorf("batch membership conflict: matched %d of %d requested events", result.MatchedCount, len(eventIDs))
		}
		return nil, nil
	})
	return err
}

func (r *BatchRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// BatchRunDoc is the MongoDB representation of a batch run.
type BatchRunDoc struct {
	BatchID      string     `bson:"batch_id"`
	TenantID     string     `bson:"tenant_id"`
	PartitionKey string     `bson:"partition_key"`
	RuleSet      string     `bson:"rule_set"`
	Status       string     `bson:"status"`
	MemberCount  int        `bson:"member_count"`
	ExecutionID  string     `bson:"execution_id,omitempty"`
	DecisionHash string     `bson:"decision_hash,omitempty"`
	WindowFrom   *time.Time `bson:"window_from,omitempty"`
	WindowTo     *time.Time `bson:"window_to,omitempty"`
	CreatedAt    time.Time  `bson:"created_at"`
}

// BatchRunDocFromDomain converts a domain batch run to its MongoDB representation.
func BatchRunDocFromDomain(run *domain.BatchRun) BatchRunDoc {
	return BatchRunDoc{
		BatchID:      run.BatchID,
		TenantID:     run.TenantID,
		PartitionKey: run.PartitionKey,
		RuleSet:      run.RuleSet,
		Status:       run.Status,
		MemberCount:  run.MemberCount,
		ExecutionID:  run.ExecutionID,
		DecisionHash: run.DecisionHash,
		WindowFrom:   run.WindowFrom,
		WindowTo:     run.WindowTo,
		CreatedAt:    run.CreatedAt,
	}
}

// ToDomain converts the MongoDB batch run document to a domain batch run.
func (d *BatchRunDoc) ToDomain() *domain.BatchRun {
	return &domain.BatchRun{
		BatchID:      d.BatchID,
		TenantID:     d.TenantID,
		PartitionKey: d.PartitionKey,
		RuleSet:      d.RuleSet,
		Status:       d.Status,
		MemberCount:  d.MemberCount,
		ExecutionID:  d.ExecutionID,
		DecisionHash: d.DecisionHash,
		WindowFrom:   d.WindowFrom,
		WindowTo:     d.WindowTo,
		CreatedAt:    d.CreatedAt,
	}
}
