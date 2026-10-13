package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DB wraps a MongoDB client and database.
type DB struct {
	client   *mongo.Client
	database *mongo.Database
}

// Config holds MongoDB connection configuration.
type Config struct {
	URI         string
	Database    string
	MaxPool     uint64
	MinPool     uint64
	MaxConnIdle time.Duration
}

// New creates a new MongoDB connection pool.
func New(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.URI == "" {
		cfg.URI = "mongodb://localhost:27017"
	}
	if cfg.Database == "" {
		cfg.Database = "flowrulz"
	}
	if cfg.MaxPool == 0 {
		cfg.MaxPool = 20
	}
	if cfg.MinPool == 0 {
		cfg.MinPool = 2
	}
	if cfg.MaxConnIdle == 0 {
		cfg.MaxConnIdle = 5 * time.Minute
	}

	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPool).
		SetMinPoolSize(cfg.MinPool).
		SetMaxConnIdleTime(cfg.MaxConnIdle)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background()) //nolint:contextcheck // cleanup must proceed even if parent context is cancelled
		return nil, fmt.Errorf("ping mongo: %w", err)
	}

	db := client.Database(cfg.Database)

	// Create indexes
	if err := createIndexes(ctx, db); err != nil {
		_ = client.Disconnect(context.Background()) //nolint:contextcheck // cleanup must proceed even if parent context is cancelled
		return nil, fmt.Errorf("create indexes: %w", err)
	}
	if err := verifyTransactionSupport(ctx, client, db); err != nil {
		_ = client.Disconnect(context.Background()) //nolint:contextcheck // cleanup must proceed even if parent context is cancelled
		return nil, fmt.Errorf("verify MongoDB transaction support (use a replica set or sharded cluster): %w", err)
	}

	return &DB{client: client, database: db}, nil
}

func verifyTransactionSupport(ctx context.Context, client *mongo.Client, db *mongo.Database) error {
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start transaction check session: %w", err)
	}
	defer session.EndSession(context.Background()) //nolint:contextcheck // cleanup must proceed even if parent context is cancelled

	sessionCtx := mongo.NewSessionContext(ctx, session)
	if err := session.StartTransaction(); err != nil {
		return fmt.Errorf("start transaction check: %w", err)
	}

	probeID := fmt.Sprintf("startup-check-%d", time.Now().UnixNano())
	collection := db.Collection(collectionTransactionChecks)
	if _, err := collection.InsertOne(sessionCtx, bson.M{"_id": probeID}); err != nil { //nolint:contextcheck // sessionCtx derived from parent ctx
		_ = session.AbortTransaction(sessionCtx) //nolint:contextcheck // sessionCtx derived from parent ctx
		return fmt.Errorf("insert transaction check record: %w", err)
	}
	if _, err := collection.DeleteOne(sessionCtx, bson.M{"_id": probeID}); err != nil { //nolint:contextcheck // sessionCtx derived from parent ctx
		_ = session.AbortTransaction(sessionCtx) //nolint:contextcheck // sessionCtx derived from parent ctx
		return fmt.Errorf("delete transaction check record: %w", err)
	}
	if err := session.CommitTransaction(sessionCtx); err != nil { //nolint:contextcheck // sessionCtx derived from parent ctx
		return fmt.Errorf("commit transaction check: %w", err)
	}
	return nil
}

// Close disconnects the MongoDB client.
func (d *DB) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.client.Disconnect(ctx)
}

// Database returns the configured MongoDB database.
func (d *DB) Database() *mongo.Database {
	return d.database
}

// Client returns the underlying MongoDB client.
func (d *DB) Client() *mongo.Client {
	return d.client
}

func createIndexes(ctx context.Context, db *mongo.Database) error {
	for collName, models := range indexModels() {
		coll := db.Collection(collName)
		_, err := coll.Indexes().CreateMany(ctx, models)
		if err != nil {
			return fmt.Errorf("create indexes for %s: %w", collName, err)
		}
	}
	return nil
}

func indexModels() map[string][]mongo.IndexModel {
	indexes := map[string][]mongo.IndexModel{
		"rule_revisions": {
			{Keys: bson.D{{Key: "tenant_scope", Value: 1}, {Key: "rule_id", Value: 1}, {Key: "revision", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "content_hash", Value: 1}}},
		},
		"rule_activations": {
			{Keys: bson.D{{Key: "tenant_scope", Value: 1}, {Key: "rule_set", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		"inbox": {
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "partition_key", Value: 1}, {Key: "first_seen_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "committed", "batch_id": bson.M{"$exists": false}})},
		},
		"executions": {
			{Keys: bson.D{{Key: "execution_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}}},
		},
		"outbox_effects": {
			{Keys: bson.D{{Key: "effect_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "execution_id", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "available_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "claim_expires_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "claimed"})},
		},
		"shard_leases": {
			{Keys: bson.D{{Key: "virtual_shard", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "owner", Value: 1}}},
			{Keys: bson.D{{Key: "expires_at", Value: 1}}},
		},
		"quarantine": {
			{Keys: bson.D{{Key: "quarantine_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: 1}}},
		},
		"batch_runs": {
			{Keys: bson.D{{Key: "batch_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: 1}}},
		},
		"scheduled_events": {
			{Keys: bson.D{{Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "scheduled_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "pending"})},
		},
		"workflow_instances": {
			{Keys: bson.D{{Key: "workflow_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "state", Value: 1}}},
			{Keys: bson.D{{Key: "updated_at", Value: 1}}},
		},
		"workflow_definitions": {
			{Keys: bson.D{{Key: "workflow_type", Value: 1}, {Key: "version", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		"contract_schemas": {
			{Keys: bson.D{{Key: "name", Value: 1}, {Key: "version", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		"tenant_domains": {
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "domain_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "event_types", Value: 1}}},
		},
		"domain_services": {
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "service_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "active", Value: 1}, {Key: "domain_ids", Value: 1}}},
		},
	}
	return indexes
}
