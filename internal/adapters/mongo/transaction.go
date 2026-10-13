package mongo

import (
	"context"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/flowrulz/flow-rulz/internal/ports"
)

// TransactionManager manages MongoDB transactions.
type TransactionManager struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewTransactionManager creates a new TransactionManager.
func NewTransactionManager(client *mongo.Client, db *mongo.Database) *TransactionManager {
	return &TransactionManager{client: client, db: db}
}

// Begin starts a new MongoDB transaction.
func (m *TransactionManager) Begin(ctx context.Context) (ports.Transaction, error) {
	session, err := m.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	txOpts := options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority"})

	err = session.StartTransaction(txOpts)
	if err != nil {
		session.EndSession(context.Background()) //nolint:contextcheck // cleanup on error
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &Transaction{
		session: session,
		ctx:     mongo.NewSessionContext(ctx, session),
		db:      m.db,
	}, nil
}

// WithTransaction executes a function within a MongoDB transaction.
func (m *TransactionManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(context.Background()) //nolint:contextcheck // cleanup must proceed even if parent context is cancelled

	txOpts := options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority"})

	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) { //nolint:contextcheck // session context passed to fn
		return nil, fn(sc)
	}, txOpts) //nolint:contextcheck // session context is intentionally used for transaction operations
	return err
}

// Transaction wraps a MongoDB session for transaction operations.
type Transaction struct {
	session      mongo.Session
	fencingToken int64
	ctx          mongo.SessionContext
	db           *mongo.Database
	mu           sync.Mutex
	done         bool
}

// Commit commits the transaction.
func (t *Transaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	err := t.session.CommitTransaction(ctx)
	if err == nil {
		t.done = true
		t.session.EndSession(context.Background()) //nolint:contextcheck // cleanup on commit
	}
	return err
}

// Rollback aborts the transaction.
func (t *Transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	err := t.session.AbortTransaction(ctx)
	t.done = true
	t.session.EndSession(context.Background()) //nolint:contextcheck // cleanup on rollback
	return err
}

// Context returns the transaction context.
func (t *Transaction) Context() context.Context {
	return t.ctx
}

// FencingToken returns the current fencing token.
func (t *Transaction) FencingToken() int64 {
	return t.fencingToken
}

// SetFencingToken sets the fencing token.
func (t *Transaction) SetFencingToken(token int64) {
	t.fencingToken = token
}

// Session returns the underlying MongoDB session
func (t *Transaction) Session() mongo.Session {
	return t.session
}

// Database returns the MongoDB database.
func (t *Transaction) Database() *mongo.Database {
	return t.db
}

// Querier interface for repositories
type Querier interface {
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
	Session() mongo.Session
}

type dbQuerier struct {
	db *mongo.Database
}

func (q *dbQuerier) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return q.db.Collection(name, opts...)
}

func (q *dbQuerier) Session() mongo.Session {
	return nil
}

type txQuerier struct {
	session mongo.Session
	db      *mongo.Database
}

func (q *txQuerier) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return q.db.Collection(name, opts...)
}

func (q *txQuerier) Session() mongo.Session {
	return q.session
}

// NewQuerier creates a Querier for database operations.
func NewQuerier(db *mongo.Database) Querier {
	return &dbQuerier{db: db}
}

// NewTxQuerier creates a Querier for transaction operations.
func NewTxQuerier(session mongo.Session, db *mongo.Database) Querier {
	return &txQuerier{session: session, db: db}
}

// TxWrapper wraps a Transaction to implement the Querier interface.
type TxWrapper struct {
	*Transaction
}

// Collection returns a MongoDB collection.
func (t *TxWrapper) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return t.db.Collection(name, opts...)
}

// Session returns the MongoDB session.
func (t *TxWrapper) Session() mongo.Session {
	return t.session
}

// WrapTx wraps a Transaction as a Querier.
func WrapTx(tx ports.Transaction) Querier {
	if mw, ok := tx.(*Transaction); ok {
		return &TxWrapper{mw}
	}
	return nil
}
