package flowrulz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Client is the main FlowRulz client
type Client struct {
	cfg            *Config
	transport      Transport
	contracts      *ContractRegistry
	lifecycleCtx   context.Context
	lifecycleClose context.CancelFunc
	mu             sync.RWMutex
	closed         bool
}

// EventHandler processes a matching event received from the configured broker.
type EventHandler func(ctx context.Context, envelope *Envelope) error

type eventSubscriber interface {
	Subscribe(ctx context.Context, ruleSet string, handler EventHandler) error
}

// SendOptions configures a send operation
type SendOptions struct {
	RuleSet       string            // Required: the rule set to evaluate against
	Payload       any               // Required: the event payload
	PartitionKey  string            // Optional: explicit partition key
	WorkflowID    string            // Optional: workflow ID
	CorrelationID string            // Optional: correlation ID
	CausationID   string            // Optional: causation ID
	Headers       map[string]string // Optional: additional headers
	SchemaVersion string            // Optional: schema version
	Opts          []EnvelopeOption  // Optional: envelope options
}

// New creates a new client from environment variables
func New() (*Client, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	return NewWithConfig(cfg)
}

// NewWithConfig creates a new client with explicit configuration
func NewWithConfig(cfg *Config) (*Client, error) {
	cfg = cfg.WithDefaults()

	transport, err := NewTransport(cfg)
	if err != nil {
		return nil, err
	}

	lifecycleCtx, lifecycleClose := context.WithCancel(context.Background())
	client := &Client{
		cfg:            cfg,
		transport:      transport,
		lifecycleCtx:   lifecycleCtx,
		lifecycleClose: lifecycleClose,
	}

	// Initialize contract registry if validation enabled
	if cfg.ValidateContracts {
		client.contracts, err = NewContractRegistry(cfg.ContractRegistry)
		if err != nil {
			lifecycleClose()
			transport.Close()
			return nil, fmt.Errorf("init contract registry: %w", err)
		}
	}

	return client, nil
}

// Send sends an event for the given rule set with type-safe payload
// The payload is marshaled to JSON and sent via the configured transport.
// Partition key is auto-extracted from payload if not provided.
//
// Example:
//
//	fs.Send(ctx, "order.created", OrderCreated{
//	    OrderID: "ord-123",
//	    Total:   99.99,
//	    CustomerID: "cust-456",
//	})
func (c *Client) Send(ctx context.Context, ruleSet string, payload any, opts ...SendOption) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	transport := c.transport
	cfg := c.cfg
	contracts := c.contracts
	c.mu.RUnlock()

	// Apply options
	options := SendOptions{
		RuleSet: ruleSet,
		Payload: payload,
	}
	for _, opt := range opts {
		opt(&options)
	}

	// Validate
	if options.RuleSet == "" {
		return ErrInvalidArgument("rule set is required")
	}
	if options.Payload == nil {
		return ErrInvalidArgument("payload is required")
	}

	// Validate contract if enabled
	if contracts != nil {
		if err := contracts.Validate(options.RuleSet, options.Payload); err != nil {
			return fmt.Errorf("contract validation failed: %w", err)
		}
	}

	// Marshal payload
	data, err := json.Marshal(options.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// Determine partition key
	partitionKey := options.PartitionKey
	if partitionKey == "" {
		partitionKey = extractPartitionKey(options.Payload, cfg.PartitionKeyExtractor)
		if partitionKey == "" {
			return ErrInvalidArgument("partition key is required (set explicitly or configure extractor)")
		}
	}

	// Create envelope
	env := &Envelope{
		ID:            uuid.New().String(),
		Type:          options.RuleSet,
		TenantID:      cfg.TenantID,
		PartitionKey:  partitionKey,
		WorkflowID:    options.WorkflowID,
		CorrelationID: options.CorrelationID,
		CausationID:   options.CausationID,
		OccurredAt:    time.Now().UTC(),
		Data:          data,
		Headers:       options.Headers,
		SchemaVersion: options.SchemaVersion,
	}

	// Apply envelope options
	for _, opt := range options.Opts {
		opt(env)
	}

	// Send with retry
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		sendCtx, cancel := context.WithTimeout(ctx, cfg.SendTimeout)
		err = transport.Send(sendCtx, env)
		cancel()

		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < cfg.MaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.RetryBackoff * time.Duration(attempt+1)):
			}
		}
	}

	return fmt.Errorf("send failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// Subscribe handles live events for a rule set from the configured transport.
// Subscriptions currently require NATS and are best-effort: they are not
// durable, do not replay events published while disconnected, and do not ACK
// or retry messages. The handler is called only for the configured tenant.
func (c *Client) Subscribe(ctx context.Context, ruleSet string, handler EventHandler) error {
	if ctx == nil {
		return ErrInvalidArgument("context is required")
	}
	if ruleSet == "" {
		return ErrInvalidArgument("rule set is required")
	}
	if handler == nil {
		return ErrInvalidArgument("event handler is required")
	}

	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	transport := c.transport
	lifecycleCtx := c.lifecycleCtx
	c.mu.RUnlock()

	subscriber, ok := transport.(eventSubscriber)
	if !ok {
		return ErrSubscriptionsUnsupported
	}

	subscribeCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifecycleCtx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	return subscriber.Subscribe(subscribeCtx, ruleSet, handler)
}

// SendBatch sends multiple events in a batch
func (c *Client) SendBatch(ctx context.Context, events []SendOptions) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	transport := c.transport
	cfg := c.cfg
	contracts := c.contracts
	c.mu.RUnlock()

	if len(events) == 0 {
		return nil
	}

	envelopes := make([]*Envelope, len(events))
	for i, opts := range events {
		if opts.RuleSet == "" {
			return ErrInvalidArgument("rule set is required for all events")
		}
		if opts.Payload == nil {
			return ErrInvalidArgument("payload is required for all events")
		}

		if contracts != nil {
			if err := contracts.Validate(opts.RuleSet, opts.Payload); err != nil {
				return fmt.Errorf("contract validation failed for event %d: %w", i, err)
			}
		}

		data, err := json.Marshal(opts.Payload)
		if err != nil {
			return fmt.Errorf("marshal payload %d: %w", i, err)
		}

		partitionKey := opts.PartitionKey
		if partitionKey == "" {
			partitionKey = extractPartitionKey(opts.Payload, cfg.PartitionKeyExtractor)
			if partitionKey == "" {
				return ErrInvalidArgument(fmt.Sprintf("partition key required for event %d", i))
			}
		}

		envelopes[i] = &Envelope{
			ID:            uuid.New().String(),
			Type:          opts.RuleSet,
			TenantID:      cfg.TenantID,
			PartitionKey:  partitionKey,
			WorkflowID:    opts.WorkflowID,
			CorrelationID: opts.CorrelationID,
			CausationID:   opts.CausationID,
			OccurredAt:    time.Now().UTC(),
			Data:          data,
			Headers:       opts.Headers,
			SchemaVersion: opts.SchemaVersion,
		}

		for _, opt := range opts.Opts {
			opt(envelopes[i])
		}
	}

	// Send with retry
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		sendCtx, cancel := context.WithTimeout(ctx, cfg.SendTimeout)
		err := transport.SendBatch(sendCtx, envelopes)
		cancel()

		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < cfg.MaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.RetryBackoff * time.Duration(attempt+1)):
			}
		}
	}

	return fmt.Errorf("batch send failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// SendOption configures a Send operation
type SendOption func(*SendOptions)

// WithPartitionKey sets the partition key explicitly
func WithPartitionKey(key string) SendOption {
	return func(o *SendOptions) {
		o.PartitionKey = key
	}
}

// WithWorkflowID sets the workflow ID
func WithWorkflowID(id string) SendOption {
	return func(o *SendOptions) {
		o.WorkflowID = id
	}
}

// WithCorrelationID sets the correlation ID
func WithCorrelationID(id string) SendOption {
	return func(o *SendOptions) {
		o.CorrelationID = id
	}
}

// WithCausationID sets the causation ID
func WithCausationID(id string) SendOption {
	return func(o *SendOptions) {
		o.CausationID = id
	}
}

// WithHeaders adds headers
func WithHeaders(headers map[string]string) SendOption {
	return func(o *SendOptions) {
		o.Headers = headers
	}
}

// WithSchemaVersion sets the schema version
func WithSchemaVersion(version string) SendOption {
	return func(o *SendOptions) {
		o.SchemaVersion = version
	}
}

// WithEnvelopeOptions adds envelope options
func WithEnvelopeOptions(opts ...EnvelopeOption) SendOption {
	return func(o *SendOptions) {
		o.Opts = opts
	}
}

// Close closes the client and underlying transport
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	if c.lifecycleClose != nil {
		c.lifecycleClose()
	}

	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}

// Transport returns the underlying transport (for advanced use)
func (c *Client) Transport() Transport {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transport
}

// Config returns the client configuration
func (c *Client) Config() *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// ErrClientClosed is returned when using a closed client
var ErrClientClosed = fmt.Errorf("client closed")

// ErrSubscriptionsUnsupported is returned when the configured transport does
// not support live event subscriptions.
var ErrSubscriptionsUnsupported = errors.New("flowrulz: live subscriptions require the NATS transport")

// ErrInvalidArgument is returned for invalid arguments
type ErrInvalidArgument string

func (e ErrInvalidArgument) Error() string {
	return "flowrulz: invalid argument: " + string(e)
}

// extractPartitionKey extracts partition key from payload
func extractPartitionKey(payload any, extractor string) string {
	if extractor == "" {
		// Try common fields
		v := reflect.ValueOf(payload)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				tag := field.Tag.Get("partition_key")
				if tag == "true" {
					val := v.Field(i)
					return fmt.Sprintf("%v", val.Interface())
				}
				// Check common field names
				name := field.Name
				if name == "PartitionKey" || name == "partition_key" || name == "Partition_key" {
					val := v.Field(i)
					return fmt.Sprintf("%v", val.Interface())
				}
			}
		}
		return ""
	}

	// Custom extractor: JSONPath or field name
	v := reflect.ValueOf(payload)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		field := v.FieldByName(extractor)
		if field.IsValid() {
			return fmt.Sprintf("%v", field.Interface())
		}
	}
	return ""
}
