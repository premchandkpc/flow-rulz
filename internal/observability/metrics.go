package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// EventsReceived counts total events received by tenant, type, and status.
	EventsReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_events_received_total",
			Help: "Total number of events received",
		},
		[]string{"tenant", "type", "status"},
	)

	// EventProcessingDuration tracks event processing duration by tenant, type, and rule set.
	EventProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_event_processing_duration_seconds",
			Help:    "Event processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tenant", "type", "rule_set"},
	)

	// ExecutionsTotal counts total rule executions by tenant, rule set, and status.
	ExecutionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_executions_total",
			Help: "Total number of rule executions",
		},
		[]string{"tenant", "rule_set", "status"},
	)

	// ExecutionDuration tracks rule execution duration by tenant and rule set.
	ExecutionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_execution_duration_seconds",
			Help:    "Rule execution duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tenant", "rule_set"},
	)

	// OutboxEffectsPending tracks pending outbox effects by tenant, destination, and effect type.
	OutboxEffectsPending = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrulz_outbox_effects_pending",
			Help: "Number of pending outbox effects",
		},
		[]string{"tenant", "destination", "effect_type"},
	)

	// OutboxDeliveryDuration tracks outbox effect delivery duration by destination and effect type.
	OutboxDeliveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_outbox_delivery_duration_seconds",
			Help:    "Outbox effect delivery duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"destination", "effect_type"},
	)

	// OutboxRetriesTotal counts total outbox effect retries by destination and effect type.
	OutboxRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_outbox_retries_total",
			Help: "Total number of outbox effect retries",
		},
		[]string{"destination", "effect_type"},
	)

	// QuarantineTotal counts total quarantined items by tenant, source type, and error class.
	QuarantineTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_quarantine_total",
			Help: "Total number of quarantined items",
		},
		[]string{"tenant", "source_type", "error_class"},
	)

	// ShardLeaseChanges tracks total shard lease changes by worker and action.
	ShardLeaseChanges = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_shard_lease_changes_total",
			Help: "Total number of shard lease changes",
		},
		[]string{"worker", "action"},
	)

	// ShardOwnership tracks current shard ownership by worker and shard.
	ShardOwnership = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrulz_shard_ownership",
			Help: "Current shard ownership (1 = owned, 0 = not owned)",
		},
		[]string{"worker", "shard"},
	)

	// BatchFormedTotal counts total batches formed by tenant and rule set.
	BatchFormedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_batch_formed_total",
			Help: "Total number of batches formed",
		},
		[]string{"tenant", "rule_set"},
	)

	// BatchSize tracks batch size by tenant and rule set.
	BatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_batch_size",
			Help:    "Batch size (number of events per batch)",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"tenant", "rule_set"},
	)

	// WorkflowStateTransitions counts total workflow state transitions by tenant, workflow type, and states.
	WorkflowStateTransitions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_workflow_state_transitions_total",
			Help: "Total number of workflow state transitions",
		},
		[]string{"tenant", "workflow_type", "from_state", "to_state"},
	)

	// WorkflowDuration tracks workflow duration by tenant and workflow type.
	WorkflowDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_workflow_duration_seconds",
			Help:    "Workflow duration in seconds",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400},
		},
		[]string{"tenant", "workflow_type"},
	)

	// ScheduledEventsReleased counts total scheduled events released by tenant and event type.
	ScheduledEventsReleased = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_scheduled_events_released_total",
			Help: "Total number of scheduled events released",
		},
		[]string{"tenant", "event_type"},
	)

	// HotKeysDetected counts total hot keys detected by tenant and partition key.
	HotKeysDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_hot_keys_detected_total",
			Help: "Total number of hot keys detected",
		},
		[]string{"tenant", "partition_key"},
	)

	// KeyQueueDepth tracks current queue depth per partition key.
	KeyQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrulz_key_queue_depth",
			Help: "Current queue depth per partition key",
		},
		[]string{"partition_key"},
	)

	// ActiveKeys tracks number of active partition keys in key queue.
	ActiveKeys = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flowrulz_active_keys",
			Help: "Number of active partition keys in key queue",
		},
	)

	// GlobalInFlight tracks number of events currently being processed.
	GlobalInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flowrulz_global_in_flight",
			Help: "Number of events currently being processed",
		},
	)

	// DatabaseQueriesDuration tracks database query duration by query type and table.
	DatabaseQueriesDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrulz_database_query_duration_seconds",
			Help:    "Database query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query_type", "table"},
	)

	// DatabaseErrors counts total database errors by query type, table, and error.
	DatabaseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrulz_database_errors_total",
			Help: "Total number of database errors",
		},
		[]string{"query_type", "table", "error"},
	)
)
