# Design

## High-level Design

FlowRulz provides **transactional event deduplication and at-least-once effect delivery**. A committed inbox record prevents a duplicate delivery of the same `(tenant_id, event_id)` from creating another execution while that inbox record is retained. This is not end-to-end exactly-once delivery: an effect may reach a destination before the publisher crashes and records it as delivered. External effects are effectively-once only when the destination durably deduplicates the `Idempotency-Key` for a documented retention window and returns the original result for repeated requests.

### Core Guarantee

```
At-least-once broker delivery
        │
        ▼
┌───────────────────┐
│  Transactional    │  ← inbox dedup prevents duplicate execution
│  Inbox            │     (unique constraint on tenant_id + event_id)
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Rule Evaluation  │  ← pure, deterministic, no side effects
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Execution Record │  ← immutable decision hash, pinned revision
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Outbox Effects   │  ← effect IDs are sent as destination idempotency keys
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Broker ACK       │  ← only after commit
└───────────────────┘
```

### Bounded Contexts

| Context | Aggregate | Key Invariant | State |
|---------|-----------|---------------|-------|
| Rule management | RuleRevision | One active revision per (internal configuration scope, rule_set) | Immutable once saved |
| Event intake | InboxEntry | One execution per (tenant_id, event_id) | processing → committed |
| Decisioning | Execution | Immutable decision hash, pinned revision | pending → completed/failed/quarantined |
| Effect delivery | OutboxEffect | At-least-once with idempotent effect_id | pending → claimed → delivered/quarantined |
| Operations | ShardLease | One owner per virtual shard with fencing token | Active/expired |

### Consistency Model

ACID applies inside SQL, not across SQL + broker + HTTP. The transaction boundary is intentionally narrow:

```
BEGIN
  ┌─────────────────────────────────────────────────────────┐
  │ 1. inbox insert (dedup check)                          │
  │ 2. activation read (find active rule set)              │
  │ 3. rule read (get compiled revision)                   │
  │ 4. evaluate (pure function, no I/O)                    │
  │ 5. execution insert (record decision)                  │
  │ 6. outbox insert (queue effects)                       │
  │ 7. inbox mark committed                                │
  └─────────────────────────────────────────────────────────┘
COMMIT
  → broker ACK only after commit succeeds
```

**Why this works:**
- Inbox unique constraint prevents duplicate execution creation
- If crash before commit: rollback, broker redelivers, inbox insert succeeds on retry
- If crash after commit but before ACK: inbox already committed, redelivery finds existing execution, returns it, ACKs
- Effect IDs are used as destination idempotency keys. Retries can still send the same request more than once; the destination must durably deduplicate repeated keys. SQL cannot make a remote HTTP side effect atomic with the outbox transaction.

## Low-level Design

### Core Types

| Type | Package | Fields | Purpose |
|------|---------|--------|---------|
| EventEnvelope | domain | ID, Type, TenantID, PartitionKey, OccurredAt, Data, Headers | Inbound event |
| RuleRevision | domain | TenantScope, RuleID, Revision, Source, Compiled, Hash, InputContract | Compiled rules + metadata |
| CompiledRule | domain | ID, Priority, When, Then, Name, Description, Tags | Single rule |
| Predicate | domain | Path, Op, Value, All, Any, Not | Leaf or composite condition |
| Action | domain | EmitAction, CommandAction | What to do on match |
| Effect | domain | ID, ExecutionID, Destination, Name, Payload, EffectType | Emitted effect |
| Decision | domain | Matched, Effects, Hash, Explanations | Evaluation result |
| Execution | domain | ID, EventID, TenantID, RuleSet, Revision, DecisionHash, Status | Processing record |
| OutboxEffect | domain | ID, ExecutionID, Destination, Name, Payload, Status, Attempts | Delivery record |
| InboxEntry | domain | TenantID, EventID, Status, ExecutionID, FirstSeenAt, CommittedAt | Dedup record |
| ShardLease | domain | VirtualShard, Owner, FencingToken, ExpiresAt, RoutingEpoch | Worker ownership |
| QuarantineEntry | domain | ID, SourceType, SourceID, ErrorClass, PayloadRef, EventID, TenantID, Error | Failed item |
| RuleActivation | domain | TenantScope, RuleSet, Revision, Version, Actor, ActivatedAt | Active revision pointer |

### Domain Behavior: State Machines

State transitions are enforced at the domain layer. Invalid transitions return errors.

#### Execution

```
                      ┌──────────────┐
                      │   pending    │
                      └──────┬───────┘
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
          ┌──────────┐ ┌──────────┐ ┌──────────────┐
          │completed │ │  failed  │ │ quarantined  │
          └──────────┘ └──────────┘ └──────────────┘
```

```go
exec.Complete(now)    // pending → completed
exec.Fail(now, msg)   // pending → failed
exec.Quarantine(now, msg) // pending → quarantined
```

#### OutboxEffect

```
        ┌──────────┐
        │ pending  │◄─────────────────┐
        └────┬─────┘                  │
             │ claim                  │ retry
             ▼                        │
        ┌──────────┐                  │
        │ claimed  │──────────────────┘
        └────┬─────┘
             │
       ┌─────┼─────┐
       ▼     ▼     ▼
┌─────────┐ ┌──────────┐ ┌──────────────┐
│delivered│ │ pending  │ │ quarantined  │
└─────────┘ │ (retry)  │ └──────────────┘
            └──────────┘
```

```go
effect.Claim(owner, now)     // pending → claimed
effect.Deliver(now)          // claimed → delivered
effect.Retry(now, delay, attempts, msg) // claimed → pending
effect.Quarantine(now, msg)  // claimed → quarantined
```

#### InboxEntry

```
┌────────────┐     ┌──────────┐
│ processing │────>│ committed│
└────────────┘     └──────────┘
```

```go
entry.Commit(executionID, now) // processing → committed
entry.IsDuplicate()            // true if committed with execution ID
```

### Transaction Pattern

```go
type TxRepos struct {
    Inbox       ports.InboxRepository
    Activations ports.ActivationRepository
    RuleRepo    ports.RuleRepository
    Executions  ports.ExecutionRepository
    Outbox      ports.OutboxRepository
}

// RepoFactory creates transaction-scoped repositories.
// The concrete type is an adapter concern (sql.Tx implements this).
type RepoFactory func(db interface{}) TxRepos
```

The `RepoFactory` receives a `sql.Tx` (wrapping pgx.Tx) and constructs repositories scoped to that transaction. The application never manages connection pooling or transaction lifecycle directly.

### Data Model (PostgreSQL)

```sql
-- Immutable rule source and compiled form
CREATE TABLE rule_revisions (
    tenant_scope  TEXT NOT NULL,
    rule_id       TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    source        JSONB NOT NULL,
    compiled      JSONB NOT NULL,
    content_hash  TEXT NOT NULL,
    compiler_version TEXT NOT NULL,
    match_mode    TEXT NOT NULL,
    input_contract JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_scope, rule_id, revision)
);

-- Active revision per rule set
CREATE TABLE rule_activations (
    tenant_scope  TEXT NOT NULL,
    rule_set      TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    version       BIGINT NOT NULL DEFAULT 1,
    actor         TEXT NOT NULL,
    activated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_scope, rule_set),
    FOREIGN KEY (tenant_scope, rule_set, revision) REFERENCES rule_revisions(...)
);

-- Dedup and processing status
CREATE TABLE inbox (
    tenant_id     TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'processing',
    execution_id  TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, event_id)
);

-- Processing record with decision audit
CREATE TABLE executions (
    execution_id  TEXT PRIMARY KEY,
    event_id      TEXT NOT NULL,
    tenant_id     TEXT NOT NULL,
    rule_set      TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    decision_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    error         TEXT,
    trace_id      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_executions_tenant_event ON executions (tenant_id, event_id);
CREATE INDEX idx_executions_status ON executions (status);

-- Durable effects with delivery state
CREATE TABLE outbox_effects (
    effect_id      TEXT PRIMARY KEY,
    execution_id   TEXT NOT NULL REFERENCES executions(execution_id),
    destination    TEXT NOT NULL,
    name           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    effect_type    TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INT NOT NULL DEFAULT 0,
    max_attempts   INT NOT NULL DEFAULT 5,
    available_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by     TEXT,
    claimed_at     TIMESTAMPTZ,
    claim_expires_at TIMESTAMPTZ,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_effects_status ON outbox_effects (status, available_at);
CREATE INDEX idx_outbox_effects_execution ON outbox_effects (execution_id);
CREATE INDEX idx_outbox_effects_claim_expiry ON outbox_effects (status, claim_expires_at) WHERE status = 'claimed';

-- Worker ownership with fencing tokens
CREATE TABLE shard_leases (
    virtual_shard  INT PRIMARY KEY,
    owner          TEXT NOT NULL,
    fencing_token  BIGINT NOT NULL DEFAULT 1,
    expires_at     TIMESTAMPTZ NOT NULL,
    routing_epoch  BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_shard_leases_owner ON shard_leases (owner);
CREATE INDEX idx_shard_leases_expires ON shard_leases (expires_at);

-- Failed items for inspection and replay
CREATE TABLE quarantine (
    quarantine_id  TEXT PRIMARY KEY,
    source_type    TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    error_class    TEXT NOT NULL,
    payload_ref    TEXT,
    event_id       TEXT,
    tenant_id      TEXT,
    error_message  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_quarantine_tenant ON quarantine (tenant_id);
CREATE INDEX idx_quarantine_created ON quarantine (created_at);
```

### Effect ID (Destination Idempotency Key)

Effect IDs are deterministic and are sent as idempotency keys to destinations:

```
effect_id = sha256(tenant_id + "|" + event_id + "|" + rule_set + "|" + revision + "|" + rule_id + "|" + action_index)
```

This formula is intended to make effect IDs stable for the same event tenant, event, rule revision, rule, and action position. The destination must persistently deduplicate the key for at least the supported retry/replay horizon. Execution IDs are derived from the tenant, event ID, rule set, and selected revision. This makes identity stable for the same event/revision tuple, but does not itself implement a replay API or define whether replay should use the original or a newer revision.

### Decision Hash

```
decision_hash = sha256(rule_set + revision + sorted(matched_rule_ids) + event_hash + sorted(effect_ids))
```

Stable across identical inputs. Used for:
- Audit trail
- Replay verification (re-evaluating produces same hash)
- Detecting rule drift

### Partitioning & Ordering

**Virtual Shards**: 4096 fixed shards mapped to physical workers.

```
shard = fnv32a(tenant_id + ":" + partition_key) % 4096
```

**Ordering scope and limits:**
- Tenant and partition key determine the virtual shard.
- A worker's key queue serializes processing for the same key within that process.
- This is not a global or end-to-end per-key ordering guarantee across redelivery, lease handoff, or concurrent outbox publishers.
- The event transaction validates shard ownership and the fencing token before mutating inbox/execution/outbox state; live-infrastructure contention and stale-worker tests remain required.
- `FOR UPDATE SKIP LOCKED` coordinates outbox claims but does not, by itself, preserve per-key effect order.

**Rebalancing target:**
1. Mark instance draining
2. Stop new fetches for affected shards
3. Finish or safely cancel bounded in-flight executions
4. Release/expire leases and allow the next owner to acquire a higher fencing token
5. Terminate

Dynamic membership and automated rebalancing are not implemented; current assignment assumes three worker replicas.

### Failure Handling

| Scenario | Behavior |
|----------|----------|
| Crash before commit | Rollback, broker redelivers, inbox dedup catches it |
| Crash after commit, before ACK | Inbox committed, redelivery returns existing execution, no duplicate |
| Destination down | Outbox retry policy applies; verify actual backoff configuration and terminal quarantine behavior against the configured adapter |
| Poison message | Permanent processing errors are persisted to quarantine before ACK; quarantine persistence failures are retried. JetStream MaxDeliver exhaustion/advisory-to-DLQ handling still requires explicit verification |
| Two workers same event | Inbox unique constraint prevents duplicate execution |
| Outbox publisher crash | Claimed effects have TTL (claim_expires_at), become claimable again after expiry |
| Worker loses lease | Fencing token prevents stale commits; new owner takes over |

### Delivery Guarantees and Crash Windows

FlowRulz does not provide end-to-end exactly-once delivery.

1. **Before database commit:** the transaction rolls back; broker redelivery can retry the event.
2. **After commit, before broker ACK:** redelivery finds the retained inbox record and must not create a second execution.
3. **After destination send, before marking the outbox effect delivered:** the request may be sent again after claim expiry or a publisher crash. The destination must durably deduplicate the `Idempotency-Key` for the supported retry/replay horizon.
4. **Inbox retention:** deduplication only holds while the inbox identity is retained. Retention must exceed the broker redelivery horizon and any supported replay horizon.

The accurate claim is **at-least-once transport with transactional inbox deduplication and effectively-once external effects only when the destination honors the idempotency contract**.

### What the Evaluator Does NOT Do

The evaluator is a **pure function**:

- No database reads
- No HTTP/gRPC calls
- No clock access
- No random numbers
- No global state mutation
- No external service calls

Given the same compiled revision and event, it **always** produces the same decision.

### Subscriber Routing Boundary

The rules evaluator is deterministic and side-effect-free. It does not query the tenant subscriber registry and does not implement subscriber-specific behavior.

Tenant domains and service/subscriber registrations are control-plane configuration. A separate generic subscriber router applies the platform routing policy: match the event type to active subscriptions within the event's tenant, enforce the fan-out limit, and produce stable delivery intents. The router runs in the event transaction after a successful active-ruleset execution, so its intents commit atomically with the execution and outbox. The dispatcher publishes those intents through NATS JetStream. Events with no active ruleset do not trigger subscriber delivery. Explicit `service_event` actions remain ruleset-declared effects and are dispatched through the same outbox infrastructure.

Keep these two contracts distinct:
- **Ruleset effects:** outputs produced by evaluating a ruleset.
- **Subscription routing:** deliveries produced because a registered subscriber matches an event under the platform routing policy.

If both mechanisms are enabled, define deduplication and overlap semantics explicitly so the same logical delivery is not emitted twice.

### Contract Validation

Rule revisions can declare an `input_contract`. Before activation, `ValidatePathsAgainstContract` checks that all predicate paths exist in the contract schema. This prevents activating rules that reference non-existent fields.

```json
{
  "rule_set": "order.created",
  "revision": 2,
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  }
}
```

### Compiler Limits (Configurable)

| Limit | Default | Description |
|-------|---------|-------------|
| MaxRulesPerSet | 100 | Maximum rules in a single rule set |
| MaxPredicates | 200 | Total predicates across all rules |
| MaxNestingDepth | 10 | Maximum nesting depth for composite predicates |
| MaxActionsPerSet | 10 | Total actions across all rules |
| MaxPayloadBytes | 256KB | Maximum event payload size |
| MaxRuleIDLen | 128 | Maximum rule ID length |

### Path Resolution

Paths use dot notation starting with `$`:

| Path | Resolves to |
|------|-------------|
| `$.id` | `event.data.id` |
| `$.order.total` | `event.data.order.total` |
| `$.items` | `event.data.items` (entire array) |

**V1 limitations:**
- No array indexing (`$.items[0]` not supported)
- No recursive queries
- Dots only (no bracket notation)

## Global configuration scope

The Domain API and ruleset management API do not accept tenant identity as configuration. They use the fixed internal scope `__domain_configuration__` for legacy persistence columns named `tenant_scope`. That column name is a storage compatibility detail, not a public API field. The event service reads `tenant_id` and `partition_key` from each runtime event and uses them for shard routing, inbox deduplication, execution identity, and tenant isolation. Never use event tenant identity as the namespace for global Domain or ruleset configuration.
