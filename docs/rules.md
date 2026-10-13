# Rules

## Rule Shape

```json
{
  "rule_set": "order.created",
  "revision": 1,
  "mode": "first_match",
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  },
  "rules": [
    {
      "id": "high-value",
      "priority": 100,
      "name": "High value order review",
      "description": "Triggers manual review for orders over $1000",
      "tags": ["finance", "review"],
      "when": {
        "path": "$.total",
        "op": "gte",
        "value": 1000
      },
      "then": [
        {
          "emit": {
            "topic": "orders.review",
            "data": {
              "reason": "high_value",
              "order_id": "$.id",
              "total": "$.total"
            }
          }
        }
      ]
    }
  ]
}
```

### Document Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| rule_set | yes | string | Maps to `EventEnvelope.type`. Events with this type trigger these rules. |
| revision | yes | int | Monotonic, immutable per rule_set. Higher = newer. |
| mode | yes | string | `first_match` or `all_matches` |
| input_contract | no | object | Schema contract for validating predicate paths |
| rules | yes | array | Array of rules to evaluate |

### Rule Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| id | yes | string | Unique within rule_set. Max 128 chars. |
| priority | yes | int | Higher = evaluated first in first_match mode |
| name | no | string | Human-readable name |
| description | no | string | What this rule does |
| tags | no | string[] | Categorization tags |
| when | yes | predicate | Condition to match |
| then | yes | action[] | Actions to execute on match |
| otherwise | no | action[] | Actions when rule doesn't match (first_match only, last rule) |

## Predicates

### Leaf Predicate

Matches a single condition against event data.

```json
{
  "path": "$.total",
  "op": "gte",
  "value": 1000
}
```

| Field | Required | Description |
|-------|----------|-------------|
| path | yes | JSONPath into event data (starts with `$.`) |
| op | yes | Operator (see below) |
| value | yes | Value to compare against |

### Composite Predicates

Combine multiple conditions with logic gates.

#### all (AND)

All sub-predicates must match.

```json
{
  "all": [
    { "path": "$.country", "op": "eq", "value": "US" },
    { "path": "$.total", "op": "gte", "value": 500 }
  ]
}
```

#### any (OR)

At least one sub-predicate must match.

```json
{
  "any": [
    { "path": "$.total", "op": "gte", "value": 1000 },
    { "path": "$.priority", "op": "eq", "value": "high" }
  ]
}
```

#### not (NEGATION)

Inverts the sub-predicate.

```json
{
  "not": {
    "path": "$.status",
    "op": "eq",
    "value": "cancelled"
  }
}
```

#### Nested Example

```json
{
  "all": [
    { "path": "$.country", "op": "eq", "value": "US" },
    {
      "any": [
        { "path": "$.total", "op": "gte", "value": 500 },
        { "path": "$.priority", "op": "eq", "value": "high" }
      ]
    },
    {
      "not": {
        "path": "$.status",
        "op": "eq",
        "value": "cancelled"
      }
    }
  ]
}
```

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

## Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| eq | equals | `{"path":"$.status","op":"eq","value":"active"}` |
| neq | not equals | `{"path":"$.type","op":"neq","value":"test"}` |
| gt | greater than | `{"path":"$.total","op":"gt","value":100}` |
| gte | greater than or equal | `{"path":"$.total","op":"gte","value":1000}` |
| lt | less than | `{"path":"$.quantity","op":"lt","value":10}` |
| lte | less than or equal | `{"path":"$.age","op":"lte","value":30}` |
| in | value in array | `{"path":"$.country","op":"in","value":["US","CA"]}` |
| not_in | value not in array | `{"path":"$.status","op":"not_in","value":["cancelled"]}` |
| exists | path exists (non-null) | `{"path":"$.discount","op":"exists"}` |
| not_exists | path missing or null | `{"path":"$.discount","op":"not_exists"}` |

**Type coercion:** Numbers are normalized (float64 → int64 when whole). String comparison for non-numeric.

## Actions

### Emit (Internal Event)

Creates an effect that gets published to a broker topic. Chains internally within FlowRulz.

```json
{
  "emit": {
    "topic": "orders.review",
    "data": {
      "reason": "high_value",
      "order_id": "$.id",
      "total": "$.total"
    }
  }
}
```

**Substitution:** Values starting with `$.` are resolved against event data at evaluation time.

```json
// Input
{ "emit": { "topic": "orders.review", "data": { "order_id": "$.id" } } }

// Event data: { "id": "order-4821" }

// Output
{ "emit": { "topic": "orders.review", "data": { "order_id": "order-4821" } } }
```

### Command (Workflow Handoff)

Pushes to an external workflow layer. Not executed by the rule engine.

```json
{
  "command": {
    "destination": "workflow/create",
    "name": "approval_workflow",
    "data": {
      "order_id": "$.id",
      "workflow_type": "approval"
    }
  }
}
```

### Emit Event (Rule Chaining)

`emit_event` creates a new FlowRulz event after the parent execution commits.
It is stored in the transactional outbox and then published to NATS as
`events.{type}`. The child has a new event ID; its `type` selects its rule set.

```json
{
  "emit_event": {
    "type": "payment.requested",
    "partition_key": "order-4821",
    "data": { "order_id": "order-4821", "amount": 1500 }
  }
}
```

The child partition key is explicit in this version. Key inheritance and
data-path-derived keys are planned routing policies, not implicit behavior.

## Evaluation Modes

### first_match

Evaluates rules in priority order (highest first). Stops at the first matching rule.

```
Rule A (priority=100): no match  → continue
Rule B (priority=50):  match     → STOP, emit effects
Rule C (priority=10):  never evaluated
```

**`otherwise` constraint:** Only allowed on the last-priority rule. Executes when no other rule matches.

```json
{
  "rules": [
    { "id": "specific", "priority": 100, "when": {...}, "then": [...] },
    { "id": "default", "priority": 1, "when": null, "then": [
      { "emit": { "topic": "orders.default", "data": {} } }
    ]}
  ]
}
```

### all_matches

Evaluates all rules. Collects all matched effects.

```
Rule A (priority=100): match → emit effect A
Rule B (priority=50):  match → emit effect B
Rule C (priority=10):  match → emit effect C

Result: [effect A, effect B, effect C]
```

## Contracts

Rule revisions can declare an input contract. Before activation, `ValidatePathsAgainstContract` checks that all predicate paths exist in the contract schema.

```json
{
  "rule_set": "order.created",
  "revision": 2,
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  },
  "rules": [...]
}
```

This prevents activating rules that reference non-existent fields.

## Compiler Limits (Configurable)

| Limit | Default | Description |
|-------|---------|-------------|
| MaxRulesPerSet | 100 | Maximum rules in a single rule set |
| MaxPredicates | 200 | Total predicates across all rules |
| MaxNestingDepth | 10 | Maximum nesting depth for composite predicates |
| MaxActionsPerSet | 10 | Total actions across all rules |
| MaxPayloadBytes | 256KB | Maximum event payload size |
| MaxRuleIDLen | 128 | Maximum rule ID length |

## Determinism

The evaluator is **pure**: no I/O, no clock, no randomness, no service calls. Given the same compiled revision and event, it **always** produces the same decision.

**What the evaluator does NOT do:**
- Read from database
- Make HTTP/gRPC calls
- Access the clock
- Use random numbers
- Mutate global state
- Call external services

This ensures:
- Replay produces identical results
- Audit trail is reproducible
- Testing is deterministic

## Error Handling

| Error Class | Example | Handling |
|-------------|---------|----------|
| Validation | Missing required field | Return error, event not processed |
| Permanent | Rule not found | Ack event, record in quarantine |
| Transient | Database timeout | Retry with backoff |
| Evaluation | Division by zero | Return error, retry |
| Transport | NATS unavailable | Retry with backoff |

## Rule Deployment Lifecycle

```
1. POST /v1/rules/{ruleSet}/revisions
   → Compiler.Compile(source)
   → Validates: schema, limits, duplicate IDs, priorities
   → Returns RuleRevision with content_hash

2. RuleRevision saved to rule_revisions table (immutable)

3. RuleActivation created/updated in rule_activations table
   → Points to new revision
   → Version increments for optimistic locking

4. Worker picks up new revision on next event
   → ActivationRepository.Get() reads latest
   → RuleRepository.GetActive() loads compiled form
```

## Hot Reload

Rules are hot-reloaded automatically:
- New revision deployed → new activation record
- Next event processed uses new revision
- No worker restart needed
- Old revisions remain for audit/replay

## Rule Explanations

Every evaluation produces `RuleExplanation` for each rule:

```json
{
  "rule_id": "high-value",
  "rule_name": "High value order review",
  "matched": true,
  "priority": 100,
  "reason": "predicate matched",
  "actions": ["emit:orders.review"]
}
```

Useful for:
- Debugging why a rule did/didn't match
- Audit trails
- Dashboard visualization
