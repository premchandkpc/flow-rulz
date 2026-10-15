# Rulesets

A **ruleset** is an immutable, versioned collection of rules. A Service references a ruleset by `ruleset_id` and `revision`; it does not copy rule logic into the Service definition.

## Identity and lifecycle

- `rule_set`: stable ruleset identifier.
- `revision`: positive integer; each published revision is immutable.
- `mode`: `first_match` or `all_matches`.
- `rules`: ordered rules with predicates and actions.
- `input_contract`: optional exact contract name/version/hash required by the revision.

To change rule behavior, publish a new revision and update the Service assignment to that revision with a higher Service revision. Never mutate a published revision in place.

A ruleset definition is configuration. It must not contain `tenant_id` or `partition_key`. Runtime events carry those fields inside `meta`, and the worker uses them for event identity, deduplication, and partition ordering.

## Example

See [ruleset.json](../data/examples/ruleset.json) and [the ruleset schema](../contracts/schemas/rule-set.schema.json).

Predicates use JSON paths rooted at `$`. Supported operators include `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `exists`, and `not_exists`. Actions create generic effects or emit a new event; rules do not call another Service directly.

## Service assignment

A Service assignment pins exact revisions:

```json
{
  "service_id": "order-worker",
  "name": "order-worker",
  "revision": 1,
  "active": true,
  "rulesets": [
    { "ruleset_id": "order-review", "revision": 3 },
    { "ruleset_id": "order-notification", "revision": 7 }
  ]
}
```

The worker must not discover every ruleset and guess ownership. The Domain's Service assignment is the deployment boundary.
