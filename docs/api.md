# HTTP API

The API exposes control-plane configuration and runtime event ingress. Configuration defines only Domain -> Service -> ruleset assignments. Tenant identity and partition keys are runtime event metadata, not configuration fields.

## Domain configuration

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/domains` | Create a Domain with Services and ruleset assignments |
| GET | `/v1/domains` | List Domains |
| GET | `/v1/domains/{domainID}` | Get a Domain |
| PUT | `/v1/domains/{domainID}` | Replace/update a Domain |
| DELETE | `/v1/domains/{domainID}` | Delete a Domain |

Example request:

```json
{
  "name": "commerce",
  "services": [
    {
      "service_id": "order-worker",
      "name": "order-worker",
      "revision": 1,
      "active": true,
      "rulesets": [
        { "ruleset_id": "order-review", "revision": 3 },
        { "ruleset_id": "order-notification", "revision": 7 }
      ],
      "worker_profile": { "min_replicas": 1, "max_replicas": 4, "concurrency": 8 }
    }
  ]
}
```

A Domain request does not accept `tenant_id`, `partition_key`, or event payload fields. The API rejects unknown configuration fields. See the [Domain request schema](../contracts/schemas/domain.schema.json), [response schema](../contracts/schemas/domain-response.schema.json), and [sample Domain](../data/examples/domain.json).

## Runtime event envelope

The event API accepts an event with stable top-level identity and business payload plus an extensible `meta` object. Put `tenant_id`, `partition_key`, `workflow_id`, `correlation_id`, `causation_id`, `headers`, trace context, and producer-specific metadata inside `meta`. Unknown `meta` fields are preserved. Ingress validates `meta.tenant_id` against the authenticated producer identity.

See [event.md](event.md), the [event schema](../contracts/schemas/event-envelope.schema.json), and [event sample](../data/examples/event.json).

## Ruleset and contract APIs

- `POST /v1/rules/{ruleSet}/revisions`: compile and activate a revision.
- `GET /v1/rules/{ruleSet}/revisions`: list revisions.
- `GET /v1/rules/{ruleSet}/revisions/{revision}`: fetch a revision.
- `DELETE /v1/rules/{ruleSet}/revisions/{revision}`: deactivate the ruleset.
- `POST /v1/contracts`, `GET /v1/contracts`, `GET /v1/contracts/{name}/{version}`: manage/read immutable contract definitions.

Ruleset and Domain configuration use the global configuration scope. Runtime event identity does not select the configuration namespace.
