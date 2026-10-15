# HTTP API

The API separates global configuration from runtime event ingestion.

## Domain configuration

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/domains` | Create a Domain containing Services and ruleset assignments |
| GET | `/v1/domains` | List Domains |
| GET | `/v1/domains/{domainID}` | Get a Domain |
| PUT | `/v1/domains/{domainID}` | Update a Domain |
| DELETE | `/v1/domains/{domainID}` | Delete a Domain |

A Domain configuration contains only Domain fields and nested Services. Each Service pins one or more immutable ruleset revisions. Runtime event identity, partitioning, tracing, and other event metadata are not configuration fields.

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

See the [Domain request schema](../contracts/schemas/domain.schema.json), [response schema](../contracts/schemas/domain-response.schema.json), and [sample Domain](../data/examples/domain.json).

## Ruleset and contract APIs

- `POST /v1/rules/{ruleSet}/revisions`: compile and publish a revision.
- `GET /v1/rules/{ruleSet}/revisions`: list revisions.
- `GET /v1/rules/{ruleSet}/revisions/{revision}`: fetch a revision.
- `DELETE /v1/rules/{ruleSet}/revisions/{revision}`: deactivate a ruleset revision.
- `POST /v1/contracts`, `GET /v1/contracts`, `GET /v1/contracts/{name}/{version}`: manage/read versioned contract definitions.

## Runtime events

`POST /v1/events/{eventType}` accepts an event whose top-level envelope contains `id`, `type`, `occurred_at`, optional `contract`, and `data`. The required `meta` object contains `tenant_id` and `partition_key`; it can also contain tracing, correlation, producer, routing, and future metadata fields. Unknown metadata keys are accepted and preserved. Unknown top-level envelope fields are rejected.

The event metadata tenant value must match the authenticated producer scope when the ingress authentication middleware supplies one. This is an event authorization check, not Domain configuration.

See [event.md](event.md), [domain.md](domain.md), and the [JSON samples](../data/examples/).
