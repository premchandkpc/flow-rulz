# HTTP API

The API has two distinct responsibilities: global control-plane configuration and runtime/event operations. A Domain configuration request must not include event identity fields.

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

There is no `tenant_id`, `partition_key`, or `event_types` property in this request. The decoder rejects unknown fields and rejects client-supplied `id`, `created_at`, and `updated_at`. See the [request schema](../contracts/schemas/domain.schema.json) and [response schema](../contracts/schemas/domain-response.schema.json).

## Ruleset and contract APIs

- `POST /v1/rules/{ruleSet}/revisions`: compile and activate a revision.
- `GET /v1/rules/{ruleSet}/revisions`: list revisions.
- `GET /v1/rules/{ruleSet}/revisions/{revision}`: fetch a revision.
- `DELETE /v1/rules/{ruleSet}/revisions/{revision}`: deactivate the ruleset.
- `POST /v1/contracts`, `GET /v1/contracts`, `GET /v1/contracts/{name}/{version}`: manage/read immutable contract definitions.

Rule and Domain configuration use the global configuration scope. Runtime event identity does not select the configuration namespace.

## Runtime events

The event envelope carries `meta.tenant_id` and `meta.partition_key`, plus any future runtime context under `meta`. These are event data, not Domain configuration or query parameters. The API/event decoder rejects top-level `tenant_id` and `partition_key`; do not send them outside `meta`. Ingress still checks `meta.tenant_id` against the authenticated producer identity; authentication is not a reason to copy tenant identity into Domain configuration. See [the canonical event example](../data/examples/event.json) and [event schema](../contracts/schemas/event-envelope.schema.json).

See [event.md](event.md), [domain.md](domain.md), and [the sample files](../data/examples/).
