# Domain configuration

## Purpose

A **Domain** is the root of FlowRulz execution configuration. It contains **Services**, and each Service pins one or more immutable ruleset revisions.

```text
Domain
└── Service
    ├── Ruleset A @ revision 3
    ├── Ruleset B @ revision 7
    └── Ruleset C @ revision 2
```

There is no tenant, partition key, or event payload in a Domain configuration. Those are runtime event data. Do not add them to a Domain, Service, or ruleset configuration document.

## Domain document

The create request accepts `name` and `services`. The server assigns `id`, `created_at`, and `updated_at`.

- `name`: stable human-readable Domain name.
- `services`: execution Services deployed under this Domain.
- A Domain must contain 1–100 Services.
- A Service ID is unique within the Domain.
- Each Service must assign 1–500 unique ruleset IDs, each pinned to a positive immutable revision.

See the [request JSON Schema](../contracts/schemas/domain.schema.json), [response JSON Schema](../contracts/schemas/domain-response.schema.json), and [sample Domain](../data/examples/domain.json).

## Service configuration

A Service is an execution/deployment boundary, not a receiver subscription.

- `service_id`: stable identifier within the Domain.
- `revision`: monotonically increasing deployment configuration revision.
- `active`: whether the deployment is eligible to receive routed work.
- `rulesets`: explicit `ruleset_id + revision` assignments.
- `worker_profile`: optional scaling/concurrency hints.

Changing ruleset assignments or the worker profile requires a higher Service revision. Do not move a revision backwards. Deactivate a Service with a higher revision before removing it.

## Runtime routing

Each event supplies `meta.tenant_id` and `meta.partition_key`. The worker uses the combined value as the stable ordering/sharding key. These fields are not copied into Domain configuration.

The intended routing flow is:

1. Validate the event envelope and its contract.
2. Resolve the Domain/Service deployment from configured routing policy and the event's type/contract.
3. Route the event to the assigned Service worker pool.
4. The worker evaluates only the rulesets pinned by that Service.
5. Persist execution effects and checkpoint/acknowledgement using the runtime's durability protocol.
6. Independently route subscriber deliveries through the generic subscriber router.

A subscriber registration is not an execution Service. Keep receiver subscriptions separate from Domain → Service → ruleset assignments.

**Implementation status:** Domain configuration and per-Service ruleset assignments are persisted by the API. Automatic projection/reconciliation of changed assignments into worker deployments is still a follow-up; current worker environment settings must be kept in sync by deployment automation.

## API

- `POST /v1/domains`: create a Domain.
- `GET /v1/domains`: list Domains.
- `GET /v1/domains/{domainID}`: read one Domain.
- `PUT /v1/domains/{domainID}`: update a Domain.
- `DELETE /v1/domains/{domainID}`: delete a Domain.

The Domain API rejects unknown fields, so adding runtime metadata such as `meta`, `tenant_id`, `partition_key`, or `event_types` to a configuration request is an error.
