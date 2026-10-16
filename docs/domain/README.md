# Domain model and contracts

This section describes concepts and event identity. Field-level truth remains in schemas, migrations and existing implementation guides; avoid copying API field lists into competing documents.

## Canonical references
- [Conceptual domain model](domain-model.md)
- [Event contract and identity](event-contract.md)
- [Domain configuration](../domain.md)
- [Event envelope](../event.md)
- [Contract registry](../contract.md)
- [JSON Schemas and examples](../../contracts/schemas/)

## Ownership boundaries
| Concept | Owns | Not the same as |
|---|---|---|
| Domain | Global execution configuration root | Tenant/event identity |
| Service | Deployment boundary and pinned ruleset assignments | Generic subscriber registration |
| Ruleset revision | Immutable evaluation logic | Mutable active assignment |
| Contract | Payload shape identity: name, version, canonical hash | Routing or authorization |
| Event envelope | Event ID/type/time, payload, contract, headers and metadata | Domain configuration |
| Inbox | Durable duplicate-detection evidence | Broker delivery count alone |
| Execution | Durable processing decision/result | Successful external side effect |
| Outbox effect | Durable intent for asynchronous delivery | Proof destination accepted it |
| Subscription | Generic receiver fan-out | Rule evaluation or worker assignment |
| Shard lease/fencing | Temporary worker ownership | Tenant authorization |

When a field or relationship changes, update schema and canonical implementation guide first, then this conceptual documentation if the model changed. Add compatibility and negative tests for cross-tenant or invalid-reference behavior.
