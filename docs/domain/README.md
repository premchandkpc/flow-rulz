# Domain and contracts

This folder organizes domain concepts and runtime contract guidance. The existing [Domain configuration guide](../domain.md), [event guide](../event.md), and [contract guide](../contract.md) remain implementation-specific references; avoid maintaining contradictory copies of API fields.

## Files
- [Domain model](domain-model.md): conceptual entities and ownership boundaries.
- [Event contract](event-contract.md): event identity, partitioning and envelope rules.

## Ownership boundaries
- **Domain / Service / ruleset assignment:** global control-plane configuration.
- **Contract:** payload shape and version identity.
- **Event envelope:** runtime metadata including tenant and partition key.
- **Ruleset revision:** immutable evaluation logic.
- **Execution / outbox effect:** durable result and delivery intent.
- **Subscription:** receiver fan-out, separate from execution Service assignment.

Update the implementation guide and this index whenever a contract change affects callers.
