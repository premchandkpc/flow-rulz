# Verification matrix

Use this matrix per feature or release. Replace example rows with test paths and exact evidence. Do not mark a row verified based on a skipped test, mock-only test, or inferred behavior.

| Requirement / invariant | Unit | Contract | Real DB + broker | E2E/recovery | Status | Evidence / gap |
|---|---|---|---|---|---|---|
| Duplicate event does not create duplicate execution | | | | | Unknown | |
| Execution and outbox intent are atomic | | | | | Unknown | |
| ACK follows durable commit | | | | | Unknown | |
| Crash after commit before ACK is safe | | | | | Unknown | |
| Stale lease holder is fenced | | | | | Unknown | |
| Per-key order survives retry and worker handoff | | | | | Unknown | |
| Poison / MaxDeliver exhaustion is observable and recoverable | | | | | Unknown | |
| External effects use stable idempotency keys | | | | | Unknown | |
| Tenant isolation covers reads and writes | | | | | Unknown | |
| Contract and immutable revision selection is enforced | | | | | Unknown | |
| Go and Rust behavior matches where parity is claimed | | | | | Unknown | |

Allowed status values: **Verified**, **Partial**, **Gap**, **Skipped**, **Not applicable**, **Unknown**.

For a release report, link this matrix to the exact source commit and CI run. Keep failed, skipped, and unimplemented cases visible.
