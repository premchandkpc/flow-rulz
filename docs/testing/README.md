# Testing and verification

Testing documentation records how correctness is demonstrated. Test names and green unit tests do not establish live infrastructure behavior unless the test actually exercises that infrastructure.

## Files
- [Test strategy](test-strategy.md): test layers and the claims each layer supports.
- [BDD scenario template](bdd-scenario-template.md): Given/When/Then acceptance examples.
- [Verification matrix](verification-matrix.md): acceptance criteria mapped to tests and evidence.

The existing [change and test checklist](../testing.md) contains repository-specific commands. Read [AGENTS.md testing standards](../../AGENTS.md) before changing coverage.

## Evidence rules
- Record exact command, commit SHA, environment/dependencies, exit status, and relevant test path.
- Mark skipped tests as Skipped, not Passed.
- Mocks prove behavior against the mock contract, not PostgreSQL, NATS JetStream, distributed leases, or process crashes.
- Use infrastructure-backed tests for transaction atomicity, redelivery, lease fencing, and crash recovery.
- Keep failed or flaky tests visible until diagnosed; do not silently delete or weaken them.
