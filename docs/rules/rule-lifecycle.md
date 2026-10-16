# Rule lifecycle

**Purpose:** expected lifecycle for rule changes. Confirm exact API and storage behavior against current implementation before using this as an operational runbook.

1. **Draft:** author a candidate ruleset without changing the active revision.
2. **Validate:** parse the document, validate fields and operators, verify contract compatibility, and compile the rule.
3. **Test:** run predicate/action tests and contract fixtures, including negative and boundary cases.
4. **Publish:** persist a new immutable revision with reproducible metadata.
5. **Assign:** pin the revision to the intended Domain Service or routing configuration.
6. **Activate:** update active configuration through the supported control-plane path.
7. **Observe:** verify selected revision, execution outcomes, errors, and outbox effects.
8. **Rollback:** select a previously valid revision through a new configuration change; do not rewrite the old revision.

## Required evidence
Record ruleset ID, revision, contract name/version/hash where applicable, deployment configuration revision, test run, activation timestamp, and rollback target. If a step is unsupported by the current runtime, mark it as a gap rather than simulating completion in docs.
