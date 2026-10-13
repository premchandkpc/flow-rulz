# Pull Request Template

## Description

<!-- Provide a clear description of the changes in this PR -->

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Refactoring (no functional changes)
- [ ] Performance improvement
- [ ] Test improvements
- [ ] CI/CD changes
- [ ] Dependency updates

## Related Issues

<!-- Link to related issues using keywords like "Fixes #123", "Closes #456", "Relates to #789" -->

## Changes Made

### Files Changed
<!-- List the main files changed and why -->

### Key Changes
- 
- 
- 

## Testing

### Test Strategy
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] E2E tests added/updated
- [ ] Manual testing performed

### Test Commands Run
```bash
# Commands run to verify the changes
make test-unit
make test-integration
make test-examples
go test -tags=e2e -count=1 ./tests/e2e/...
make ci
```

### Test Results
<!-- Paste test output or summarize results -->

## Checklist

### Code Quality
- [ ] `make check-fmt` passes (code is formatted)
- [ ] `make vet` passes (go vet)
- [ ] `make lint` passes (golangci-lint)
- [ ] No new warnings or errors introduced

### Architecture & Design
- [ ] Changes follow dependency direction (see AGENTS.md Section 4)
- [ ] Business logic does not depend on transport/storage/vendor libraries
- [ ] Existing domain types, schemas, and helpers are reused
- [ ] Interfaces are narrow, consumer-oriented, and necessary
- [ ] No speculative abstractions or catch-all `common`/`utils` packages

### Correctness & Reliability
- [ ] At-least-once delivery semantics preserved
- [ ] Duplicate delivery, retry, cancellation, shutdown handled
- [ ] Tenant isolation maintained at repository query boundaries
- [ ] Errors wrapped with context, classification preserved
- [ ] Goroutine ownership, cancellation, synchronization explicit
- [ ] No unbounded channels or goroutine-per-message patterns
- [ ] Fencing tokens and lease expiry validated

### Security
- [ ] No secrets, credentials, or tokens logged
- [ ] No SQL injection or BSON injection vulnerabilities
- [ ] Tenant scoping enforced at repository boundaries
- [ ] Request bodies bounded, malformed input rejected
- [ ] Trusted proxy headers handled correctly

### Compatibility
- [ ] API/event/schema/SDK backward compatibility assessed
- [ ] Database migrations are ordered, repeatable, tested
- [ ] Configuration changes documented with defaults/validation
- [ ] No breaking changes without version bump and migration plan

### Observability
- [ ] Structured logs with stable field names (event ID, tenant ID, shard, etc.)
- [ ] Metrics describe outcomes and resource pressure
- [ ] No high-cardinality labels on metrics
- [ ] Traces cross async boundaries where supported

### Documentation
- [ ] README.md updated if user-facing behavior changed
- [ ] AGENTS.md updated if architecture/ownership changed
- [ ] API schemas updated if contracts changed
- [ ] Code comments/godoc updated for exported symbols
- [ ] CHANGELOG.md updated (if applicable)

### Docker/Compose/CI
- [ ] Dockerfiles build successfully
- [ ] docker-compose.yml validates (`docker compose config --quiet`)
- [ ] .env.example updated if new config added
- [ ] CI workflows updated if test/build process changed

## Screenshots/Recordings

<!-- If UI changes, add screenshots or recordings -->

## Additional Notes

<!-- Any additional information, concerns, or context for reviewers -->

## Reviewer Guide

### Focus Areas
- [ ] Business logic correctness
- [ ] Distributed systems guarantees (delivery, ordering, fencing)
- [ ] Error handling and retry behavior
- [ ] Tenant isolation
- [ ] Test coverage for failure paths

### Questions for Reviewers
<!-- Any specific questions you want reviewers to address -->