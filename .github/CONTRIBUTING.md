# Contributing to FlowRulz

Thank you for your interest in contributing to FlowRulz! This document outlines the process for contributing code, documentation, and other improvements.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Workflow](#development-workflow)
4. [Code Standards](#code-standards)
5. [Testing](#testing)
6. [Pull Request Process](#pull-request-process)
7. [Release Process](#release-process)
8. [Architecture Overview](#architecture-overview)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- Go 1.23+
- Docker & Docker Compose
- Make
- Node.js 22+ (for web console)
- Rust 1.80+ (for Rust worker)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/flow-rulz/flow-rulz.git
cd flow-rulz

# Copy environment template
cp .env.example .env

# Start dependencies (PostgreSQL, NATS, Redis)
make docker-up

# Apply database migrations
make migrate-up

# Build and run the API server
make run-api

# In another terminal, run the worker
make run-worker

# Deploy domain contracts (in separate terminals)
cd domains/ecommerce && ./deploy.sh
cd domains/banking && ./deploy.sh
cd domains/food-delivery && ./deploy.sh

# Run the random producer to generate test events
docker compose --profile examples run --rm random-producer
```

## Development Workflow

### 1. Inspect Before Editing

Before making changes:
- Read [AGENTS.md](../AGENTS.md) for engineering standards
- Inspect the current implementation, callers, tests, and runtime paths
- Trace success, error, retry, cancellation, shutdown, and recovery paths

### 2. Make Small, Cohesive Changes

- One observable change per PR
- Keep dependencies pointing inward (business policy must not depend on transport/storage/vendor)
- Reuse existing domain types, schemas, and helpers
- Prefer the smallest design that preserves clear ownership and testability

### 3. Branching Strategy

- `main` - Stable, deployable branch
- Feature branches: `feat/<short-description>`
- Bug fixes: `fix/<short-description>`
- Documentation: `docs/<short-description>`

### 4. Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`

Example:
```
feat(events): add fencing token validation on event processing

Ensure that stale workers cannot settle work owned by a newer claimant
by validating the fencing token against the current lease holder.

Closes #123
```

## Code Standards

### Go Standards

- Run `gofmt` (enforced by `make check-fmt`)
- Follow idiomatic naming and project lint rules (`make lint`)
- Pass `context.Context` explicitly for operation-scoped work
- Wrap errors with useful operation context using `fmt.Errorf("...: %w", err)`
- Preserve error classification using `errors.Is`/`errors.As`
- Do not swallow persistence, cleanup, cancellation, or settlement errors
- Make goroutine ownership, cancellation, synchronization, and termination explicit
- Use deterministic clocks/IDs where helpful
- Add table-driven tests, race tests for concurrency changes, benchmarks before performance claims

### Dependency Direction

```
cmd/* (composition roots)
  -> concrete adapters + storage + services/application
services/application/runtime
  -> domain + consumer-owned ports
adapters/storage
  -> ports + domain + external libraries
domain/rules
  -> standard library and justified pure dependencies
```

### File Placement

See [AGENTS.md Section 11](../AGENTS.md#11-detailed-package-and-file-placement-rules) for detailed placement rules.

### Testing

- Unit tests: Pure functions, domain invariants, local error handling
- Use-case/service tests: Orchestration, ports, retries, idempotency decisions
- Adapter/contract tests: Encoding, query mapping, status/ACK translation, port conformance
- Integration tests: Real service interaction and persistence behavior (tagged `integration`)
- E2E tests: Full producer-to-durable-result flow (tagged `e2e`)
- Failure injection tests: Restart, timeout, lost response, lease loss, partial outage

Run tests locally:
```bash
# Unit tests
make test-unit

# Example scenario tests
make test-examples

# Integration tests (requires running services)
make test-integration

# All tests with race detector
make test

# E2E tests (requires testcontainers)
go test -tags=e2e -count=1 ./tests/e2e/...
```

### CI Pipeline

The CI pipeline runs:
1. Format check (`make check-fmt`)
2. Schema validation
3. Build (`go build ./cmd/...`)
4. Tests with race detector (`go test -race -count=1 ./...`)
5. E2E tests (`go test -tags=e2e -count=1 ./tests/e2e/...`)
6. Vet (`go vet ./...`)
7. Lint (`golangci-lint run ./...`)
8. SDK and example tests
9. Docker image builds

Run locally: `make ci`

## Pull Request Process

### Before Submitting

- [ ] Run `make ci` locally and ensure all checks pass
- [ ] Update documentation (README, AGENTS.md, API docs) if behavior changes
- [ ] Add regression tests for bug fixes
- [ ] Do not weaken invariants to make tests pass
- [ ] Distinguish implemented, tested, partially supported, planned, and unverified behavior
- [ ] Remove obsolete code only after checking usage and compatibility

### PR Requirements

- Clear title following Conventional Commits
- Description explaining the problem, solution, and testing performed
- Link to related issue(s)
- All CI checks passing
- At least one approval from a code owner (see CODEOWNERS)

### Review Process

Reviewers will verify:
- Behavior is correct and acceptance criterion is observable
- Files live in the right layer with clear ownership
- Dependencies point in the intended direction
- Interfaces are narrow, consumer-oriented, and necessary
- Common logic is reused without erasing real domain differences
- Names, errors, formatting, and comments are consistent
- Tenant boundaries and secrets are protected
- Duplicate delivery, retry, cancellation, shutdown, and partial failure are handled
- Schema, migrations, config, docs, examples, Docker/Compose, and CI are synchronized
- CI results tied to the exact reported commit
- Limitations, skipped checks, and unresolved risks stated plainly

## Release Process

Releases are created from `main` branch:

1. Ensure `main` is green
2. Run `make release` (builds binaries, runs CI, builds Docker images)
3. Create a git tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
4. Push tag: `git push origin vX.Y.Z`
5. GitHub Actions will build and publish release artifacts

## Architecture Overview

FlowRulz is a Go event-driven rules runtime with:

- **API** (`cmd/api`): HTTP server for rule management, event publishing, and observability
- **Worker** (`cmd/worker`): Consumes events from NATS JetStream, evaluates rules, emits effects
- **Domain** (`internal/domain`): Business types, invariants, validation, IDs, routing helpers
- **Rules** (`internal/rules`): Pure rule parsing, compilation, validation, evaluation
- **Services** (`internal/services/*`): Application use cases (events, rules, effects, workflow, batches, quarantine)
- **Runtime** (`internal/runtime/*`): Shard leases, fencing, key queues, scheduler
- **Adapters** (`internal/adapters/*`): HTTP, NATS, PostgreSQL, MongoDB, Redis, effects, cache
- **SDKs** (`sdk/go`, `sdk/go/flow-rulz`): Client libraries for producers/consumers
- **Domains** (`domains/*`): Contract specifications, rules, workflows, generated code

### Key Guarantees

- **At-least-once delivery**: Inbox deduplication and transactional outbox protect state transitions
- **Per-key ordering**: Bounded per-key queue preserves ordering for same partition key
- **Lease-based shard ownership**: Workers acquire shard leases with fencing tokens
- **Tenant isolation**: All queries and operations are tenant-scoped

See [docs/architecture/map.md](../docs/architecture/map.md) for detailed topology.

## Questions?

- Open a [Discussion](https://github.com/flow-rulz/flow-rulz/discussions) for design questions
- Open an [Issue](https://github.com/flow-rulz/flow-rulz/issues) for bugs or feature requests
- Check [AGENTS.md](../AGENTS.md) for detailed engineering standards