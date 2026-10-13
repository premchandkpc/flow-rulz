---
name: Bug Report
about: Create a report to help us improve
title: "[BUG] "
labels: bug
assignees: ''
---

## Bug Description

A clear and concise description of what the bug is.

## Steps to Reproduce

1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

## Expected Behavior

A clear and concise description of what you expected to happen.

## Actual Behavior

A clear and concise description of what actually happened.

## Environment

- OS: [e.g. Ubuntu 22.04, macOS 14.0]
- Go version: [e.g. 1.23.0]
- Docker version: [e.g. 24.0.0]
- FlowRulz version/commit: [e.g. main branch, commit abc123]

## Component Affected

- [ ] API Server (`cmd/api`)
- [ ] Worker (`cmd/worker`)
- [ ] Rules Engine (`internal/rules`)
- [ ] Event Processing (`internal/services/events`)
- [ ] Effects/Outbox (`internal/services/effects`)
- [ ] Workflow (`internal/services/workflow`)
- [ ] Shard/Lease (`internal/runtime/shard`)
- [ ] Scheduler (`internal/runtime/scheduler`)
- [ ] NATS Adapter (`internal/adapters/nats`)
- [ ] PostgreSQL Adapter (`internal/adapters/postgres`)
- [ ] MongoDB Adapter (`internal/adapters/mongo`)
- [ ] Redis Adapter (`internal/adapters/redis`)
- [ ] HTTP Adapter (`internal/adapters/http`)
- [ ] SDK (`sdk/go`, `sdk/go/flowrulz`)
- [ ] Domain Contracts (`domains/*`)
- [ ] Examples (`examples/*`)
- [ ] Rust Worker (`rust-worker`)
- [ ] Web Console (`web/console`)
- [ ] Docker/Compose
- [ ] Migrations
- [ ] Documentation
- [ ] CI/CD
- [ ] Other: ___________

## Logs/Error Output

```
Paste relevant logs here
```

## Additional Context

Add any other context about the problem here (screenshots, configuration, etc.).

## Checklist

- [ ] I have searched existing issues to avoid duplicates
- [ ] I have read the [Contributing Guide](../CONTRIBUTING.md)
- [ ] I have read the [Engineering Standards](../AGENTS.md)
- [ ] This bug is reproducible on the latest `main` branch