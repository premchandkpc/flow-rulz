# Legacy FlowRulz Architecture Redesign Plan

> **Status: historical draft.** This plan is preserved for context, not as a current task list. Several paths and checkboxes no longer match the repository. Verify every item against `../../AGENTS.md`, `../architecture/map.md`, source code and CI before acting on it.

## Phase 1: Critical Blockers (Week 1)

### 1.1 Move NATS Adapter from SQL to dedicated package
- [ ] Create `internal/adapters/nats/jetstream.go` from `internal/adapters/sql/nats.go`
- [ ] Create `internal/adapters/nats/config.go` for NATS config
- [ ] Update `cmd/worker/main.go` to use `nats.NewConsumer()` instead of `sql.NewJetStreamConsumer()`
- [ ] Remove `internal/adapters/sql/nats.go`
- [ ] Update `internal/storage/storage.go` to not reference SQL NATS

### 1.2 Fix Code Generator - Remove Internal Domain Imports
- [ ] Create `sdk/go/runtime/envelope.go` with public Envelope type
- [ ] Create `sdk/go/runtime/publisher.go` with public Publisher interface
- [ ] Create `sdk/go/runtime/consumer.go` with public Consumer interface
- [ ] Update `internal/codegen/generator.go` to use SDK types instead of `internal/domain`
- [ ] Ensure generated code compiles as standalone module

### 1.3 Remove Hardcoded Tenant "default"
- [ ] Create `internal/transport/http/middleware/auth.go` for tenant extraction
- [ ] Add tenant context key and helper functions
- [ ] Update `cmd/api/main.go` handlers to extract tenant from context
- [ ] Add authentication middleware to routes

### 1.4 Fix Fencing Validation Order
- [ ] Move fencing token validation BEFORE `tx.Commit()` in `internal/services/events/service.go`
- [ ] Ensure validation fails the transaction if fencing token is invalid

---

## Phase 2: Domain Boundaries & Correctness (Week 2)

### 2.1 Enforce Sharding Contract
- [ ] Make `shardedConsumer` required in `internal/application/worker.go`
- [ ] Add `ShardCount() uint32` to `ports.BrokerConsumer`
- [ ] Update NATS adapter to implement `ShardCount()`

### 2.2 Strict Config Validation
- [ ] Modify `getIntEnv` and `getDurationEnv` to return error on parse failure
- [ ] Update `Config.Validate()` to check all parsed values
- [ ] Add validation for unknown enum values (BatchMode, StorageBackend)

### 2.3 Contract Validation in Rule Activation
- [ ] Add `ContractRegistry` lookup in `internal/services/rules/service.go.Activate()`
- [ ] Validate `InputContract` exists and version matches
- [ ] Call `rules.ValidatePathsAgainstContract()` during compilation

### 2.4 Scheduler Shard Count from Config
- [ ] Pass `numShards` to `scheduler.NewScheduler()`
- [ ] Update `cmd/worker/main.go` to pass config value
- [ ] Remove hardcoded 4096 in `scheduler.go:161`

---

## Phase 3: Schema & Code Generation (Week 3)

### 3.1 Canonical Schemas Directory
- [ ] Create `schemas/events/envelope.schema.json` from `api/envelope-schema.json`
- [ ] Create `schemas/rules/rule-set.schema.json` from `api/rule-schema.json`
- [ ] Create `schemas/contracts/contract.schema.json`
- [ ] Create `schemas/workflows/workflow-definition.schema.json`
- [ ] Add schema validation tests in `tests/contract/`

### 3.2 Public SDK Module
- [ ] Create `sdk/go/go.mod` as separate module
- [ ] Generate contract types into `sdk/go/contracts/`
- [ ] Generate runtime types into `sdk/go/runtime/`
- [ ] Update generator to output to SDK module

---

## Phase 4: Backend Parity & Migrations (Week 4)

### 4.1 Mongo Migration Support
- [ ] Implement migration runner for MongoDB
- [ ] Or document as prerequisite with setup instructions

### 4.2 Adapter Conformance Tests
- [ ] Create shared test suite in `tests/conformance/`
- [ ] Run against both Postgres and Mongo adapters
- [ ] Verify transactions, duplicate detection, atomic claims, fencing

---

## Dependencies & Order

```
Phase 1.1 (NATS adapter) → Phase 1.2 (Codegen) → Phase 1.3 (Auth) → Phase 1.4 (Fencing)
                                                      ↓
Phase 2.1 (Sharding) ← Phase 2.2 (Config) ← Phase 2.3 (Contracts) ← Phase 2.4 (Scheduler)
                                                      ↓
Phase 3.1 (Schemas) → Phase 3.2 (SDK)
                                                      ↓
Phase 4.1 (Mongo Migrations) → Phase 4.2 (Conformance)
```

---

## Success Criteria

- [ ] `go build ./cmd/api ./cmd/worker ./cmd/codegen` succeeds
- [ ] `go test -race ./...` passes
- [ ] Generated code compiles in separate module without FlowRulz internal deps
- [ ] API accepts tenant from auth context (not hardcoded)
- [ ] Worker uses NATS adapter (not SQL adapter)
- [ ] Fencing validated before commit
- [ ] Config rejects malformed numeric values
- [ ] Contract validation runs on rule activation