# FlowRulz Refactoring Plan: SOLID, Hexagonal, DDD, Loose Coupling

## Phase 1: Method Decomposition (Critical - Week 1)

### 1.1 EventService.Process() - Decompose 183 lines → 6 methods
- [ ] `validateAndEnrich()` - validation & enrichment
- [ ] `beginTransaction()` - transaction setup
- [ ] `deduplicateEvent()` - inbox deduplication
- [ ] `loadRuleAndEvaluate()` - rule loading + evaluation
- [ ] `persistExecutionAndOutbox()` - persistence
- [ ] `commitTransaction()` - commit

### 1.2 Worker.runSharded() - Decompose 70 lines → 4 methods
- [ ] `syncConsumersAndFetch()` - sync + fetch
- [ ] `dispatchDeliveries()` - delivery dispatch loop
- [ ] `processSingleDelivery()` - single delivery processing
- [ ] `handleDeliveryError()` - error handling

### 1.3 Worker.processDelivery() - Decompose 35 lines → 3 methods
- [ ] `processDeliveryCore()` - core processing
- [ ] `handleDeliveryError()` - error handling
- [ ] `acknowledgeDelivery()` - ACK/NAK

### 1.4 Evaluator.Evaluate() - Decompose 59 lines → 3 methods
- [ ] `evaluateRules()` - rule iteration
- [ ] `buildEffects()` - effect building
- [ ] `buildExplanations()` - explanation building

### 1.5 Compiler.Compile() - Decompose 67 lines → 3 methods
- [ ] `parseRuleSet()` - JSON parsing
- [ ] `validateRuleSet()` - validation
- [ ] `compileRules()` - compilation

### 1.6 Evaluator.buildEffect() - Decompose 80 lines → 4 methods
- [ ] `buildEmitEffect()`
- [ ] `buildEmitEventEffect()`
- [ ] `buildCommandEffect()`
- [ ] `resolvePartitionKey()`

---

## Phase 2: Interface Segregation & Loose Coupling (Week 2)

### 2.1 Port Extraction
- [ ] Extract `KeyQueue` port from `keyqueue.KeyQueue`
- [ ] Extract `EnvelopeSerializer` port for JSON marshaling
- [ ] Split `EventTxRepos` into focused interfaces
- [ ] Add `EnvelopeValidator` port

### 2.2 Multi-Ruleset Consumer Support
- [ ] Implement `NATSRulesetConsumer` 
- [ ] Implement `ConsumerRegistry` with multi-ruleset support
- [ ] Update `Worker` to use `ConsumerRegistry`

### 2.3 Port Cleanup
- [ ] Move `PubSub` from cache.go to broker.go
- [ ] Split `EventTxRepos` into focused repo bundles

---

## Phase 3: DDD Tactical Patterns (Week 3)

### 3.1 Domain Events
- [ ] Add `DomainEventPublisher` port
- [ ] Emit `RuleEvaluated`, `ExecutionCompleted`, `EffectEmitted` events
- [ ] Emit `WorkflowStateChanged` from `WorkflowInstance.Transition()`

### 3.2 Aggregate Boundaries
- [ ] Define `RuleSet` as Aggregate Root
- [ ] Define `Workflow` as Aggregate Root
- [ ] Add factory methods for complex aggregates

### 3.3 Factory Methods
- [ ] `RuleRevision.Create()` factory
- [ ] `WorkflowInstance.Start()` factory
- [ ] `Execution.Start()` factory

---

## Phase 4: Testing & Observability (Week 4)

### 4.1 Critical Tests
- [ ] Worker crash recovery tests
- [ ] Lease expiration during processing tests
- [ ] Concurrent rule evaluation tests
- [ ] Effect delivery retry/quarantine tests
- [ ] Rule hot-swap during processing tests

### 4.2 Mutation Testing
- [ ] Add mutation testing for domain logic
- [ ] Target 80%+ mutation score for domain

---

## Phase 5: Documentation & ADRs

- [ ] ADR: Multi-ruleset consumer support
- [ ] ADR: Domain event publishing mechanism
- [ ] ADR: Aggregate boundaries (RuleSet, Workflow)
- [ ] ADR: Saga/orchestration for workflows
- [ ] ADR: Testing strategy for runtime

---

## Execution Order

1. **Week 1**: Phase 1 (Method Decomposition) - CRITICAL
2. **Week 2**: Phase 2 (Interface Segregation) - CRITICAL  
3. **Week 3**: Phase 3 (DDD Patterns) - HIGH
4. **Week 4**: Phase 4 (Testing) - HIGH
4. **Week 5**: Phase 5 (Documentation) - MEDIUM