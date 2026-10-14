# FlowRulz Phase 1 (P0) Implementation Plan

## Overview
This plan addresses the critical findings from the architecture review. Each task includes file locations, acceptance criteria, and test strategy.

---

## P0-1: Persist InputContract on Rule Revisions

### Problem
- `RuleRevision.InputContract` exists in domain model but not persisted
- PostgreSQL `rule_revisions` table missing `input_contract` column
- MongoDB adapter likely has same gap

### Files to Modify
1. **Migration**: Create `migrations/019_rule_revisions_input_contract.sql`
2. **PostgreSQL Adapter**: `internal/adapters/postgres/rules.go` (Save, Get, GetActive, List)
3. **MongoDB Adapter**: `internal/adapters/mongo/rules.go` (if exists)
4. **Domain**: Verify `RuleRevision.InputContract` serialization

### Acceptance Criteria
- [x] Migration adds `input_contract JSONB` column to `rule_revisions`
- [x] `RuleRepository.Save` persists `InputContract` as JSONB
- [x] `RuleRepository.Get`/`GetActive`/`List` load `InputContract` from JSONB
- [x] Round-trip test: save revision with InputContract → load → matches original
- [x] Existing revisions without InputContract load as nil (backward compatible)

### Test Strategy
- Unit test for postgres repository Save/Get round-trip
- Integration test with real PostgreSQL
- Verify migration applies cleanly to existing schema

---

## P0-2: Enforce Contract Identity at Worker Boundary

### Problem
- Worker only validates event contract when `envelope.Contract != nil`
- No enforcement that event contract matches rule revision's `InputContract`
- Producers can omit contract reference entirely

### Files to Modify
1. **Event Service**: `internal/services/events/service.go` (loadActiveRevision, processInTransaction)
2. **Domain Errors**: Add `ErrContractMismatch`, `ErrContractRequired` to `internal/domain/errors.go`

### Logic Changes
```go
// In loadActiveRevision or processInTransaction:
revision, err := s.loadActiveRevision(ctx, repos, envelope)
if err != nil { return nil, err }

// NEW: Enforce contract binding
if revision.InputContract != nil {
    // Rule requires a specific contract
    if envelope.Contract == nil {
        return nil, domain.ErrContractRequired  // Event must declare contract
    }
    if envelope.Contract.Name != revision.InputContract.Name ||
       envelope.Contract.Version != revision.InputContract.Version ||
       envelope.Contract.SchemaHash != revision.InputContract.SchemaHash {
        return nil, domain.ErrContractMismatch  // Contract doesn't match rule expectation
    }
} else if envelope.Contract != nil {
    // Rule doesn't require contract but event has one - allow but log?
    // Or reject if we want strict mode? For now, allow.
}
```

### Acceptance Criteria
- [x] Rule revision with `InputContract` rejects events without matching contract
- [x] Rule revision with `InputContract` rejects events with different contract name/version/hash
- [x] Rule revision without `InputContract` accepts events with or without contract (backward compatible)
- [x] Clear error messages distinguishing "contract required" vs "contract mismatch"

### Test Strategy
- Unit tests for all contract enforcement combinations
- Integration test: deploy rule with InputContract → publish event without contract → reject
- Integration test: deploy rule with InputContract v1 → publish event with v2 → reject

---

## P0-3: Contract-to-Ruleset Bindings (Explicit Routing)

### Problem
- Event type doubles as ruleset key (`ruleSet := envelope.Type`)
- Cannot route one event type to multiple rulesets
- No explicit binding configuration

### Files to Create/Modify
1. **Migration**: `migrations/020_contract_ruleset_bindings.sql`
2. **Domain**: `internal/domain/bindings.go` (new file)
3. **Ports**: Add `BindingRepository` to `internal/ports/repository.go`
4. **PostgreSQL Adapter**: `internal/adapters/postgres/bindings.go` (new file)
5. **Event Service**: `internal/services/events/service.go` (loadActiveRevision → use bindings)
6. **API Handlers**: Add binding CRUD endpoints
7. **Services**: `internal/services/bindings/` (new package for use cases)

### Domain Model
```go
// Binding connects an event contract to an eligible ruleset
type ContractBinding struct {
    ID              string
    TenantID        string
    ContractName    string
    ContractVersion string
    ContractHash    string
    RuleSet         string
    Priority        int           // For ordering when multiple bindings match
    ActivationPolicy BindingActivationPolicy
    CreatedAt       time.Time
    UpdatedAt       time.Time
}

type BindingActivationPolicy string
const (
    BindingPolicyAutomatic BindingActivationPolicy = "automatic"  // Auto-activate when rule exists
    BindingPolicyManual    BindingActivationPolicy = "manual"     // Requires explicit activation
)
```

### Routing Logic (Replaces `ruleSet := envelope.Type`)
```go
func (s *Service) resolveRulesets(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) ([]*domain.RuleRevision, error) {
    if envelope.Contract == nil {
        // Legacy: no contract declared, fall back to type-based routing
        return s.resolveLegacyRuleset(ctx, repos, envelope)
    }
    
    // Find all bindings matching this event's contract
    bindings, err := repos.Bindings.ListByContract(ctx, envelope.TenantID, envelope.Contract.Name, envelope.Contract.Version)
    if err != nil { return nil, err }
    
    var revisions []*domain.RuleRevision
    for _, binding := range bindings {
        // Verify contract hash matches
        if binding.ContractHash != envelope.Contract.SchemaHash {
            continue // Hash mismatch, skip
        }
        
        // Load active revision for this ruleset
        revision, err := repos.RuleRepo.GetActive(ctx, envelope.TenantID, binding.RuleSet)
        if err != nil || revision == nil {
            continue // No active revision for this ruleset
        }
        
        revisions = append(revisions, revision)
    }
    
    // Sort by binding priority
    sort.Slice(revisions, func(i, j int) bool {
        // Need to carry priority through... design decision needed
    })
    
    return revisions, nil
}
```

### Acceptance Criteria
- [x] Binding table created with proper indexes
- [x] CRUD API for bindings (create, list, delete) - repository layer done
- [x] Event service resolves rulesets via bindings when contract present
- [x] Legacy type-based routing preserved for events without contract
- [x] Multiple bindings for same contract → multiple rulesets execute
- [x] Binding priority controls execution order

### Test Strategy
- Unit tests for binding resolution logic
- Integration test: create binding → publish event → both rulesets execute
- Integration test: binding with hash mismatch → skipped
- Integration test: legacy event without contract → type-based routing works

---

## P0-4: Prevent Deletion of Referenced Contract Versions

### Problem
- `ContractRegistry.Delete` removes contract without checking references
- Referenced by: rule_revisions.InputContract, bindings, active rules

### Files to Modify
1. **PostgreSQL Adapter**: `internal/adapters/postgres/contract.go` (Delete method)
2. **Domain Errors**: Add `ErrContractReferenced` to `internal/domain/errors.go`

### Logic Changes
```go
func (r *ContractRegistry) Delete(ctx context.Context, name, version string) error {
    // Check rule_revisions references
    var count int
    err := r.db.QueryRow(ctx, `
        SELECT COUNT(*) FROM rule_revisions 
        WHERE input_contract->>'name' = $1 AND input_contract->>'version' = $2
    `, name, version).Scan(&count)
    if err != nil { return err }
    if count > 0 {
        return domain.ErrContractReferenced // "contract referenced by N rule revisions"
    }
    
    // Check bindings references (when implemented)
    err = r.db.QueryRow(ctx, `
        SELECT COUNT(*) FROM contract_bindings 
        WHERE contract_name = $1 AND contract_version = $2
    `, name, version).Scan(&count)
    if err != nil { return err }
    if count > 0 {
        return domain.ErrContractReferenced // "contract referenced by N bindings"
    }
    
    // Proceed with delete
    _, err = r.db.Exec(ctx, `DELETE FROM contract_schemas WHERE name = $1 AND version = $2`, name, version)
    return err
}
```

### Acceptance Criteria
- [x] Delete fails when contract referenced by rule_revisions.InputContract
- [x] Delete fails when contract referenced by bindings
- [x] Error message indicates what references the contract
- [x] Contracts with no references can still be deleted

### Test Strategy
- Unit test: delete unreferenced contract → succeeds
- Unit test: delete contract referenced by rule revision → fails with ErrContractReferenced
- Integration test: full workflow with reference protection

---

## P0-5: Migration Tracking and Coordinated Execution

### Problem
- `RunMigrations` executes all SQL files on every startup
- No version tracking, no coordination between API and worker
- Race condition on concurrent startup

### Files to Modify
1. **Migration**: `migrations/000_schema_migrations.sql` (create tracking table)
2. **PostgreSQL Adapter**: `internal/adapters/postgres/db.go` (RunMigrations)
3. **Storage Interface**: Add migration status methods if needed

### New Migration Table
```sql
-- migrations/000_schema_migrations.sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT NOT NULL PRIMARY KEY,  -- e.g., "001", "019"
    name        TEXT NOT NULL,              -- e.g., "rule_revisions"
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT NOT NULL               -- SHA256 of migration file
);
```

### Updated RunMigrations
```go
func (d *DB) RunMigrations(ctx context.Context) error {
    // Acquire advisory lock to prevent concurrent migration execution
    const migrationLockID = 0x464C525A // "FLRZ" in hex
    _, err := d.pool.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID)
    if err != nil {
        return fmt.Errorf("acquire migration lock: %w", err)
    }
    defer func() {
        _, _ = d.pool.Exec(ctx, `SELECT pg_advisory_xact_unlock($1)`, migrationLockID)
    }()
    
    // Get applied migrations
    applied := make(map[string]string) // version -> checksum
    rows, err := d.pool.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
    if err != nil { return err }
    for rows.Next() {
        var version, checksum string
        rows.Scan(&version, &checksum)
        applied[version] = checksum
    }
    rows.Close()
    
    // Read and sort migration files
    entries, err := os.ReadDir(d.migrations)
    if err != nil { return err }
    
    var files []string
    for _, e := range entries {
        if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
            files = append(files, e.Name())
        }
    }
    sort.Strings(files)
    
    for _, f := range files {
        version := strings.TrimSuffix(f, ".sql")
        version = strings.Split(version, "_")[0] // "001_rule_revisions" -> "001"
        
        // Check if already applied
        if existingChecksum, ok := applied[version]; ok {
            // Verify checksum matches
            data, _ := os.ReadFile(filepath.Join(d.migrations, f))
            currentChecksum := fmt.Sprintf("%x", sha256.Sum256(data))
            if existingChecksum != currentChecksum {
                return fmt.Errorf("migration %s checksum mismatch: applied %s, current %s", version, existingChecksum, currentChecksum)
            }
            continue // Already applied
        }
        
        // Apply migration in transaction
        data, err := os.ReadFile(filepath.Join(d.migrations, f))
        if err != nil { return err }
        
        tx, err := d.pool.Begin(ctx)
        if err != nil { return err }
        
        if _, err := tx.Exec(ctx, string(data)); err != nil {
            tx.Rollback(ctx)
            return fmt.Errorf("exec migration %s: %w", f, err)
        }
        
        // Record migration
        checksum := fmt.Sprintf("%x", sha256.Sum256(data))
        if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`, 
            version, strings.TrimSuffix(f, ".sql"), checksum); err != nil {
            tx.Rollback(ctx)
            return fmt.Errorf("record migration %s: %w", f, err)
        }
        
        if err := tx.Commit(ctx); err != nil {
            return fmt.Errorf("commit migration %s: %w", f, err)
        }
    }
    return nil
}
```

### Acceptance Criteria
- [x] `schema_migrations` table created as first migration
- [x] Advisory lock prevents concurrent migration execution
- [x] Only pending migrations are applied
- [x] Checksum verification detects modified migration files
- [x] Each migration runs in its own transaction
- [x] API and worker can start concurrently without migration conflicts

### Test Strategy
- Unit test: RunMigrations twice → second call is no-op
- Unit test: Modified migration file → checksum mismatch error
- Integration test: Start API and worker simultaneously → no race
- Integration test: Add new migration → only new migration applies

---

## P0-6: SDK Envelope Contract Fields

### Problem
- SDK `Envelope` struct missing `Contract` and `SchemaHash` fields
- Producers using SDK cannot declare contract reference

### Files to Modify
1. **SDK Transport**: `sdk/go/flowrule/transport.go` (Envelope struct)
2. **SDK NATS Transport**: `sdk/go/flowrule/transport_nats.go` (marshaling)
3. **SDK HTTP Transport**: `sdk/go/flowrule/transport_http.go` (if needed)

### Acceptance Criteria
- [x] `Envelope` has `Contract *ContractRef` field
- [x] `ContractRef` type exists in SDK
- [x] NATS transport serializes contract field correctly
- [x] Helper functions for setting contract on envelope

### Test Strategy
- Unit test: Envelope with Contract serializes to JSON with contract field
- Integration test: SDK producer → worker validates contract

---

## Priority Order & Dependencies

```
P0-1 (Persist InputContract) ──┐
                               ├──→ P0-2 (Enforce at Worker) ──→ P0-3 (Bindings)
P0-5 (Migration Tracking) ─────┘           │
                                           ↓
P0-4 (Prevent Contract Deletion) ◄─────────┘ (needs bindings table)
                                           ↓
P0-6 (SDK Contract Fields) ────────────────┘ (independent, can parallelize)
```

**Recommended execution order:**
1. P0-5 (Migration tracking) - Foundation for all schema changes
2. P0-1 (Persist InputContract) - Core schema change
3. P0-2 (Enforce at Worker) - Uses P0-1
4. P0-3 (Bindings) - Uses P0-1, P0-2
5. P0-4 (Prevent Deletion) - Uses P0-1, P0-3
6. P0-6 (SDK Fields) - Can be done anytime, needed for producers

---

## Rollback Plan
Each migration is independent and can be rolled back by:
1. Reverting code changes
2. Manually dropping added columns/tables if needed
3. Migration tracking table prevents re-application

---

## Verification Checklist Before Production

- [ ] All P0 migrations apply cleanly to fresh and existing databases
- [ ] Contract enforcement works end-to-end (producer → worker → DB)
- [ ] Multiple rulesets can subscribe to same event contract via bindings
- [ ] Referenced contracts cannot be deleted
- [ ] Concurrent API/worker startup doesn't cause migration conflicts
- [ ] SDK producers can declare contracts
- [ ] No regression in existing type-based routing (legacy events)
- [ ] All tests pass: unit, integration, race detector