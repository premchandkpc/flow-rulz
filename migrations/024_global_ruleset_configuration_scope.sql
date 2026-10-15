-- Move rule revisions, activations, and contract bindings into one global
-- configuration scope. Event tenant identity remains on event/runtime tables.
--
-- Do not silently merge tenant-specific rule sets that have the same ID but
-- different content or active revisions. Fail with a useful message so an
-- operator can rename/reconcile the conflicting rulesets before retrying.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM rule_revisions a
        JOIN rule_revisions b
          ON a.rule_id = b.rule_id
         AND a.revision = b.revision
         AND a.tenant_scope < b.tenant_scope
        WHERE a.content_hash IS DISTINCT FROM b.content_hash
           OR a.source IS DISTINCT FROM b.source
           OR a.compiled IS DISTINCT FROM b.compiled
           OR a.compiler_version IS DISTINCT FROM b.compiler_version
           OR a.match_mode IS DISTINCT FROM b.match_mode
           OR a.input_contract IS DISTINCT FROM b.input_contract
    ) THEN
        RAISE EXCEPTION
            'Cannot globalize rulesets: the same rule_set/revision has different content across configuration scopes';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rule_activations a
        JOIN rule_activations b
          ON a.rule_set = b.rule_set
         AND a.tenant_scope < b.tenant_scope
        WHERE a.revision IS DISTINCT FROM b.revision
    ) THEN
        RAISE EXCEPTION
            'Cannot globalize rulesets: a rule_set has different active revisions across configuration scopes';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM contract_bindings a
        JOIN contract_bindings b
          ON a.contract_name = b.contract_name
         AND a.contract_version = b.contract_version
         AND a.rule_set = b.rule_set
         AND a.id < b.id
        WHERE a.contract_hash IS DISTINCT FROM b.contract_hash
    ) THEN
        RAISE EXCEPTION
            'Cannot globalize contract bindings: a contract version has different schema hashes across configuration scopes';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM contract_bindings a
        JOIN contract_bindings b
          ON a.contract_name = b.contract_name
         AND a.contract_version = b.contract_version
         AND a.contract_hash = b.contract_hash
         AND a.rule_set = b.rule_set
         AND a.id < b.id
        WHERE a.priority IS DISTINCT FROM b.priority
           OR a.activation_policy IS DISTINCT FROM b.activation_policy
    ) THEN
        RAISE EXCEPTION
            'Cannot globalize contract bindings: duplicate contract/ruleset bindings have conflicting priority or activation policy';
    END IF;
END $$;

INSERT INTO rule_revisions (
    tenant_scope, rule_id, revision, source, compiled, content_hash,
    compiler_version, match_mode, created_at, input_contract
)
SELECT DISTINCT ON (rule_id, revision)
    '__domain_configuration__', rule_id, revision, source, compiled, content_hash,
    compiler_version, match_mode, created_at, input_contract
FROM rule_revisions
ORDER BY rule_id, revision, created_at DESC
ON CONFLICT (tenant_scope, rule_id, revision) DO NOTHING;

INSERT INTO rule_activations (
    tenant_scope, rule_set, revision, version, actor, activated_at
)
SELECT DISTINCT ON (rule_set)
    '__domain_configuration__', rule_set, revision, version, actor, activated_at
FROM rule_activations
ORDER BY rule_set, version DESC, activated_at DESC
ON CONFLICT (tenant_scope, rule_set) DO UPDATE SET
    revision = EXCLUDED.revision,
    version = GREATEST(rule_activations.version, EXCLUDED.version),
    actor = EXCLUDED.actor,
    activated_at = GREATEST(rule_activations.activated_at, EXCLUDED.activated_at);

WITH binding_source AS (
    SELECT DISTINCT ON (contract_name, contract_version, contract_hash, rule_set)
        contract_name, contract_version, contract_hash, rule_set,
        priority, activation_policy, created_at, updated_at
    FROM contract_bindings
    ORDER BY contract_name, contract_version, contract_hash, rule_set, updated_at DESC, id DESC
)
INSERT INTO contract_bindings (
    tenant_id, contract_name, contract_version, contract_hash, rule_set,
    priority, activation_policy, created_at, updated_at
)
SELECT
    '__domain_configuration__', s.contract_name, s.contract_version, s.contract_hash,
    s.rule_set, s.priority, s.activation_policy, s.created_at, s.updated_at
FROM binding_source s
WHERE NOT EXISTS (
    SELECT 1
    FROM contract_bindings existing
    WHERE existing.tenant_id = '__domain_configuration__'
      AND existing.contract_name = s.contract_name
      AND existing.contract_version = s.contract_version
      AND existing.contract_hash = s.contract_hash
      AND existing.rule_set = s.rule_set
);

-- Remove exact duplicate global bindings left by older tenant-scoped copies.
-- Conflict checks above guarantee duplicates have identical routing policy.
DELETE FROM contract_bindings older
USING contract_bindings newer
WHERE older.tenant_id = '__domain_configuration__'
  AND newer.tenant_id = '__domain_configuration__'
  AND older.contract_name = newer.contract_name
  AND older.contract_version = newer.contract_version
  AND older.contract_hash = newer.contract_hash
  AND older.rule_set = newer.rule_set
  AND (older.updated_at, older.id) < (newer.updated_at, newer.id);
