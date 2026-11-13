-- Recover only identities proven by the execution referenced by a legacy row.
-- Unknown or conflicting rows remain empty and must be reconciled by an operator.
UPDATE inbox AS i
SET rule_set = e.rule_set
FROM executions AS e
WHERE i.rule_set = ''
  AND i.status = 'committed'
  AND i.execution_id = e.execution_id
  AND i.tenant_id = e.tenant_id
  AND i.event_id = e.event_id
  AND e.rule_set <> ''
  AND NOT EXISTS (
      SELECT 1 FROM inbox AS current_row
      WHERE current_row.tenant_id = i.tenant_id
        AND current_row.event_id = i.event_id
        AND current_row.rule_set = e.rule_set
  );
