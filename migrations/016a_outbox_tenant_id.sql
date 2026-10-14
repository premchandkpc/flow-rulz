ALTER TABLE outbox_effects
    ADD COLUMN IF NOT EXISTS tenant_id TEXT;

-- Backfill existing effects from their durable execution record before enforcing
-- the invariant for all new outbox writes.
UPDATE outbox_effects AS effects
SET tenant_id = executions.tenant_id
FROM executions
WHERE effects.execution_id = executions.execution_id
  AND (effects.tenant_id IS NULL OR effects.tenant_id = '');

ALTER TABLE outbox_effects
    ALTER COLUMN tenant_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_effects_tenant_execution
    ON outbox_effects (tenant_id, execution_id);
